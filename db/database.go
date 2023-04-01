package db

import (
	"fmt"
	cm "mygodis/common"
	"mygodis/common/commoninterface"
	"mygodis/datadriver/dict"
	"mygodis/lib/delay"
	"mygodis/lib/sync/lockermap"
	"mygodis/resp"
	"strings"
	"time"
)

const (
	//dataDictSize = 1 << 16
	//ttlDictSize  = 1 << 10
	//lockerSize   = 1024
	dataDictSize = 4
	ttlDictSize  = 4
	lockerSize   = 1024
)

type DataBaseImpl struct {
	index          int
	data           dict.Dict
	ttlMap         dict.Dict
	versionMap     dict.Dict
	addAof         func(cm.CmdLine)
	insertCallback commoninterface.KeyEventCallback
	deleteCallback commoninterface.KeyEventCallback
	locker         *lockermap.LockerMap
}

// Dump used for testing
func dump(db *DataBaseImpl) {
	fmt.Println("dumping db")
	fmt.Println("DB Index: ", db.index)
	fmt.Println("DB Data: ")
	db.data.ForEach(func(key string, value any) bool {
		fmt.Println(key, ":", value)
		return true
	})
	fmt.Println("DB TTL: ")
	db.ttlMap.ForEach(func(key string, value any) bool {
		fmt.Println(key, ":", value)
		return true
	})
	fmt.Println("DB Version: ")
	db.versionMap.ForEach(func(key string, value any) bool {
		fmt.Println(key, ":", value)
		return true
	})

}

type ExecFunc func(db *DataBaseImpl, args cm.CmdLine) resp.Reply
type PreFunc func(args cm.CmdLine) ([]string, []string)
type UndoFunc func(db *DataBaseImpl, args cm.CmdLine) []cm.CmdLine

func NewDB() *DataBaseImpl {
	db := &DataBaseImpl{
		data:       dict.NewConcurrentDict(),
		ttlMap:     dict.NewConcurrentDict(),
		versionMap: dict.NewConcurrentDict(),
		locker:     lockermap.NewLockerMap(lockerSize),
		addAof:     func(line cm.CmdLine) {},
	}
	return db
}
func newBasicDB() *DataBaseImpl {
	db := &DataBaseImpl{
		data:       dict.NewConcurrentDict(),
		ttlMap:     dict.NewSimpleDict(ttlDictSize),
		versionMap: dict.NewSimpleDict(dataDictSize),
		//locker:     lock.Make(lockerSize),
		addAof: func(line cm.CmdLine) {},
	}
	return db
}
func (dbi *DataBaseImpl) Exec(c commoninterface.Connection, cmd cm.CmdLine) (reply resp.Reply) {
	s := strings.ToLower(string(cmd[0]))
	switch s {
	case "multi": //开启事务
		if len(cmd) != 1 {
			return resp.MakeArgNumErrReply(s)
		}
		return StartMulti(c)
	case "discard": //取消事务
		if len(cmd) != 1 {
			return resp.MakeArgNumErrReply(s)
		}
		return DiscardMulti(c)
	case "exec": //执行事务
		if len(cmd) != 1 {
			return resp.MakeArgNumErrReply(s)
		}
		return execMulti(dbi, c)
	case "watch": //监视key
		if len(cmd) < 2 {
			return resp.MakeArgNumErrReply(s)
		}
		return Watch(dbi, c, cmd)
	}
	if c != nil && c.InMultiState() {
		EnQueue(c, cmd)
	}
	reply = dbi.ExecNormal(cmd)
	return reply

}
func (dbi *DataBaseImpl) GetEntity(key string) (dataEntity *commoninterface.DataEntity, ok bool) {
	val, exists := dbi.data.Get(key) //hash
	if !exists {
		return nil, false
	}
	dataEntity, ok = val.(*commoninterface.DataEntity)
	return
}
func (dbi *DataBaseImpl) PutEntity(key string, dataEntity *commoninterface.DataEntity) int {
	result := dbi.data.Put(key, dataEntity)
	if insertCb := dbi.insertCallback; result > 0 && insertCb != nil {
		insertCb(dbi.index, key, dataEntity)
	}
	return result
}
func (dbi *DataBaseImpl) PutExists(key string, dataEntity *commoninterface.DataEntity) int {
	exists := dbi.data.PutIfExists(key, dataEntity)
	return exists
}
func (dbi *DataBaseImpl) PutAbsent(key string, dataEntity *commoninterface.DataEntity) int {
	r := dbi.data.PutIfAbsent(key, dataEntity)
	if r > 0 {
		if insertCb := dbi.insertCallback; insertCb != nil {
			insertCb(dbi.index, key, dataEntity)
		}
	}
	return r

}
func (dbi *DataBaseImpl) DeleteEntity(key string) int {
	val, r := dbi.data.Remove(key)
	dbi.ttlMap.Remove(key)

	if deleteCb := dbi.deleteCallback; r > 0 && deleteCb != nil {
		deleteCb(dbi.index, key, val.(*commoninterface.DataEntity))
	}
	return r
}
func (dbi *DataBaseImpl) Remove(key string) int {
	val, result := dbi.data.Remove(key)
	dbi.ttlMap.Remove(key)
	dbi.versionMap.Remove(key)
	taskKey := expireTaskKey(key)
	delay.Cancel(taskKey)
	if deleteCb := dbi.deleteCallback; deleteCb != nil {
		if result > 0 {
			deleteCb(dbi.index, key, val.(*commoninterface.DataEntity))
		}
	}
	return result
}
func (dbi *DataBaseImpl) RemoveBatch(keys ...string) int {
	var count int
	for _, key := range keys {
		_, result := dbi.data.Remove(key)
		count += result
	}
	return count

}
func (dbi *DataBaseImpl) Flush() {
	dbi.data.Clear()
	dbi.ttlMap.Clear()
	dbi.versionMap.Clear()
}
func (dbi *DataBaseImpl) Expire(key string, ttl time.Time) {
	dbi.ttlMap.Put(key, ttl)
	taskKey := expireTaskKey(key)
	delay.At(ttl, taskKey, func() {
		fmt.Println("到点了 expire key:", key)
		dbi.Remove(key)
	})
}
func (dbi *DataBaseImpl) Persist(key string) {
	dbi.ttlMap.Remove(key)
	taskKey := expireTaskKey(key)
	delay.Cancel(taskKey)
}
func (dbi *DataBaseImpl) IsExpire(key string) bool {
	expireTime, exists := dbi.ttlMap.Get(key)
	if !exists {
		return false
	}
	if time.Now().After(expireTime.(time.Time)) {
		dbi.Remove(key)
		return true
	}
	return true
}
func (dbi *DataBaseImpl) GetVersion(key string) (version uint32, ok bool) {
	val, ok := dbi.versionMap.Get(key)
	if ok {
		version = val.(uint32)
	}
	return
}
func (dbi *DataBaseImpl) SetVersion(key ...string) {
	for _, k := range key {
		val, ok := dbi.versionMap.Get(k)
		if ok {
			dbi.versionMap.Put(k, val.(uint32)+1)
		} else {
			dbi.versionMap.Put(k, 1)
		}
	}
}
func (dbi *DataBaseImpl) ForEach(f func(key string, entity *commoninterface.DataEntity, expireTime time.Time) bool) {
	dbi.data.ForEach(func(key string, val any) bool {
		entity := val.(*commoninterface.DataEntity)
		expireTime, _ := dbi.ttlMap.Get(key)
		return f(key, entity, expireTime.(time.Time))
	})
}
func (dbi *DataBaseImpl) RWLocks(writeKeys []string, readKeys []string) {
	dbi.locker.RWLockBatch(writeKeys, readKeys)
}
func (dbi *DataBaseImpl) RWUnLocks(writeKeys []string, readKeys []string) {
	dbi.locker.URWLockBatch(writeKeys, readKeys)
}
func (dbi *DataBaseImpl) ExecNormal(line cm.CmdLine) resp.Reply {
	command, b := GetCommand(line)
	reply := validateCommand(command, b, line)
	if reply != nil {
		return reply
	}
	return command.executor(dbi, line[1:])
}
func (dbi *DataBaseImpl) ExecWithLock(line cm.CmdLine) resp.Reply {
	command, b := GetCommand(line)
	reply := validateCommand(command, b, line)
	if reply != nil {
		return reply
	}
	wkeys, rkeys := command.prepare(line)
	defer dbi.RWUnLocks(wkeys, rkeys)
	dbi.RWLocks(wkeys, rkeys)
	dbi.SetVersion(wkeys...)
	return command.executor(dbi, line)
}
func validateArity(arity int, cmdArgs cm.CmdLine) bool {
	argNum := len(cmdArgs)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}
func expireTaskKey(key string) string {
	return "expire:" + key
}
func validateCommand(command *Command, b bool, line cm.CmdLine) resp.Reply {
	if !b {
		return resp.MakeErrReply("ERR unknown command " + string(line[0]))
	}
	if !validateArity(command.arity, line) {
		return resp.MakeArgNumErrReply(string(line[0]))
	}
	return nil
}
