package db

import (
	cm "mygodis/common"
	"mygodis/common/commoninterface"
	"mygodis/datadriver/dict"
	"mygodis/resp"
	"mygodis/util/cmdutil"
	"strconv"
)

func (db *DataBaseImpl) getAsHash(key string) (dict.Dict, resp.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	dict, ok := entity.Data.(dict.Dict)
	if !ok {
		return nil, &resp.WrongTypeErrReply{}
	}
	return dict, nil
}
func (db *DataBaseImpl) getOrCreateAsHash(key string) (result dict.Dict, isNew bool) {
	d, _ := db.getAsHash(key)
	if d == nil {
		d = dict.NewConcurrentDict()
		return d, true
	}
	return d, false
}
func execHGet(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 2 {
		return resp.MakeErrReply("wrong number of arguments for 'hget' command")
	}
	key := args[0]
	field := args[1]
	d, err := db.getAsHash(string(key))
	if err != nil {
		return err
	}
	if d == nil {
		return resp.MakeNullBulkReply()
	}
	value, exists := d.Get(string(field))
	if !exists {
		return resp.MakeNullBulkReply()
	}
	return resp.MakeBulkReply([]byte(value.(string)))
}
func execHSet(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 3 {
		return resp.MakeErrReply("wrong number of arguments for 'hset' command")
	}
	key := args[0]
	field := args[1]
	value := args[2]
	d, isNew := db.getOrCreateAsHash(string(key))
	_, exists := d.Get(string(field))
	d.Put(string(field), string(value))
	if exists {
		return resp.MakeIntReply(0)
	}
	if isNew {
		data := new(commoninterface.DataEntity)
		data.Data = d
		db.PutEntity(string(key), data)
	}
	db.addAof(cmdutil.ToCmdLineWithBytes("hset", key, field, value))
	return resp.MakeIntReply(1)

}
func execHMSet(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) < 3 || len(args)%2 == 0 {
		return resp.MakeErrReply("wrong number of arguments for 'hmset' command")
	}
	key := args[0]
	d, isNew := db.getOrCreateAsHash(string(key))
	for i := 1; i < len(args); i += 2 {
		field := args[i]
		value := args[i+1]
		d.Put(string(field), string(value))
	}
	if isNew {
		data := new(commoninterface.DataEntity)
		data.Data = d
		db.PutEntity(string(key), data)
	}
	db.addAof(cmdutil.ToCmdLineWithBytes("hmset", args...))
	return resp.MakeOkReply()
}
func execHMGet(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) < 2 {
		return resp.MakeErrReply("wrong number of arguments for 'hmget' command")
	}
	key := args[0]
	d, err := db.getAsHash(string(key))
	if err != nil {
		return err
	}
	if d == nil {
		return resp.MakeNullBulkReply()
	}
	var result [][]byte
	for i := 1; i < len(args); i++ {
		field := args[i]
		value, exists := d.Get(string(field))
		if !exists {
			result = append(result, nil)
		} else {
			result = append(result, []byte(value.(string)))
		}
	}
	return resp.MakeMultiBulkReply(result)
}
func execHGetAll(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 1 {
		return resp.MakeErrReply("wrong number of arguments for 'hgetall' command")
	}
	key := args[0]
	d, err := db.getAsHash(string(key))
	if err != nil {
		return err
	}
	if d == nil {
		return resp.MakeNullBulkReply()
	}
	var result [][]byte
	d.ForEach(func(key string, value interface{}) bool {
		result = append(result, []byte(key))
		result = append(result, []byte(value.(string)))
		return true
	})
	return resp.MakeMultiBulkReply(result)
}
func execHDel(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) < 2 {
		return resp.MakeErrReply("wrong number of arguments for 'hdel' command")
	}
	key := args[0]
	d, err := db.getAsHash(string(key))
	if err != nil {
		return err
	}
	if d == nil {
		return resp.MakeIntReply(0)
	}
	var count int64
	for i := 1; i < len(args); i++ {
		field := args[i]
		if _, r := d.Remove(string(field)); r > 0 {
			count++
		}
	}
	db.addAof(cmdutil.ToCmdLineWithBytes("hdel", args...))
	return resp.MakeIntReply(count)
}
func execHExists(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 2 {
		return resp.MakeErrReply("wrong number of arguments for 'hexists' command")
	}
	key := args[0]
	field := args[1]
	d, err := db.getAsHash(string(key))
	if err != nil {
		return err
	}
	if d == nil {
		return resp.MakeIntReply(0)
	}
	if _, exists := d.Get(string(field)); exists {
		return resp.MakeIntReply(1)
	}
	return resp.MakeIntReply(0)
}
func execHIncrBy(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 3 {
		return resp.MakeErrReply("wrong number of arguments for 'hincrby' command")
	}
	key := args[0]
	field := args[1]
	delta, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return resp.MakeErrReply("value is not an integer or out of range")
	}
	d, isNew := db.getOrCreateAsHash(string(key))
	value, exists := d.Get(string(field))
	if !exists {
		value = "0"
	}
	v, err := strconv.ParseInt(value.(string), 10, 64)
	if err != nil {
		return resp.MakeErrReply("hash value is not an integer")
	}
	v += delta
	d.Put(string(field), strconv.FormatInt(v, 10))
	if isNew {
		return resp.MakeIntReply(delta)
	}
	db.addAof(cmdutil.ToCmdLineWithBytes("hincrby", args...))
	return resp.MakeIntReply(v)
}
func execHIncrByFloat(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 3 {
		return resp.MakeErrReply("wrong number of arguments for 'hincrbyfloat' command")
	}
	key := args[0]
	field := args[1]
	delta, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return resp.MakeErrReply("value is not a valid float")
	}
	d, isNew := db.getOrCreateAsHash(string(key))
	value, exists := d.Get(string(field))
	if !exists {
		value = "0"
	}
	v, err := strconv.ParseFloat(value.(string), 64)
	if err != nil {
		return resp.MakeErrReply("hash value is not a float")
	}
	v += delta
	d.Put(string(field), strconv.FormatFloat(v, 'f', -1, 64))
	if isNew {
		return resp.MakeBulkReply([]byte(strconv.FormatFloat(delta, 'f', -1, 64)))
	}
	db.addAof(cmdutil.ToCmdLineWithBytes("hincrbyfloat", args...))
	return resp.MakeBulkReply([]byte(strconv.FormatFloat(v, 'f', -1, 64)))
}
func execHKeys(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 1 {
		return resp.MakeErrReply("wrong number of arguments for 'hkeys' command")
	}
	key := args[0]
	d, err := db.getAsHash(string(key))
	if err != nil {
		return err
	}
	if d == nil {
		return resp.MakeNullBulkReply()
	}
	var result [][]byte
	keys := d.Keys()
	for _, keyItem := range keys {
		result = append(result, []byte(keyItem))
	}
	return resp.MakeMultiBulkReply(result)
}
func execHLen(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 1 {
		return resp.MakeErrReply("wrong number of arguments for 'hlen' command")
	}
	key := args[0]
	d, err := db.getAsHash(string(key))
	if err != nil {
		return err
	}
	if d == nil {
		return resp.MakeIntReply(0)
	}
	return resp.MakeIntReply(int64(d.Len()))
}
func execHSetNx(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 3 {
		return resp.MakeErrReply("wrong number of arguments for 'hsetnx' command")
	}
	key := args[0]
	field := args[1]
	value := args[2]
	d, isNew := db.getOrCreateAsHash(string(key))
	if _, exists := d.Get(string(field)); exists {
		return resp.MakeIntReply(0)
	}
	d.Put(string(field), string(value))
	if isNew {
		data := new(commoninterface.DataEntity)
		data.Data = d
		db.PutEntity(string(key), data)
	}
	return resp.MakeIntReply(1)

}
func execHVals(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 1 {
		return resp.MakeErrReply("wrong number of arguments for 'hvals' command")
	}
	key := args[0]
	d, err := db.getAsHash(string(key))
	if err != nil {
		return err
	}
	if d == nil {
		return resp.MakeNullBulkReply()
	}
	var result [][]byte
	d.ForEach(func(key string, value interface{}) bool {
		result = append(result, []byte(value.(string)))
		return true
	})
	return resp.MakeMultiBulkReply(result)
}

// TODO hscan
func undoHSetCommands(db *DataBaseImpl, args cm.CmdLine) []cm.CmdLine {
	if len(args) != 3 {
		return nil
	}
	key := args[0]
	field := args[1]
	return rollbackHashFields(db, string(key), string(field))
}
func undoHSetNxCommands(db *DataBaseImpl, args cm.CmdLine) []cm.CmdLine {
	if len(args) != 3 {
		return nil
	}
	key := args[0]
	field := args[1]
	return rollbackHashFields(db, string(key), string(field))
}
func undoHDelCommands(db *DataBaseImpl, args cm.CmdLine) []cm.CmdLine {
	if len(args) < 3 {
		return nil
	}
	key := args[0]
	var fields []string
	for i := 1; i < len(args); i++ {
		fields = append(fields, string(args[i]))
	}
	return rollbackHashFields(db, string(key), fields...)
}
func undoHIncrByCommands(db *DataBaseImpl, args cm.CmdLine) []cm.CmdLine {
	if len(args) != 4 {
		return nil
	}
	key := args[0]
	field := args[1]
	delta, err := strconv.Atoi(string(args[3]))
	if err != nil {
		return nil
	}
	return []cm.CmdLine{cmdutil.ToCmdLine("hincrby", string(key), string(field), strconv.Itoa(-delta))}
}
func undoHIncrByFloatCommands(db *DataBaseImpl, args cm.CmdLine) []cm.CmdLine {
	if len(args) != 4 {
		return nil
	}
	key := args[0]
	field := args[1]
	delta, err := strconv.ParseFloat(string(args[3]), 64)
	if err != nil {
		return nil
	}
	return []cm.CmdLine{cmdutil.ToCmdLine("hincrbyfloat", string(key), string(field), strconv.FormatFloat(-delta, 'f', -1, 64))}
}
func undoHMSetCommands(db *DataBaseImpl, args cm.CmdLine) []cm.CmdLine {
	if len(args) < 3 {
		return nil
	}
	key := args[0]
	var fields []string
	for i := 1; i < len(args); i += 2 {
		fields = append(fields, string(args[i]))
	}
	return rollbackHashFields(db, string(key), fields...)
}
func init() {
	RegisterCommand("hget", execHGet, readFirstKey, nil, 3, ReadOnly)
	RegisterCommand("hgetall", execHGetAll, readFirstKey, nil, 2, ReadOnly)
	RegisterCommand("hmget", execHMGet, readFirstKey, nil, -3, ReadOnly)
	RegisterCommand("hexists", execHExists, readFirstKey, nil, 3, ReadOnly)
	RegisterCommand("hkeys", execHKeys, readFirstKey, nil, 2, ReadOnly)
	RegisterCommand("hlen", execHLen, readFirstKey, nil, 2, ReadOnly)
	RegisterCommand("hvals", execHVals, readFirstKey, nil, 2, ReadOnly)

	RegisterCommand("hdel", execHDel, writeFirstKey, undoHDelCommands, -3, Write)
	RegisterCommand("hincrby", execHIncrBy, writeFirstKey, undoHIncrByCommands, 4, Write)
	RegisterCommand("hincrbyfloat", execHIncrByFloat, writeFirstKey, undoHIncrByFloatCommands, 4, Write)
	RegisterCommand("hset", execHSet, writeFirstKey, undoHSetCommands, -4, Write)
	RegisterCommand("hsetnx", execHSetNx, writeFirstKey, undoHSetNxCommands, 4, Write)
	RegisterCommand("hmset", execHMSet, writeFirstKey, undoHMSetCommands, -4, Write)

}
