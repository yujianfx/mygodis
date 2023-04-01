package db

import (
	"bytes"
	"fmt"
	cm "mygodis/common"
	"mygodis/common/commoninterface"
	"mygodis/datadriver/list"
	"mygodis/resp"
	"mygodis/util/cmdutil"
	"strconv"
)

func (d *DataBaseImpl) getAsList(key string) (list.List, resp.ErrorReply) {
	if value, ok := d.GetEntity(key); ok {
		if list, isOk := value.Data.(list.List); isOk {
			return list, nil
		}
		return nil, resp.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return nil, nil
}
func getOrCreateList(d *DataBaseImpl, key string) (result list.List, isCreated bool) {
	if value, ok := d.GetEntity(key); ok {
		if list, isOk := value.Data.(list.List); isOk {
			return list, false
		}
	}
	quickList := list.NewQuickList()
	data := new(commoninterface.DataEntity)
	data.Data = quickList
	d.PutEntity(key, data)
	return quickList, true
}
func execLIndex(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 2 {
		return resp.MakeErrReply("wrong number of arguments for 'lindex' command")
	}
	key := string(args[0])
	index, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return resp.MakeErrReply("value is not an integer or out of range")
	}
	list, err := db.getAsList(key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if list == nil {
		return resp.MakeNullBulkReply()
	}
	if index < 0 {
		index = list.Len() + index
	}
	if index < 0 || index >= list.Len() {
		return resp.MakeNullBulkReply()
	}
	return resp.MakeBulkReply(list.Get(index).([]byte))
}
func execLLen(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 1 {
		return resp.MakeErrReply("wrong number of arguments for 'llen' command")
	}
	key := string(args[0])
	list, err := db.getAsList(key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if list == nil {
		return resp.MakeIntReply(0)
	}
	return resp.MakeIntReply(int64(list.Len()))
}
func execLPop(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 1 {
		return resp.MakeErrReply("wrong number of arguments for 'lpop' command")
	}
	key := string(args[0])
	list, err := db.getAsList(key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if list == nil {
		return resp.MakeNullBulkReply()
	}
	val := list.Remove(0)
	return resp.MakeBulkReply(val.([]byte))
}
func execLPush(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) < 2 {
		return resp.MakeErrReply("wrong number of arguments for 'lpush' command")
	}
	key := string(args[0])
	list, isCreated := getOrCreateList(db, key)
	for i := 1; i < len(args); i++ {
		list.Add(args[i])
	}
	if isCreated {
		db.PutEntity(key, &commoninterface.DataEntity{Data: list})
	}
	return resp.MakeIntReply(int64(list.Len()))
}
func execLPushX(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 2 {
		return resp.MakeErrReply("wrong number of arguments for 'lpushx' command")
	}
	key := string(args[0])
	list, err := db.getAsList(key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if list == nil {
		return resp.MakeIntReply(0)
	}
	list.Add(args[1])
	return resp.MakeIntReply(int64(list.Len()))
}
func execLRange(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 3 {
		return resp.MakeErrReply("wrong number of arguments for 'lrange' command")
	}
	key := string(args[0])
	start, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return resp.MakeErrReply("value is not an integer or out of range")
	}
	stop, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return resp.MakeErrReply("value is not an integer or out of range")
	}
	list, err := db.getAsList(key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if list == nil {
		return resp.MakeMultiBulkReply(nil)
	}
	if start < 0 {
		start = list.Len() + start
	}
	if start < 0 {
		start = 0
	}
	if stop < 0 {
		stop = list.Len() + stop
	}
	if stop >= list.Len() {
		stop = list.Len() - 1
	}
	if start > stop {
		return resp.MakeMultiBulkReply(nil)
	}
	result := make([][]byte, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		result = append(result, list.Get(i).([]byte))
	}
	return resp.MakeMultiBulkReply(result)
}
func execLRem(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 3 {
		return resp.MakeErrReply("wrong number of arguments for 'lrem' command")
	}
	key := string(args[0])
	count, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return resp.MakeErrReply("value is not an integer or out of range")
	}
	list, err := db.getAsList(key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if list == nil {
		return resp.MakeIntReply(0)
	}
	if count == 0 {
		return resp.MakeIntReply(0)
	}
	if count > 0 {
		for i := 0; i < list.Len(); i++ {
			if bytes.Equal(list.Get(i).([]byte), args[2]) {
				list.Remove(i)
				count--
				if count == 0 {
					break
				}
			}
		}
	} else {
		for i := list.Len() - 1; i >= 0; i-- {
			if bytes.Equal(list.Get(i).([]byte), args[2]) {
				list.Remove(i)
				count++
				if count == 0 {
					break
				}
			}
		}
	}
	return resp.MakeIntReply(int64(list.Len()))
}
func execLSet(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 3 {
		return resp.MakeErrReply("wrong number of arguments for 'lset' command")
	}
	key := string(args[0])
	index, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return resp.MakeErrReply("value is not an integer or out of range")
	}
	list, err := db.getAsList(key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if list == nil {
		return resp.MakeErrReply("no such key")
	}
	if index < 0 {
		index = list.Len() + index
	}
	if index < 0 || index >= list.Len() {
		return resp.MakeErrReply("index out of range")
	}
	list.Set(index, args[2])
	return resp.MakeOkReply()
}
func execRPop(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 1 {
		return resp.MakeErrReply("wrong number of arguments for 'rpop' command")
	}
	key := string(args[0])
	list, err := db.getAsList(key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if list == nil {
		return resp.MakeNullBulkReply()
	}
	val := list.Remove(list.Len() - 1)
	return resp.MakeBulkReply(val.([]byte))
}
func execRPopLPush(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 2 {
		return resp.MakeErrReply("wrong number of arguments for 'rpoplpush' command")
	}
	srcKey := string(args[0])
	dstKey := string(args[1])
	srcList, err := db.getAsList(srcKey)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if srcList == nil {
		return resp.MakeNullBulkReply()
	}
	dstList, isCreated := getOrCreateList(db, dstKey)
	val := srcList.Remove(srcList.Len() - 1)
	dstList.Add(val)
	if isCreated {
		db.PutEntity(dstKey, &commoninterface.DataEntity{Data: dstList})
	}
	return resp.MakeBulkReply(val.([]byte))
}
func execRPush(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) < 2 {
		return resp.MakeErrReply("wrong number of arguments for 'rpush' command")
	}
	key := string(args[0])
	list, isCreated := getOrCreateList(db, key)
	for i := 1; i < len(args); i++ {
		list.Add(args[i])
	}
	if isCreated {
		db.PutEntity(key, &commoninterface.DataEntity{Data: list})
	}
	return resp.MakeIntReply(int64(list.Len()))
}
func execRPushX(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 2 {
		return resp.MakeErrReply("wrong number of arguments for 'rpushx' command")
	}
	key := string(args[0])
	list, err := db.getAsList(key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if list == nil {
		return resp.MakeIntReply(0)
	}
	list.Add(args[1])
	return resp.MakeIntReply(int64(list.Len()))
}
func execLTrim(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 3 {
		return resp.MakeErrReply("wrong number of arguments for 'ltrim' command")
	}
	key := string(args[0])
	start, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return resp.MakeErrReply("value is not an integer or out of range")
	}
	stop, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return resp.MakeErrReply("value is not an integer or out of range")
	}
	list, err := db.getAsList(key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if list == nil {
		return resp.MakeOkReply()
	}
	if start < 0 {
		start = list.Len() + start
	}
	if start < 0 {
		start = 0
	}
	if stop < 0 {
		stop = list.Len() + stop
	}
	if stop >= list.Len() {
		stop = list.Len() - 1
	}
	if start > stop {
		list.RemoveAllByVal(func(a any) bool {
			return true
		})
		return resp.MakeOkReply()
	}
	list.RemoveByVal(func(a any) bool {
		return true
	}, start)
	list.ReverseRemoveByVal(func(a any) bool {
		return true
	}, list.Len()-stop)
	return resp.MakeOkReply()
}
func undoLPopCommands(db *DataBaseImpl, args cm.CmdLine) []cm.CmdLine {
	key := string(args[0])
	list, err := db.getAsList(key)
	if err != nil {
		return nil
	}
	if list == nil {
		return nil
	}
	val := list.Get(0)
	return []cm.CmdLine{cmdutil.ToCmdLine("lpush", key, val.(string))}
}
func undoLPushCommands(db *DataBaseImpl, args cm.CmdLine) []cm.CmdLine {
	key := string(args[0])
	list, err := db.getAsList(key)
	if err != nil {
		return nil
	}
	if list == nil {
		return nil
	}
	return []cm.CmdLine{cmdutil.ToCmdLine("lpop", key)}
}
func undoLPushXCommands(db *DataBaseImpl, args cm.CmdLine) []cm.CmdLine {
	key := string(args[0])
	list, err := db.getAsList(key)
	if err != nil {
		return nil
	}
	if list == nil {
		return nil
	}
	return []cm.CmdLine{cmdutil.ToCmdLine("lpop", key)}
}
func undoRPopCommands(db *DataBaseImpl, args cm.CmdLine) []cm.CmdLine {
	key := string(args[0])
	list, err := db.getAsList(key)
	if err != nil {
		return nil
	}
	if list == nil {
		return nil
	}
	val := list.Get(list.Len() - 1)
	return []cm.CmdLine{cmdutil.ToCmdLine("rpush", key, val.(string))}
}
func undoRPopLPushCommands(db *DataBaseImpl, args cm.CmdLine) []cm.CmdLine {
	srcKey := string(args[0])
	dstKey := string(args[1])
	srcList, err := db.getAsList(srcKey)
	if err != nil {
		return nil
	}
	if srcList == nil {
		return nil
	}
	dstList, err := db.getAsList(dstKey)
	if err != nil {
		return nil
	}
	if dstList == nil {
		return nil
	}
	val := dstList.Get(dstList.Len() - 1)
	return []cm.CmdLine{cmdutil.ToCmdLine("rpush", srcKey, val.(string)), cmdutil.ToCmdLine("lpop", dstKey)}
}
func undoRPushCommands(db *DataBaseImpl, args cm.CmdLine) []cm.CmdLine {
	key := string(args[0])
	list, err := db.getAsList(key)
	result := make([]cm.CmdLine, 0, len(args)-1)
	if err != nil {
		return nil
	}
	if list == nil {
		return nil
	}
	for i := 1; i < len(args); i++ {
		result = append(result, cmdutil.ToCmdLine("rpop", key))
	}
	return result
}
func undoRPushXCommands(db *DataBaseImpl, args cm.CmdLine) []cm.CmdLine {
	return undoRPushCommands(db, args)
}
func undoLtrimCommands(db *DataBaseImpl, args cm.CmdLine) []cm.CmdLine {
	key := string(args[0])
	start, _ := strconv.Atoi(string(args[1]))
	stop, _ := strconv.Atoi(string(args[2]))
	list, _ := db.getAsList(key)
	result := make([]cm.CmdLine, 0)
	if start < 0 {
		start = list.Len() + start
	}
	if start < 0 {
		start = 0
	}
	if stop < 0 {
		stop = list.Len() + stop
	}
	if stop >= list.Len() {
		stop = list.Len() - 1
	}
	if start > stop {
		list.ForEach(func(i int, a any) bool {
			result = append(result, cmdutil.ToCmdLine("rpush", key, a.(string)))
			return true
		})
		return result

	}
	list.ForEach(func(i int, a any) bool {
		if i > start {
			result = append(result, cmdutil.ToCmdLine("lset", key, fmt.Sprintf("%d", i), a.(string)))
		}
		if i > stop && i < list.Len() {
			result = append(result, cmdutil.ToCmdLine("rpush", key, a.(string)))
		}
		return true
	})
	return result
}
func preparePopPush(args cm.CmdLine) (writeKeys []string, readKeys []string) {
	writeKeys = make([]string, 0, 2)
	writeKeys = append(writeKeys, string(args[0]), string(args[1]))
	return writeKeys, readKeys
}
func init() {
	RegisterCommand("lindex", execLIndex, readFirstKey, nil, 2, ReadOnly)
	RegisterCommand("llen", execLLen, readFirstKey, nil, 1, ReadOnly)
	RegisterCommand("lrange", execLRange, readFirstKey, nil, 3, ReadOnly)
	RegisterCommand("lpop", execLPop, writeFirstKey, undoLPopCommands, 1, Write)
	RegisterCommand("lpush", execLPush, writeFirstKey, undoLPushCommands, -2, Write)
	RegisterCommand("lpushx", execLPushX, writeFirstKey, undoLPushXCommands, 2, Write)
	RegisterCommand("lrem", execLRem, writeFirstKey, undoLtrimCommands, 3, Write)
	RegisterCommand("lset", execLSet, writeFirstKey, rollbackFirstKey, 3, Write)
	RegisterCommand("ltrim", execLTrim, writeFirstKey, rollbackFirstKey, 3, Write)
	RegisterCommand("rpop", execRPop, writeFirstKey, undoRPopCommands, 1, Write)
	RegisterCommand("rpoplpush", execRPopLPush, preparePopPush, undoRPopLPushCommands, 2, Write)
	RegisterCommand("rpush", execRPush, writeFirstKey, undoRPushCommands, -3, Write)
	RegisterCommand("rpushx", execRPushX, writeFirstKey, undoRPushXCommands, -3, Write)

}
