package db

import (
	"mygodis/aof"
	cm "mygodis/common"
	"mygodis/common/commoninterface"
	"mygodis/datadriver/dict"
	"mygodis/datadriver/list"
	"mygodis/datadriver/set"
	"mygodis/datadriver/sortedset"
	"mygodis/resp"
	"mygodis/util/cmdutil"
	"strconv"
	"strings"
	"time"
)

func execDelete(db *DataBaseImpl, line cm.CmdLine) resp.Reply {
	if len(line) == 0 {
		return resp.MakeErrReply("wrong number of arguments for 'del' command")
	}

	keys := make([]string, 0, len(line))
	for _, key := range line {
		if _, ok := db.GetEntity(string(key)); ok {
			keys = append(keys, string(key))
		}
	}
	deleted := db.RemoveBatch(keys...)
	if deleted > 0 {
		db.addAof(cmdutil.ToCmdLineWithName("del", keys...))
	}
	return resp.MakeIntReply(int64(deleted))
}
func execExists(db *DataBaseImpl, cmd cm.CmdLine) resp.Reply {
	if len(cmd) == 0 {
		return resp.MakeErrReply("wrong number of arguments for 'exists' command")
	}
	count := 0
	for _, key := range cmd {
		if _, ok := db.GetEntity(string(key)); ok {
			count++
		}
	}
	return resp.MakeIntReply(int64(count))
}
func execFlushDB(db *DataBaseImpl, line cm.CmdLine) resp.Reply {
	db.Flush()
	db.addAof(cmdutil.ToCmdLine("flushdb"))
	return resp.MakeOkReply()
}
func execType(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 1 {
		return resp.MakeErrReply("wrong number of arguments for 'type' command")
	}
	entity, ok := db.GetEntity(string(args[0]))
	if !ok {
		return resp.MakeNullBulkReply()
	}
	switch entity.Data.(type) {
	case []byte:
		return resp.MakeBulkReply([]byte("string"))
	case list.List:
		return resp.MakeBulkReply([]byte("list"))
	case dict.ConcurrentDict:
		return resp.MakeBulkReply([]byte("hash"))
	case *set.Set:
		return resp.MakeBulkReply([]byte("set"))
	case *sortedset.ZSet:
		return resp.MakeBulkReply([]byte("zset"))
	default:
		return resp.MakeBulkReply([]byte("unknown type"))
	}
}
func renameKey(db *DataBaseImpl, nx bool, src, dest string) resp.ErrorReply {
	srcEntity, o := db.GetEntity(src)
	if _, ok := srcEntity, o; !ok {
		return resp.MakeErrReply("no such key")
	}
	if nx {
		if _, ok := db.GetEntity(dest); ok {
			return resp.MakeErrReply("destination key exists")
		}
	}
	ttl, exists := db.ttlMap.Get(src)
	db.PutEntity(dest, srcEntity)
	db.Remove(src)
	if exists {
		db.Persist(src)
		db.Persist(dest)
		db.Expire(dest, ttl.(time.Time))
	}
	rename := "rename"
	if nx {
		rename = "renamenx"
	}
	db.addAof(cmdutil.ToCmdLineWithName(rename, src, dest))
	return nil
}
func execRename(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 2 {
		return resp.MakeErrReply("wrong number of arguments for 'rename' command")
	}
	src := string(args[0])
	dest := string(args[1])
	errorReply := renameKey(db, false, src, dest)
	if errorReply != nil {
		return errorReply
	}
	return resp.MakeOkReply()
}
func execRenameNx(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 2 {
		return resp.MakeErrReply("wrong number of arguments for 'rename' command")
	}
	src := string(args[0])
	dest := string(args[1])
	errorReply := renameKey(db, true, src, dest)
	if errorReply != nil {
		return errorReply
	}
	return resp.MakeOkReply()
}
func expire(db *DataBaseImpl, key string, t time.Time) resp.Reply {
	db.Expire(key, t)
	db.addAof(aof.ExpireToCmd(key, t).Args)
	return resp.MakeIntReply(1)
}
func execExpire(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	parseInt, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return resp.MakeErrReply("ERR value is not an integer or out of range")
	}
	ttl := time.Duration(parseInt) * time.Second
	_, ok := db.GetEntity(key)
	if !ok {
		return resp.MakeIntReply(0)
	}
	expireAt := time.Now().Add(ttl)
	return expire(db, key, expireAt)
}
func execExpireAt(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	parseInt, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return resp.MakeErrReply("ERR value is not an integer or out of range")
	}
	ttl := time.Unix(parseInt, 0)
	_, ok := db.GetEntity(key)
	if !ok {
		return resp.MakeIntReply(0)
	}
	return expire(db, key, ttl)
}
func execPExpire(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	parseInt, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return resp.MakeErrReply("ERR value is not an integer or out of range")
	}
	ttl := time.Duration(parseInt) * time.Millisecond
	_, ok := db.GetEntity(key)
	if !ok {
		return resp.MakeIntReply(0)
	}
	expireAt := time.Now().Add(ttl)
	return expire(db, key, expireAt)
}
func execPExpireAt(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	parseInt, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return resp.MakeErrReply("ERR value is not an integer or out of range")
	}
	ttl := time.UnixMilli(parseInt)
	_, ok := db.GetEntity(key)
	if !ok {
		return resp.MakeIntReply(0)
	}
	return expire(db, key, ttl)
}
func execTTL(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	if _, ok := db.GetEntity(key); !ok {
		return resp.MakeIntReply(-2)
	}
	ttl, ok := db.ttlMap.Get(key)
	if !ok {
		return resp.MakeIntReply(-1)
	}
	return resp.MakeIntReply(int64(ttl.(time.Time).Sub(time.Now()) / time.Second))
}
func execPTTL(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	if _, ok := db.GetEntity(key); !ok {
		return resp.MakeIntReply(-2)
	}
	ttl, ok := db.ttlMap.Get(key)
	if !ok {
		return resp.MakeIntReply(-1)
	}
	return resp.MakeIntReply(int64(ttl.(time.Time).Sub(time.Now()) / time.Millisecond))
}
func execPersist(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	if _, ok := db.GetEntity(key); !ok {
		return resp.MakeIntReply(0)
	}
	if _, ok := db.ttlMap.Get(key); !ok {
		return resp.MakeIntReply(0)
	}
	db.Persist(key)
	db.addAof(cmdutil.ToCmdLine("persist", key))
	return resp.MakeIntReply(1)
}
func execCopy(manage *StandaloneDatabaseManager, connection commoninterface.Connection, line cm.CmdLine) resp.Reply {
	dbIndex := connection.GetDBIndex()
	db := manage.selectDB(dbIndex)
	src := string(line[0])
	dest := string(line[1])
	replaceFlag := false
	if len(line) > 2 {
		for i := 2; i < len(line); i++ {
			arg := strings.ToLower(string(line[i]))
			if arg == "db" {
				if i+1 > len(line) {
					return resp.MakeErrReply("ERR syntax error")
				}
				targetDBIndex, err := strconv.Atoi(string(line[i+1]))
				if err != nil {
					return resp.MakeErrReply("ERR syntax error")
				}
				if targetDBIndex < 0 || targetDBIndex >= len(manage.Dbs) {
					return resp.MakeErrReply("ERR invalid DB index")
				}
				dbIndex = targetDBIndex
				i++
			} else if arg == "replace" {
				replaceFlag = true
			} else {
				return resp.MakeErrReply("ERR syntax error")
			}
		}
	}
	if dbIndex != connection.GetDBIndex() || src == dest {
		return resp.MakeErrReply("ERR source and destination objects are the same")
	}
	srcEntity, ok := db.GetEntity(src)
	if !ok {
		return resp.MakeIntReply(0)
	}
	selectDB := manage.selectDB(dbIndex)
	if _, ex := selectDB.GetEntity(dest); ex {
		if !replaceFlag {
			return resp.MakeIntReply(0)
		}
	}
	selectDB.PutEntity(dest, srcEntity)
	val, exists := db.ttlMap.Get(src)
	if exists {
		selectDB.Expire(dest, val.(time.Time))
	}
	manage.AddAof(connection.GetDBIndex(), cmdutil.ToCmdLine("copy", src, dest, "db", strconv.Itoa(dbIndex)))
	return resp.MakeIntReply(1)
}

// TODO keys
func toTTLcmd(db *DataBaseImpl, key string) *resp.MultiBulkReply {
	val, exists := db.ttlMap.Get(key)

	if !exists {
		return resp.MakeMultiBulkReply(cmdutil.ToCmdLine("persist", key))
	}
	expireAt := val.(time.Time)
	return resp.MakeMultiBulkReply(cmdutil.ToCmdLine("expireat", key, strconv.FormatInt(expireAt.Unix()/1e6, 10)))
}
func prepareRename(args cm.CmdLine) ([]string, []string) {
	src := string(args[0])
	dest := string(args[1])
	return []string{dest}, []string{src}
}
func undoDeleteCommands(db *DataBaseImpl, line cm.CmdLine) []cm.CmdLine {
	keys := make([]string, 0, len(line))
	for _, v := range line {
		keys = append(keys, string(v))
	}
	return rollbackGivenKeys(db, keys...)
}
func undoRenameCommands(db *DataBaseImpl, line cm.CmdLine) []cm.CmdLine {
	src := string(line[0])
	dest := string(line[1])
	return rollbackGivenKeys(db, src, dest)
}
func undoExpireCommands(db *DataBaseImpl, line cm.CmdLine) []cm.CmdLine {
	key := string(line[0])
	return []cm.CmdLine{toTTLcmd(db, key).Args}
}
func init() {
	RegisterCommand("exists", execExists, readFirstKey, nil, -2, ReadOnly)
	RegisterCommand("ttl", execTTL, readFirstKey, nil, 2, ReadOnly)
	RegisterCommand("pttl", execPTTL, readFirstKey, nil, 2, ReadOnly)
	RegisterCommand("type", execType, readFirstKey, nil, 2, ReadOnly)
	RegisterCommand("keys", nil, nil, nil, 3, ReadOnly) // TODO keys
	RegisterCommand("del", execDelete, writeFirstKey, undoDeleteCommands, -2, Write)
	RegisterCommand("expire", execExpire, writeFirstKey, undoExpireCommands, 3, Write)
	RegisterCommand("expireat", execExpireAt, writeFirstKey, undoExpireCommands, 3, Write)
	RegisterCommand("pexpire", execPExpire, writeFirstKey, undoExpireCommands, 3, Write)
	RegisterCommand("pexpireat", execPExpireAt, writeFirstKey, undoExpireCommands, 3, Write)
	RegisterCommand("persist", execPersist, writeFirstKey, nil, 2, Write)
	RegisterCommand("rename", execRename, prepareRename, undoRenameCommands, 3, Write)
	RegisterCommand("renamenx", execRenameNx, prepareRename, undoRenameCommands, 3, Write)
}
