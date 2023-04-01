package db

import (
	"mygodis/aof"
	cm "mygodis/common"
	"mygodis/resp"
	"mygodis/util/cmdutil"
	"strconv"
	"time"
)

func readFirstKey(args cm.CmdLine) (writeKeys []string, readKeys []string) {
	key := string(args[0])
	return nil, []string{key}
}
func writeFirstKey(args cm.CmdLine) (writeKeys []string, readKeys []string) {
	key := string(args[0])
	return []string{key}, nil
}
func writeAllKeys(args cm.CmdLine) (writeKeys []string, readKeys []string) {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	return keys, nil
}
func readAllKeys(args cm.CmdLine) (writeKeys []string, readKeys []string) {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	return nil, keys
}
func noPrepare() (writeKeys []string, readKeys []string) {
	return nil, nil
}
func rollbackFirstKey(db *DataBaseImpl, args cm.CmdLine) []cm.CmdLine {
	key := string(args[0])
	return rollbackGivenKeys(db, key)
}
func rollbackGivenKeys(db *DataBaseImpl, keys ...string) []cm.CmdLine {
	var undoCmdLines []cm.CmdLine
	for _, key := range keys {
		entity, ok := db.GetEntity(key)
		if !ok {
			undoCmdLines = append(undoCmdLines,
				cmdutil.ToCmdLine("DEL", key),
			)
		} else {
			undoCmdLines = append(undoCmdLines,
				cmdutil.ToCmdLine("DEL", key),     // clean existed first
				aof.EntityToCmd(key, entity).Args, // then restore
				ToTTLCmdLine(db, key).Args,
			)
		}
	}
	return undoCmdLines
}
func rollbackHashFields(db *DataBaseImpl, key string, fields ...string) []cm.CmdLine {
	var undoCmdLines []cm.CmdLine
	hash, errorReply := db.getAsHash(key)
	if errorReply != nil {
		return nil
	}
	if hash == nil {
		undoCmdLines = append(undoCmdLines,
			cmdutil.ToCmdLine("DEL", key),
		)
	}
	for _, field := range fields {
		value, ok := hash.Get(field)
		if !ok {
			undoCmdLines = append(undoCmdLines,
				cmdutil.ToCmdLine("HDEL", key, field),
			)
		} else {
			undoCmdLines = append(undoCmdLines,
				cmdutil.ToCmdLine("HSET", key, field, value.(string)),
			)
		}
	}
	return undoCmdLines
}
func ToTTLCmdLine(db *DataBaseImpl, key string) *resp.MultiBulkReply {
	val, exists := db.ttlMap.Get(key)
	if !exists {
		return resp.MakeMultiBulkReply(cmdutil.ToCmdLine("PERSIST", key))
	}
	expireTime, ok := val.(time.Time)
	if ok {
		ttl := strconv.FormatInt(expireTime.UnixNano()/1e6, 10)
		return resp.MakeMultiBulkReply(cmdutil.ToCmdLine("PEXPIREAT", key, ttl))
	}
	return nil
}
func prepareSetCalculate(args cm.CmdLine) ([]string, []string) {
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}
	return nil, keys
}
func prepareSetCalculateStore(args cm.CmdLine) ([]string, []string) {
	dest := string(args[0])
	keys := make([]string, len(args)-1)
	keyArgs := args[1:]
	for i, arg := range keyArgs {
		keys[i] = string(arg)
	}
	return []string{dest}, keys
}
func rollbackSetMember(db *DataBaseImpl, key string, members ...string) []cm.CmdLine {
	var undoCmdLines []cm.CmdLine
	set, errorReply := db.getAsSet(key)
	if errorReply != nil {
		return nil
	}
	if set == nil {
		undoCmdLines = append(undoCmdLines,
			cmdutil.ToCmdLine("DEL", key),
		)
	}
	for _, member := range members {
		if set.Has(member) {
			undoCmdLines = append(undoCmdLines,
				cmdutil.ToCmdLine("SADD", key, member),
			)
		} else {
			undoCmdLines = append(undoCmdLines,
				cmdutil.ToCmdLine("SREM", key, member),
			)
		}
	}
	return undoCmdLines
}
func rollbackZsetMember(db *DataBaseImpl, key string, members ...string) []cm.CmdLine {
	var undoCmdLines []cm.CmdLine
	zset, errorReply := db.getAsZSet(key)
	if errorReply != nil {
		return nil
	}
	if zset == nil {
		undoCmdLines = append(undoCmdLines,
			cmdutil.ToCmdLine("DEL", key),
		)
	}
	for _, member := range members {
		elem, ok := zset.Get(member)
		if ok {
			undoCmdLines = append(undoCmdLines,
				cmdutil.ToCmdLine("ZADD", key, strconv.FormatFloat(elem.Score, 'f', -1, 64), member),
			)
		} else {
			undoCmdLines = append(undoCmdLines,
				cmdutil.ToCmdLine("ZREM", key, member),
			)
		}
	}
	return undoCmdLines
}
