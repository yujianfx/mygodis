package aof

import (
	"mygodis/common/commoninterface"
	"mygodis/datadriver/dict"
	"mygodis/datadriver/list"
	"mygodis/datadriver/set"
	"mygodis/datadriver/sortedset"
	"mygodis/resp"
	"strconv"
	"time"
)

var setCmd = []byte("SET")
var rPushAllCmd = []byte("RPUSH")
var sAddCmd = []byte("SADD")
var hmSetCmd = []byte("HMSET")
var zAddCmd = []byte("ZADD")
var pExpireAtBytesCmd = []byte("PEXPIREAT")

func EntityToCmd(key string, entity *commoninterface.DataEntity) *resp.MultiBulkReply {
	if entity == nil {
		return nil
	}
	var result *resp.MultiBulkReply
	switch val := entity.Data.(type) {
	case []byte:
		result = stringToCmd(key, val)
	case list.List:
		result = listToCmd(key, val)
	case *set.Set:
		result = setToCmd(key, val)
	case dict.Dict:
		result = hashToCmd(key, val)
	case *sortedset.ZSet:
		result = zSetToCmd(key, val)

	}
	return result
}
func stringToCmd(key string, bytes []byte) *resp.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = setCmd
	args[1] = []byte(key)
	args[2] = bytes
	return resp.MakeMultiBulkReply(args)
}
func listToCmd(key string, list list.List) *resp.MultiBulkReply {
	args := make([][]byte, 2+list.Len())
	args[0] = rPushAllCmd
	args[1] = []byte(key)
	list.ForEach(func(i int, val interface{}) bool {
		bytes, _ := val.([]byte)
		args[2+i] = bytes
		return true
	})
	return resp.MakeMultiBulkReply(args)
}
func setToCmd(key string, set *set.Set) *resp.MultiBulkReply {
	args := make([][]byte, 2+set.Len())
	args[0] = sAddCmd
	args[1] = []byte(key)
	i := 0
	set.ForEach(func(val string) bool {
		args[2+i] = []byte(val)
		i++
		return true
	})
	return resp.MakeMultiBulkReply(args)
}
func hashToCmd(key string, h dict.Dict) *resp.MultiBulkReply {
	args := make([][]byte, 2+h.Len()*2)
	args[0] = hmSetCmd
	args[1] = []byte(key)
	i := 0
	h.ForEach(func(field string, val interface{}) bool {
		bytes, _ := val.([]byte)
		args[2+i*2] = []byte(field)
		args[3+i*2] = bytes
		i++
		return true
	})
	return resp.MakeMultiBulkReply(args)
}
func zSetToCmd(key string, zSet *sortedset.ZSet) *resp.MultiBulkReply {
	args := make([][]byte, 2+zSet.Len()*2)
	args[0] = zAddCmd
	args[1] = []byte(key)
	i := 0
	zSet.ForEach(int64(0), zSet.Len(), false, func(element *sortedset.Element) bool {
		value := strconv.FormatFloat(element.Score, 'f', -1, 64)
		args[2+i*2] = []byte(value)
		args[3+i*2] = []byte(element.Member)
		i++
		return true
	})
	return resp.MakeMultiBulkReply(args)

}
func expireToCmd(key string, expiration time.Time) *resp.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = pExpireAtBytesCmd
	args[1] = []byte(key)
	args[2] = []byte(strconv.FormatInt(expiration.UnixNano()/1e6, 10))
	return resp.MakeMultiBulkReply(args)
}
