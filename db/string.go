package db

import (
	"fmt"
	"mygodis/aof"
	cm "mygodis/common"
	"mygodis/common/commoninterface"
	"mygodis/datadriver/bitmap"
	"mygodis/resp"
	"mygodis/util/cmdutil"
	"strconv"
	"time"
)

const (
	put = uint8(iota) << 1
	putNx
	putXx
)
const (
	noEx = uint8(iota) << 1
	ex
	px
	exAt
	pxAt
)

type setPolicy struct {
	keepTTL      bool
	putPolicy    uint8
	expirePolicy uint8
	get          bool
	expireTime   time.Time
}

func (db *DataBaseImpl) getAsString(key string) ([]byte, resp.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	switch val := entity.Data.(type) {
	case []byte:
		return val, nil
	case string:
		return []byte(val), nil
	default:
		return nil, resp.MakeErrReply("value is not a string")
	}
}
func execGet(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 1 {
		return resp.MakeErrReply("wrong number of arguments for 'get' command")
	}
	key := string(args[0])
	val, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if val == nil {
		return resp.MakeNullBulkReply()
	}
	return resp.MakeBulkReply(val)
}
func execGetEx(db *DataBaseImpl, args cm.CmdLine) (res resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			res = resp.MakeSyntaxErrReply()
		}
	}()
	key := string(args[0])
	val, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if val == nil {
		return resp.MakeNullBulkReply()
	}
	flag := string(args[1])
	expireTime := time.Time{}
	switch flag {
	case "EX":
		expireSecond, err := strconv.Atoi(string(args[2]))
		if err != nil {
			return resp.MakeSyntaxErrReply()
		}
		expireTime = time.Now().Add(time.Duration(expireSecond) * time.Second)
	case "PX":
		expireMillisecond, err := strconv.Atoi(string(args[2]))
		if err != nil {
			return resp.MakeSyntaxErrReply()
		}
		expireTime = time.Now().Add(time.Duration(expireMillisecond) * time.Millisecond)

	case "EXAT":
		expireSecond, err := strconv.Atoi(string(args[2]))
		if err != nil {
			return resp.MakeSyntaxErrReply()
		}
		expireTime = time.Unix(int64(expireSecond), 0)

	case "PXAT":
		expireMillisecond, err := strconv.Atoi(string(args[2]))
		if err != nil {
			return resp.MakeSyntaxErrReply()
		}
		expireTime = time.UnixMilli(int64(expireMillisecond))
	case "PERSIST":
		db.Persist(key)
		db.addAof(cmdutil.ToCmdLine("persist", key))
	default:
		return resp.MakeErrReply("syntax error unknown flag " + flag)
	}
	if !expireTime.IsZero() {
		db.Expire(key, expireTime)
		db.addAof(aof.ExpireToCmd(key, expireTime).Args)
	}
	return resp.MakeBulkReply(val)
}
func execSet(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	policy := setPolicy{
		putPolicy:    put,
		expirePolicy: noEx,
		get:          false,
		keepTTL:      false,
	}
	key := string(args[0])
	value := args[1]
	data := new(commoninterface.DataEntity)
	data.Data = value
	var line cm.CmdLine
	if len(args) > 2 {
		line = args[2:]
		expireTime := time.Time{}
		var oldVal []byte
		parseErr := parseSet(line, &policy)
		if parseErr != nil {
			return resp.MakeErrReply(parseErr.Error())
		}
		if policy.expireTime.Before(time.Now()) && policy.expirePolicy != noEx {
			return resp.MakeNullBulkReply()
		} else {
			expireTime = policy.expireTime
		}
		if policy.get {
			oldVal, _ = db.getAsString(key)
			if (oldVal == nil && policy.putPolicy == putXx) || (oldVal != nil && policy.putPolicy == putNx) {
				return resp.MakeNullBulkReply()
			}
		}
		if policy.putPolicy == put {
			db.PutEntity(key, data)
			if !policy.keepTTL {
				db.ttlMap.Remove(key)
			}
			if policy.expirePolicy != noEx {
				db.Expire(key, expireTime)
			}
		}
		if policy.putPolicy == putNx && oldVal == nil {
			db.PutEntity(key, data)
			if !policy.keepTTL {
				db.ttlMap.Remove(key)
			}
			if policy.expirePolicy != noEx {
				db.Expire(key, expireTime)
			}
		}
		if policy.putPolicy == putXx && oldVal != nil {
			db.PutEntity(key, data)
			if !policy.keepTTL {
				db.ttlMap.Remove(key)
			}
			if policy.expirePolicy != noEx {
				db.Expire(key, expireTime)
			}

		}
		if policy.get {
			return resp.MakeBulkReply(oldVal)
		}
	} else {
		db.PutEntity(key, data)
	}
	dump(db)
	return resp.MakeOkReply()
}
func parseSet(args cm.CmdLine, policy *setPolicy) error {
	if len(args) == 0 {
		return nil
	}
	flag := string(args[0])
	switch flag {
	case "NX":
		if policy.putPolicy != put {
			return fmt.Errorf("syntax error")
		}
		policy.putPolicy = putNx
		return parseSet(args[1:], policy)
	case "XX":
		if policy.putPolicy != put {
			return fmt.Errorf("syntax error")
		}
		policy.putPolicy = putXx
		return parseSet(args[1:], policy)
	case "EX":
		if policy.keepTTL || policy.expirePolicy != noEx {
			return fmt.Errorf("syntax error")
		}
		policy.expirePolicy = ex
		second, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return err
		}
		policy.expireTime = time.Now().Add(time.Duration(second) * time.Second)
		return parseSet(args[2:], policy)
	case "PX":
		if policy.keepTTL || policy.expirePolicy != noEx {
			return fmt.Errorf("syntax error")
		}
		policy.expirePolicy = px
		s := string(args[1])
		millisecond, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		policy.expireTime = time.Now().Add(time.Duration(millisecond) * time.Millisecond)
		return parseSet(args[2:], policy)
	case "EXAT":
		if policy.keepTTL || policy.expirePolicy != noEx {
			return fmt.Errorf("syntax error")
		}
		policy.expirePolicy = exAt
		s := string(args[1])
		second, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		policy.expireTime = time.Unix(int64(second), 0)
		return parseSet(args[2:], policy)
	case "PXAT":
		if policy.keepTTL || policy.expirePolicy != noEx {
			return fmt.Errorf("syntax error")
		}
		policy.expirePolicy = pxAt
		s := string(args[1])
		millisecond, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		policy.expireTime = time.UnixMilli(int64(millisecond))
		return parseSet(args[2:], policy)
	case "KEEPTTL":
		if policy.expirePolicy != noEx {
			return fmt.Errorf("syntax error")
		}
		policy.keepTTL = true
		return parseSet(args[1:], policy)
	case "GET":
		policy.get = true
	default:
		return fmt.Errorf("syntax error unknown flag %s", flag)
	}
	return fmt.Errorf("syntax error unknown flag %s", flag)
}
func execSetNx(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	value := args[1]
	data := new(commoninterface.DataEntity)
	data.Data = value
	if db.PutAbsent(key, data) > 0 {
		return resp.MakeIntReply(1)
	}
	return resp.MakeIntReply(0)
}
func execSetEx(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	value := args[1]
	data := new(commoninterface.DataEntity)
	data.Data = value
	expireMillisecond, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	expireTime := time.Now().Add(time.Duration(expireMillisecond) * time.Millisecond)
	db.PutEntity(key, data)
	db.Expire(key, expireTime)
	return resp.MakeOkReply()
}
func execPSetEx(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	value := args[1]
	data := new(commoninterface.DataEntity)
	data.Data = value
	expireMillisecond, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	expireTime := time.Now().Add(time.Duration(expireMillisecond) * time.Millisecond)
	db.PutEntity(key, data)
	db.Expire(key, expireTime)
	db.addAof(aof.ExpireToCmd(key, expireTime).Args)
	return resp.MakeOkReply()
}
func execMSet(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args)%2 != 0 {
		return resp.MakeErrReply("wrong number of arguments for 'mset' command")
	}
	for i := 0; i < len(args); i += 2 {
		key := string(args[i])
		value := string(args[i+1])
		data := new(commoninterface.DataEntity)
		data.Data = value
		db.PutEntity(key, data)
	}
	return resp.MakeOkReply()
}
func execMGet(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	strings := make([][]byte, 0, len(args)/2)
	for i := 0; i < len(args); i++ {
		key := string(args[i])
		value, err := db.getAsString(key)
		if err != nil {
			reply := err.(*resp.WrongTypeErrReply)
			if reply != nil {
				strings = append(strings, nil)
				continue
			} else {
				return resp.MakeErrReply(err.Error())
			}
		}
		strings = append(strings, value)
	}
	return resp.MakeMultiBulkReply(strings)
}
func execMSetNx(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args)%2 != 0 {
		return resp.MakeErrReply("wrong number of arguments for 'mset' command")
	}
	for i := 0; i < len(args); i += 2 {
		key := string(args[i])
		value := string(args[i+1])
		data := new(commoninterface.DataEntity)
		data.Data = value
		if db.PutAbsent(key, data) == 0 {
			return resp.MakeIntReply(0)
		}
	}
	return resp.MakeIntReply(1)
}
func execGetSet(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	value := args[1]
	data := new(commoninterface.DataEntity)
	data.Data = value
	oldValue, err := db.getAsString(key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	db.PutEntity(key, data)
	if oldValue == nil {
		return resp.MakeNullBulkReply()
	}
	return resp.MakeBulkReply(oldValue)
}
func execGetDel(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	oldValue, err := db.getAsString(key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	db.Remove(key)
	if oldValue == nil {
		return resp.MakeNullBulkReply()
	}
	return resp.MakeBulkReply(oldValue)
}
func execIncr(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	value, err := db.getAsString(key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if value == nil {
		db.PutEntity(key, new(commoninterface.DataEntity))
		return resp.MakeIntReply(1)
	}
	intValue, parseErr := strconv.Atoi(string(value))
	if parseErr != nil {
		return resp.MakeErrReply(err.Error())
	}
	intValue++
	db.PutEntity(key, new(commoninterface.DataEntity))
	return resp.MakeIntReply(int64(intValue))
}
func execIncrBy(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	value, err := db.getAsString(key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if value == nil {
		db.PutEntity(key, new(commoninterface.DataEntity))
		return resp.MakeIntReply(1)
	}
	intValue, parseErr := strconv.Atoi(string(value))
	if parseErr != nil {
		return resp.MakeErrReply(err.Error())
	}
	increment, parseErr := strconv.Atoi(string(args[1]))
	if parseErr != nil {
		return resp.MakeErrReply(err.Error())
	}
	intValue += increment
	db.PutEntity(key, new(commoninterface.DataEntity))
	return resp.MakeIntReply(int64(intValue))
}
func execIncrByFloat(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	value, err := db.getAsString(key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if value == nil {
		db.PutEntity(key, new(commoninterface.DataEntity))
		return resp.MakeIntReply(1)
	}
	floatValue, parseErr := strconv.ParseFloat(string(value), 64)
	if parseErr != nil {
		return resp.MakeErrReply(err.Error())
	}
	increment, parseErr := strconv.ParseFloat(string(args[1]), 64)
	if parseErr != nil {
		return resp.MakeErrReply(err.Error())
	}
	floatValue += increment
	db.PutEntity(key, new(commoninterface.DataEntity))
	return resp.MakeIntReply(int64(floatValue))
}
func execDecr(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	value, err := db.getAsString(key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if value == nil {
		db.PutEntity(key, new(commoninterface.DataEntity))
		return resp.MakeIntReply(-1)
	}
	intValue, parseErr := strconv.Atoi(string(value))
	if parseErr != nil {
		return resp.MakeErrReply(err.Error())
	}
	intValue--
	db.PutEntity(key, new(commoninterface.DataEntity))
	return resp.MakeIntReply(int64(intValue))
}
func execDecrBy(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	value, err := db.getAsString(key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if value == nil {
		db.PutEntity(key, new(commoninterface.DataEntity))
		return resp.MakeIntReply(-1)
	}
	intValue, parseErr := strconv.Atoi(string(value))
	if parseErr != nil {
		return resp.MakeErrReply(err.Error())
	}
	decrement, parseErr := strconv.Atoi(string(args[1]))
	if parseErr != nil {
		return resp.MakeErrReply(err.Error())
	}
	intValue -= decrement
	db.PutEntity(key, new(commoninterface.DataEntity))
	return resp.MakeIntReply(int64(intValue))
}
func execStrLen(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	value, err := db.getAsString(key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if value == nil {
		return resp.MakeIntReply(0)
	}
	return resp.MakeIntReply(int64(len(value)))
}
func execAppend(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 2 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'append' command")
	}
	key := string(args[0])
	value, err := db.getAsString(key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if value == nil {
		value = []byte{}
	}
	value = append(value, args[1]...)
	entity := new(commoninterface.DataEntity)
	entity.Data = value
	db.PutEntity(key, entity)
	return resp.MakeIntReply(int64(len(value)))
}
func execGetRange(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 3 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'getrange' command")
	}
	key := string(args[0])
	value, err := db.getAsString(key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if value == nil {
		return resp.MakeNullBulkReply()
	}
	start, parseErr := strconv.Atoi(string(args[1]))
	if parseErr != nil {
		return resp.MakeErrReply(err.Error())
	}
	end, parseErr := strconv.Atoi(string(args[2]))
	if parseErr != nil {
		return resp.MakeErrReply(err.Error())
	}
	strLen := len(value)
	if start < 0 {
		start = strLen + start
	}
	if end < 0 {
		end = strLen + end
	}
	if start < 0 {
		start = 0
	}
	if end > strLen {
		end = strLen
	}
	if start > end {
		return resp.MakeNullBulkReply()
	}
	result := value[start:end]
	return resp.MakeBulkReply(result)
}
func execSetRange(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 3 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'setrange' command")
	}
	key := string(args[0])
	value, err := db.getAsString(key)

	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if value == nil {
		value = []byte{}
	}
	start, parseErr := strconv.Atoi(string(args[1]))
	if parseErr != nil {
		return resp.MakeErrReply(err.Error())
	}
	if start < 0 {
		start = 0
	}
	if start > len(value) {
		value = append(value, make([]byte, start-len(value)+len(args[2]))...)
	}
	copy(value[start:], args[2])
	entity := new(commoninterface.DataEntity)
	entity.Data = value
	db.PutEntity(key, entity)
	return resp.MakeIntReply(int64(len(value)))
}
func execSetBit(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	value, err := db.getAsString(key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	offset, offsetErr := strconv.ParseInt(string(args[1]), 10, 64)
	if offsetErr != nil {
		return resp.MakeErrReply(err.Error())
	}
	if offset < 0 {
		return resp.MakeErrReply("bit offset is not an integer or out of range")
	}
	bitValue, bitErr := strconv.Atoi(string(args[2]))
	if bitErr != nil {
		return resp.MakeErrReply(err.Error())
	}
	bitMap := bitmap.FromBytes(value)
	code := int64(bitMap.GetBit(offset))
	bitMap.SetBit(offset, byte(bitValue))
	entity := new(commoninterface.DataEntity)
	entity.Data = bitMap.ToBytes()
	db.PutEntity(key, entity)
	return resp.MakeIntReply(code)
}
func execGetBit(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	value, err := db.getAsString(key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	offset, offsetErr := strconv.ParseInt(string(args[1]), 10, 64)
	if offsetErr != nil {
		return resp.MakeErrReply(err.Error())
	}
	if offset < 0 {
		return resp.MakeErrReply("bit offset is not an integer or out of range")
	}
	bitMap := bitmap.FromBytes(value)
	code := int64(bitMap.GetBit(offset))
	return resp.MakeIntReply(code)
}
func execBitCount(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	value, err := db.getAsString(key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if len(value) == 0 {
		return resp.MakeIntReply(0)
	}
	bitMap := bitmap.FromBytes(value)
	if len(args) == 3 {
		start, startErr := strconv.ParseInt(string(args[1]), 10, 64)
		if startErr != nil {
			return resp.MakeErrReply(err.Error())
		}
		end, endErr := strconv.ParseInt(string(args[2]), 10, 64)
		if endErr != nil {
			return resp.MakeErrReply(err.Error())
		}
		if start < 0 {
			start = 0
		}
		if end > bitMap.BitSize() {
			end = bitMap.BitSize()
		}
		if start > end {
			return resp.MakeIntReply(0)
		}
		count := int64(0)
		bitMap.ForEachBit(start, end, func(offset int64, bit byte) bool {
			if bit == 1 {
				count++
			}
			return true
		})
		return resp.MakeIntReply(count)
	}
	count := int64(0)
	bitMap.ForEachBit(0, bitMap.BitSize(), func(offset int64, bit byte) bool {
		if bit == 1 {
			count++
		}
		return true
	})
	return resp.MakeIntReply(count)

}
func execBitOp(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	op := string(args[0])
	bitMap := bitmap.NewBitMap()
	switch op {
	case "AND":
		if len(args) < 3 {
			return resp.MakeErrReply("ERR syntax error")
		}
		bitMaps := make([]*bitmap.BitMap, 0, len(args)-2)
		for i := 2; i < len(args); i++ {
			value, err := db.getAsString(string(args[i]))
			if err != nil {
				return resp.MakeErrReply(err.Error())
			}
			if value == nil {
				return resp.MakeErrReply("ERR key not exists")
			}
			bitMaps = append(bitMaps, bitmap.FromBytes(value))
		}
		bitmap.And(bitMap, bitMaps...)
	case "OR":
		bitMaps := make([]*bitmap.BitMap, 0, len(args)-2)
		if len(args) < 3 {
			return resp.MakeErrReply("ERR syntax error")
		}
		for i := 2; i < len(args); i++ {
			value, err := db.getAsString(string(args[i]))
			if err != nil {
				return resp.MakeErrReply(err.Error())
			}
			if value == nil {
				return resp.MakeErrReply("ERR key not exists")
			}
			bitMaps = append(bitMaps, bitmap.FromBytes(value))
		}
		bitmap.Or(bitMap, bitMaps...)
	case "XOR":
		bitMaps := make([]*bitmap.BitMap, 0, len(args)-2)
		if len(args) < 3 {
			return resp.MakeErrReply("ERR syntax error")
		}
		for i := 2; i < len(args); i++ {
			value, err := db.getAsString(string(args[i]))
			if err != nil {
				return resp.MakeErrReply(err.Error())
			}
			if value == nil {
				return resp.MakeErrReply("ERR key not exists")
			}
			bitMaps = append(bitMaps, bitmap.FromBytes(value))
		}
		bitmap.Xor(bitMap, bitMaps...)
	case "NOT":
		if len(args) != 3 {
			return resp.MakeErrReply("ERR syntax error")
		}
		value, err := db.getAsString(string(args[2]))
		if err != nil {
			return resp.MakeErrReply(err.Error())
		}
		if value == nil {
			return resp.MakeErrReply("ERR key not exists")
		}
		bitmap.Not(bitMap, bitmap.FromBytes(value))
	default:
		return resp.MakeErrReply("ERR syntax error")
	}
	entity := new(commoninterface.DataEntity)
	entity.Data = bitMap.ToBytes()
	db.PutEntity(string(args[1]), entity)
	return resp.MakeIntReply(int64(len(bitMap.ToBytes())))
}
func undoSetBitCommands(db *DataBaseImpl, args cm.CmdLine) []cm.CmdLine {
	key := args[0]
	offset := args[1]
	value := args[2]
	bitString, reply := db.getAsString(string(key))
	exsist := false
	if reply != nil {
		return nil
	}
	if bitString != nil {
		exsist = true
	}
	if exsist {
		return []cm.CmdLine{cmdutil.ToCmdLine("SETBIT", string(key), string(offset), string(value))}
	} else {
		return []cm.CmdLine{cmdutil.ToCmdLine("DEL", string(key))}
	}
}
func undoBitOpCommands(db *DataBaseImpl, args cm.CmdLine) []cm.CmdLine {
	key := args[1]
	_, reply := db.getAsString(string(key))
	if reply != nil {
		return nil
	}
	return rollbackGivenKeys(db, string(key))
}
func undoMSetCommands(db *DataBaseImpl, args cm.CmdLine) []cm.CmdLine {
	keys := make([]string, 0, len(args)-1)
	for i := 1; args[i] != nil; i += 1 {
		keys = append(keys, string(args[i]))
	}
	return rollbackGivenKeys(db, keys...)
}

func init() {
	RegisterCommand("Get", execGet, readFirstKey, nil, 2, ReadOnly)
	RegisterCommand("MGet", execMGet, readAllKeys, nil, -2, ReadOnly)
	RegisterCommand("BitCount", execBitCount, readFirstKey, nil, 4, ReadOnly)
	RegisterCommand("GetBit", execGetBit, readFirstKey, nil, 3, ReadOnly)
	RegisterCommand("StrLen", execStrLen, readFirstKey, nil, 2, ReadOnly)
	RegisterCommand("GetRange", execGetRange, readFirstKey, nil, 4, ReadOnly)
	RegisterCommand("SetNx", execSetNx, writeFirstKey, rollbackFirstKey, 3, Write)
	RegisterCommand("MSetNx", execMSetNx, writeAllKeys, undoMSetCommands, -3, Write)
	RegisterCommand("PSetEx", execPSetEx, writeFirstKey, rollbackFirstKey, 4, Write)
	RegisterCommand("SetEx", execSetEx, writeFirstKey, rollbackFirstKey, 4, Write)
	RegisterCommand("Set", execSet, writeFirstKey, rollbackFirstKey, -3, Write)
	RegisterCommand("GetSet", execGetSet, writeFirstKey, rollbackFirstKey, 3, Write)
	RegisterCommand("GetDel", execGetDel, writeFirstKey, rollbackFirstKey, 2, Write)
	RegisterCommand("MSet", execMSet, writeAllKeys, undoMSetCommands, -3, Write)
	RegisterCommand("Append", execAppend, writeFirstKey, rollbackFirstKey, 3, Write)
	RegisterCommand("SetRange", execSetRange, writeFirstKey, rollbackFirstKey, 4, Write)
	RegisterCommand("Incr", execIncr, writeFirstKey, rollbackFirstKey, 2, Write)
	RegisterCommand("IncrBy", execIncrBy, writeFirstKey, rollbackFirstKey, 3, Write)
	RegisterCommand("IncrByFloat", execIncrByFloat, writeFirstKey, rollbackFirstKey, 3, Write)
	RegisterCommand("Decr", execDecr, writeFirstKey, rollbackFirstKey, 2, Write)
	RegisterCommand("DecrBy", execDecrBy, writeFirstKey, rollbackFirstKey, 3, Write)
	RegisterCommand("SetBit", execSetBit, writeFirstKey, undoSetBitCommands, 4, Write)
	RegisterCommand("BitOp", execBitOp, writeFirstKey, undoBitOpCommands, -4, Write)

}
