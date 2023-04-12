package db

import (
	cm "mygodis/common"
	"mygodis/common/commoninterface"
	"mygodis/datadriver/set"
	"mygodis/resp"
	"mygodis/util/cmdutil"
	"strconv"
)

func (db *DataBaseImpl) getAsSet(key string) (*set.Set, resp.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	set, ok := entity.Data.(*set.Set)
	if !ok {
		return nil, &resp.WrongTypeErrReply{}
	}
	return set, nil
}
func (db *DataBaseImpl) getOrCreateSet(key string) (result *set.Set, isNew bool) {
	entity, exists := db.GetEntity(key)
	if !exists {
		entity = new(commoninterface.DataEntity)
		entity.Data = set.MakeSet()

		return entity.Data.(*set.Set), true
	}
	if set, ok := entity.Data.(*set.Set); ok {
		return set, false
	}
	return nil, false
}
func execSAdd(db *DataBaseImpl, args cm.CmdLine) (reply resp.Reply) {
	if len(args) < 2 {
		return resp.MakeErrReply("wrong number of arguments for 'sadd' command")
	}
	key := string(args[0])
	set, isNew := db.getOrCreateSet(key)
	if set == nil {
		return &resp.WrongTypeErrReply{}
	}
	added := 0
	for i := 1; i < len(args); i++ {
		added += set.Add(string(args[i]))
	}
	if isNew {
		db.PutEntity(key, &commoninterface.DataEntity{
			Data: set,
		})
	}
	db.addAof(cmdutil.ToCmdLineWithBytes("sadd", args...))
	return resp.MakeIntReply(int64(added))
}
func execSCard(db *DataBaseImpl, args cm.CmdLine) (reply resp.Reply) {
	if len(args) != 1 {
		return resp.MakeErrReply("wrong number of arguments for 'scard' command")
	}
	key := string(args[0])
	set, err := db.getAsSet(key)
	if err != nil {
		return err
	}
	if set == nil {
		return resp.MakeIntReply(0)
	}
	return resp.MakeIntReply(int64(set.Len()))
}
func execSDiff(db *DataBaseImpl, args cm.CmdLine) (reply resp.Reply) {
	if len(args) < 2 {
		return resp.MakeErrReply("wrong number of arguments for 'sdiff' command")
	}
	sets := make([]*set.Set, 0, len(args))
	for _, arg := range args {
		key := string(arg)
		getAsSet, err := db.getAsSet(key)
		if err != nil {
			return err
		}
		if getAsSet == nil {
			return resp.MakeMultiBulkReply(nil)
		}
		sets = append(sets, getAsSet)
	}
	result := sets[0]
	for i := 1; i < len(sets); i++ {
		result = result.Diff(sets[i])
	}

	slice := result.ToSlice()
	r := make([][]byte, len(slice))
	for i := 0; i < len(slice); i++ {
		r[i] = []byte(slice[i])
	}

	return resp.MakeMultiBulkReply(r)
}
func execSDiffStore(db *DataBaseImpl, args cm.CmdLine) (reply resp.Reply) {
	if len(args) < 3 {
		return resp.MakeErrReply("wrong number of arguments for 'sdiffstore' command")
	}
	destKey := string(args[0])
	sets := make([]*set.Set, 0, len(args)-1)
	for _, arg := range args[1:] {
		key := string(arg)
		set, err := db.getAsSet(key)
		if err != nil {
			return err
		}
		if set == nil {
			return resp.MakeIntReply(0)
		}
		sets = append(sets, set)
	}
	result := sets[0]
	for i := 1; i < len(sets); i++ {
		result = result.Diff(sets[i])
	}
	db.PutEntity(destKey, &commoninterface.DataEntity{
		Data: result,
	})
	db.addAof(cmdutil.ToCmdLineWithBytes("sdiffstore", args...))
	return resp.MakeIntReply(int64(result.Len()))
}
func execSInter(db *DataBaseImpl, args cm.CmdLine) (reply resp.Reply) {
	if len(args) < 2 {
		return resp.MakeErrReply("wrong number of arguments for 'sinter' command")
	}
	sets := make([]*set.Set, 0, len(args))
	for _, arg := range args {
		key := string(arg)
		getAsSet, err := db.getAsSet(key)
		if err != nil {
			return err
		}
		if getAsSet == nil {
			return resp.MakeMultiBulkReply(nil)
		}
		sets = append(sets, getAsSet)
	}
	result := sets[0]
	for i := 1; i < len(sets); i++ {
		result = result.Inter(sets[i])
	}

	slice := result.ToSlice()
	r := make([][]byte, len(slice))
	for i := 0; i < len(slice); i++ {
		r[i] = []byte(slice[i])
	}

	return resp.MakeMultiBulkReply(r)
}
func execSInterStore(db *DataBaseImpl, args cm.CmdLine) (reply resp.Reply) {
	if len(args) < 3 {
		return resp.MakeErrReply("wrong number of arguments for 'sinterstore' command")
	}
	destKey := string(args[0])
	sets := make([]*set.Set, 0, len(args)-1)
	for _, arg := range args[1:] {
		key := string(arg)
		set, err := db.getAsSet(key)
		if err != nil {
			return err
		}
		if set == nil {
			return resp.MakeIntReply(0)
		}
		sets = append(sets, set)
	}
	result := sets[0]
	for i := 1; i < len(sets); i++ {
		result = result.Inter(sets[i])
	}
	db.PutEntity(destKey, &commoninterface.DataEntity{
		Data: result,
	})
	db.addAof(cmdutil.ToCmdLineWithBytes("sinterstore", args...))
	return resp.MakeIntReply(int64(result.Len()))
}
func execSIsMember(db *DataBaseImpl, args cm.CmdLine) (reply resp.Reply) {
	if len(args) != 2 {
		return resp.MakeErrReply("wrong number of arguments for 'sismember' command")
	}
	key := string(args[0])
	set, err := db.getAsSet(key)
	if err != nil {
		return err
	}
	if set == nil {
		return resp.MakeIntReply(0)
	}
	r := int64(0)
	member := string(args[1])
	if set.Has(member) {
		r = 1
	}
	return resp.MakeIntReply(r)
}
func execSMembers(db *DataBaseImpl, args cm.CmdLine) (reply resp.Reply) {
	if len(args) != 1 {
		return resp.MakeErrReply("wrong number of arguments for 'smembers' command")
	}
	key := string(args[0])
	set, err := db.getAsSet(key)
	if err != nil {
		return err
	}
	if set == nil {
		return resp.MakeMultiBulkReply([][]byte{})
	}

	slice := set.ToSlice()
	r := make([][]byte, len(slice))
	for i := 0; i < len(slice); i++ {
		r[i] = []byte(slice[i])
	}

	return resp.MakeMultiBulkReply(r)
}
func execSMove(db *DataBaseImpl, args cm.CmdLine) (reply resp.Reply) {
	if len(args) != 3 {
		return resp.MakeErrReply("wrong number of arguments for 'smove' command")
	}
	srcKey := string(args[0])
	destKey := string(args[1])
	member := string(args[2])
	srcSet, err := db.getAsSet(srcKey)
	dest, isNew := db.getOrCreateSet(destKey)
	if err != nil {
		return err
	}
	if srcSet == nil {
		return resp.MakeIntReply(0)
	}
	if srcSet.Has(member) {
		srcSet.Remove(member)
		dest.Add(member)
		if isNew {
			db.PutEntity(destKey, &commoninterface.DataEntity{
				Data: dest,
			})
		}
		return resp.MakeIntReply(1)
	}
	db.addAof(cmdutil.ToCmdLineWithBytes("smove", args...))
	return resp.MakeIntReply(0)
}
func execSPop(db *DataBaseImpl, args cm.CmdLine) (reply resp.Reply) {
	key := string(args[0])
	set, err := db.getAsSet(key)
	if err != nil {
		return err
	}
	if set == nil {
		return resp.MakeNullBulkReply()
	}
	limit := 0
	if len(args) == 1 {
		limit = 1
		member := set.RandomMembers(limit)
		if member == nil {
			return resp.MakeNullBulkReply()
		}
		elem := member[0]
		remove := set.Remove(elem)
		if remove > 0 {
			return resp.MakeBulkReply([]byte(elem))
		}
		return resp.MakeNullBulkReply()
	}
	if len(args) == 2 {
		i, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return resp.MakeErrReply("value is not an integer or out of range")
		}
		limit = i
	}
	randomMembers := set.RandomMembers(limit)
	if randomMembers == nil {
		return resp.MakeNullBulkReply()
	}
	r := make([][]byte, len(randomMembers))
	for i := 0; i < len(randomMembers); i++ {
		set.Remove(randomMembers[i])
		r[i] = []byte(randomMembers[i])
	}
	db.addAof(cmdutil.ToCmdLineWithBytes("spop", args...))
	return resp.MakeMultiBulkReply(r)
}
func execSRandMember(db *DataBaseImpl, args cm.CmdLine) (reply resp.Reply) {
	key := string(args[0])
	set, err := db.getAsSet(key)
	if err != nil {
		return err
	}
	if set == nil {
		return resp.MakeMultiBulkReply(nil)
	}
	limit := 1
	if len(args) == 2 {
		i, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return resp.MakeErrReply("value is not an integer or out of range")
		}
		limit = i
	}
	member := set.RandomMembers(limit)
	if member == nil {
		return resp.MakeMultiBulkReply(nil)
	}
	r := make([][]byte, len(member))
	for i := 0; i < len(member); i++ {
		r[i] = []byte(member[i])
	}
	return resp.MakeMultiBulkReply(r)
}
func execSRem(db *DataBaseImpl, args cm.CmdLine) (reply resp.Reply) {
	if len(args) < 2 {
		return resp.MakeErrReply("wrong number of arguments for 'srem' command")
	}
	key := string(args[0])
	set, err := db.getAsSet(key)
	if err != nil {
		return err
	}
	if set == nil {
		return resp.MakeIntReply(0)
	}
	count := 0
	for _, arg := range args[1:] {
		member := string(arg)
		if set.Has(member) {
			set.Remove(member)
			count++
		}
	}
	db.addAof(cmdutil.ToCmdLineWithBytes("srem", args...))
	return resp.MakeIntReply(int64(count))
}
func execSUnion(db *DataBaseImpl, args cm.CmdLine) (reply resp.Reply) {
	if len(args) < 2 {
		return resp.MakeErrReply("wrong number of arguments for 'sunion' command")
	}
	result := make([][]byte, 0)
	src, err := db.getAsSet(string(args[0]))
	if err != nil {
		return err
	}
	for _, arg := range args[1:] {
		key := string(arg)
		set, err := db.getAsSet(key)
		if err != nil {
			return err
		}
		if set == nil {
			continue
		}
		src = src.Union(set)
	}
	if src == nil {
		return resp.MakeMultiBulkReply(nil)
	}
	slice := src.ToSlice()
	for i := 0; i < len(slice); i++ {
		result = append(result, []byte(slice[i]))
	}
	return resp.MakeMultiBulkReply(result)
}
func execSUnionStore(db *DataBaseImpl, args cm.CmdLine) (reply resp.Reply) {
	if len(args) < 3 {
		return resp.MakeErrReply("wrong number of arguments for 'sunionstore' command")
	}
	dest, isNew := getOrCreateList(db, string(args[0]))
	src, _ := db.getAsSet(string(args[1]))
	for _, arg := range args[2:] {
		key := string(arg)
		set, err := db.getAsSet(key)
		if err != nil {
			return err
		}
		if set == nil {
			continue
		}
		src = src.Union(set)
	}
	if src == nil {
		return resp.MakeIntReply(0)
	}
	slice := src.ToSlice()
	for i := 0; i < len(slice); i++ {
		dest.Add(slice[i])
	}
	if isNew {
		db.PutEntity(string(args[0]), &commoninterface.DataEntity{
			Data: dest,
		})
	}
	db.addAof(cmdutil.ToCmdLineWithBytes("sunionstore", args...))
	return resp.MakeIntReply(int64(len(slice)))
}

// TODO sscan
// func execSScan(db *DataBaseImpl, args cm.CmdLine) (reply resp.Reply) {
//
// }
func undoSAddCommands(db *DataBaseImpl, args cm.CmdLine) (undo []cm.CmdLine) {
	members := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		members[i-1] = string(args[i])
	}
	return rollbackSetMember(db, string(args[0]), members...)

}
func undoSMoveCommands(db *DataBaseImpl, args cm.CmdLine) (undo []cm.CmdLine) {
	undo = append(undo, rollbackSetMember(db, string(args[0]), string(args[2]))...)
	undo = append(undo, rollbackSetMember(db, string(args[1]), string(args[2]))...)
	return undo
}
func undoSRemCommands(db *DataBaseImpl, args cm.CmdLine) (undo []cm.CmdLine) {
	members := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		members[i-1] = string(args[i])
	}
	return rollbackSetMember(db, string(args[0]), members...)
}
func init() {
	RegisterCommand("scard", execSCard, readFirstKey, nil, 2, ReadOnly)
	RegisterCommand("sdiff", execSDiff, readAllKeys, nil, -3, ReadOnly)
	RegisterCommand("sinter", execSInter, readAllKeys, nil, -3, ReadOnly)
	RegisterCommand("sismember", execSIsMember, readFirstKey, nil, 3, ReadOnly)
	RegisterCommand("smembers", execSMembers, readFirstKey, nil, 2, ReadOnly)
	RegisterCommand("srandmember", execSRandMember, readFirstKey, nil, 2, ReadOnly)
	RegisterCommand("sunion", execSUnion, readAllKeys, nil, -3, ReadOnly)
	RegisterCommand("sadd", execSAdd, writeFirstKey, undoSAddCommands, -3, Write)
	RegisterCommand("sdiffstore", execSDiffStore, writeFirstKey, rollbackFirstKey, -3, Write)
	RegisterCommand("sinterstore", execSInterStore, writeFirstKey, rollbackFirstKey, -3, Write)
	RegisterCommand("smove", execSMove, writeFirstKey, undoSMoveCommands, 3, Write)
	RegisterCommand("spop", execSPop, writeFirstKey, rollbackFirstKey, 2, Write)
	RegisterCommand("srem", execSRem, writeFirstKey, undoSRemCommands, -2, Write)
	RegisterCommand("sunionstore", execSUnionStore, writeFirstKey, rollbackFirstKey, -3, Write)

}
