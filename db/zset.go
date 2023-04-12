package db

import (
	cm "mygodis/common"
	"mygodis/common/commoninterface"
	"mygodis/datadriver/sortedset"
	"mygodis/resp"
	to "mygodis/util/ternaryoperator"
	"strconv"
)

type zRangePolicy struct {
	start, stop   *sortedset.ScoreBorder
	withScores    bool
	rev           bool
	offset, count int64
	isByLex       bool
}
type zSetPolicy struct {
	destKey    string
	zsets      []*sortedset.ZSet
	weights    []float64
	aggregate  string
	limit      int
	withScores bool
}

func parsePolicy(db *DataBaseImpl, args cm.CmdLine, policy *zSetPolicy) resp.ErrorReply {
	if len(args) == 0 {
		return nil
	}
	if policy.weights == nil && policy.zsets == nil {
		if numKeys, err := strconv.Atoi(string(args[0])); err == nil {
			policy.zsets = make([]*sortedset.ZSet, numKeys)
			for i := 0; i < numKeys; i++ {
				zset, err := db.getAsZSet(string(args[1+i]))
				if err != nil {
					return err
				}
				policy.zsets[i] = zset
			}
			policy.weights = make([]float64, numKeys)
			for i := 0; i < numKeys; i++ {
				policy.weights[i] = 1
			}
			return parsePolicy(db, args[1+numKeys:], policy)
		} else {
			flag := string(args[0])
			switch flag {
			case "WEIGHTS":
				for i := 0; i < len(policy.weights); i++ {
					weight, err := strconv.ParseFloat(string(args[1+i]), 64)
					if err != nil {
						return &resp.SyntaxErrReply{}
					}
					policy.weights[i] = weight
				}
				return parsePolicy(db, args[1+len(policy.weights):], policy)
			case "AGGREGATE":
				policy.aggregate = string(args[1])
				return parsePolicy(db, args[2:], policy)
			case "LIMIT":
				limit, err := strconv.Atoi(string(args[1]))
				if err != nil {
					return &resp.SyntaxErrReply{}
				}
				policy.limit = limit
				return parsePolicy(db, args[2:], policy)
			case "WITHSCORES":
				policy.withScores = true
				return parsePolicy(db, args[1:], policy)
			}
		}
	}
	return nil
}
func (db *DataBaseImpl) getAsZSet(key string) (*sortedset.ZSet, resp.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	zset, ok := entity.Data.(*sortedset.ZSet)
	if !ok {
		return nil, &resp.WrongTypeErrReply{}
	}
	return zset, nil
}
func (db *DataBaseImpl) getOrCreateZSet(key string) (z *sortedset.ZSet, isNew bool) {
	entity, exists := db.GetEntity(key)
	if !exists {
		z = sortedset.MakeZSet()
		return z, true
	}
	zset, ok := entity.Data.(*sortedset.ZSet)
	if !ok {
		return nil, false
	}
	return zset, false
}
func execZAdd(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) < 3 || len(args)%2 != 1 {
		return &resp.SyntaxErrReply{}
	}
	key := string(args[0])
	zset, isNew := db.getOrCreateZSet(key)
	if zset == nil {
		return &resp.WrongTypeErrReply{}
	}
	var added int
	for i := 1; i < len(args); i += 2 {
		score, err := strconv.ParseFloat(string(args[i]), 64)
		if err != nil {
			return &resp.SyntaxErrReply{}
		}
		member := args[i+1]
		if zset.Add(string(member), score) {
			added++
		}
	}
	if isNew {
		data := new(commoninterface.DataEntity)
		data.Data = zset
		db.PutEntity(key, data)
	}
	return resp.MakeIntReply(int64(added))
}
func execZCard(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 1 {
		return &resp.SyntaxErrReply{}
	}
	key := string(args[0])
	zset, err := db.getAsZSet(key)
	if err != nil {
		return err
	}
	if zset == nil {
		return resp.MakeIntReply(0)
	}
	return resp.MakeIntReply(zset.Len())
}
func execZIncrBy(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	if len(args) != 3 {
		return &resp.SyntaxErrReply{}
	}
	key := string(args[0])
	zset, isNew := db.getOrCreateZSet(key)
	if zset == nil {
		return &resp.WrongTypeErrReply{}
	}
	delta, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		return &resp.SyntaxErrReply{}
	}
	member := string(args[2])
	element, ok := zset.Get(member)

	if ok {
		element.Score += delta
	} else {
		zset.Add(member, delta)
	}
	if isNew {
		data := new(commoninterface.DataEntity)
		data.Data = zset
		db.PutEntity(key, data)
	}
	return resp.MakeBulkReply([]byte(strconv.FormatFloat(element.Score, 'f', -1, 64)))
}
func execZInter(db *DataBaseImpl, args cm.CmdLine) (result resp.Reply) {
	defer func() {
		if r := recover(); r != nil {
			result = &resp.SyntaxErrReply{}
			return
		}
	}()
	policy := new(zSetPolicy)
	if err := parsePolicy(db, args, policy); err != nil {
		return err
	}
	resultSet := policy.zsets[0].Inter(policy.aggregate, policy.weights, policy.zsets[1:]...)
	bytes := make([][]byte, 0, to.Which(policy.withScores, resultSet.Len()*2, resultSet.Len()))
	resultSet.ForEach(0, resultSet.Len(), false, func(element *sortedset.Element) bool {
		bytes = to.Which(policy.withScores, append(bytes, []byte(element.Member), []byte(strconv.FormatFloat(element.Score, 'f', -1, 64))), append(bytes, []byte(element.Member)))
		return true
	})
	return resp.MakeMultiBulkReply(bytes)
}
func execZInterStore(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	defer func() {
		if r := recover(); r != nil {
			return
		}
	}()
	policy := new(zSetPolicy)
	policy.destKey = string(args[0])
	if err := parsePolicy(db, args[1:], policy); err != nil {
		return err
	}
	resultSet := policy.zsets[0].Inter(policy.aggregate, policy.weights, policy.zsets[1:]...)
	bytes := make([][]byte, 0, to.Which(policy.withScores, resultSet.Len()*2, resultSet.Len()))
	resultSet.ForEach(0, resultSet.Len(), false, func(element *sortedset.Element) bool {
		bytes = to.Which(policy.withScores, append(bytes, []byte(element.Member), []byte(strconv.FormatFloat(element.Score, 'f', -1, 64))), append(bytes, []byte(element.Member)))
		return true
	})
	data := new(commoninterface.DataEntity)
	data.Data = resultSet
	db.PutEntity(policy.destKey, data)
	return resp.MakeIntReply(resultSet.Len())
}
func execZUnion(db *DataBaseImpl, args cm.CmdLine) (result resp.Reply) {
	defer func() {
		if r := recover(); r != nil {
			result = &resp.SyntaxErrReply{}
			return
		}
	}()
	policy := new(zSetPolicy)
	if err := parsePolicy(db, args, policy); err != nil {
		return err
	}
	resultSet := policy.zsets[0].Union(policy.aggregate, policy.weights, policy.zsets[1:]...)
	bytes := make([][]byte, 0, to.Which(policy.withScores, resultSet.Len()*2, resultSet.Len()))
	resultSet.ForEach(0, resultSet.Len(), false, func(element *sortedset.Element) bool {
		bytes = to.Which(policy.withScores, append(bytes, []byte(element.Member), []byte(strconv.FormatFloat(element.Score, 'f', -1, 64))), append(bytes, []byte(element.Member)))
		return true
	})
	return resp.MakeMultiBulkReply(bytes)
}
func execZUnionStore(db *DataBaseImpl, args cm.CmdLine) (result resp.Reply) {
	defer func() {
		if r := recover(); r != nil {
			result = &resp.SyntaxErrReply{}
			return
		}
	}()
	policy := new(zSetPolicy)
	policy.destKey = string(args[0])
	if err := parsePolicy(db, args[1:], policy); err != nil {
		return err
	}
	resultSet := policy.zsets[0].Union(policy.aggregate, policy.weights, policy.zsets[1:]...)
	bytes := make([][]byte, 0, to.Which(policy.withScores, resultSet.Len()*2, resultSet.Len()))
	resultSet.ForEach(0, resultSet.Len(), false, func(element *sortedset.Element) bool {
		bytes = to.Which(policy.withScores, append(bytes, []byte(element.Member), []byte(strconv.FormatFloat(element.Score, 'f', -1, 64))), append(bytes, []byte(element.Member)))
		return true
	})
	data := new(commoninterface.DataEntity)
	data.Data = resultSet
	db.PutEntity(policy.destKey, data)
	return resp.MakeIntReply(resultSet.Len())
}
func execZDiff(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	defer func() {
		if r := recover(); r != nil {
			return
		}
	}()
	policy := new(zSetPolicy)
	if err := parsePolicy(db, args, policy); err != nil {
		return err
	}
	resultSet := policy.zsets[0].Diff(policy.zsets[1:]...)
	bytes := make([][]byte, 0, to.Which(policy.withScores, resultSet.Len()*2, resultSet.Len()))
	resultSet.ForEach(0, resultSet.Len(), false, func(element *sortedset.Element) bool {
		bytes = to.Which(policy.withScores, append(bytes, []byte(element.Member), []byte(strconv.FormatFloat(element.Score, 'f', -1, 64))), append(bytes, []byte(element.Member)))
		return true
	})
	return resp.MakeMultiBulkReply(bytes)
}
func execZDiffStore(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	defer func() {
		if r := recover(); r != nil {
			return
		}
	}()
	policy := new(zSetPolicy)
	policy.destKey = string(args[0])
	if err := parsePolicy(db, args[1:], policy); err != nil {
		return err
	}
	resultSet := policy.zsets[0].Diff(policy.zsets[1:]...)
	bytes := make([][]byte, 0, to.Which(policy.withScores, resultSet.Len()*2, resultSet.Len()))
	resultSet.ForEach(0, resultSet.Len(), false, func(element *sortedset.Element) bool {
		bytes = to.Which(policy.withScores, append(bytes, []byte(element.Member), []byte(strconv.FormatFloat(element.Score, 'f', -1, 64))), append(bytes, []byte(element.Member)))
		return true
	})
	data := new(commoninterface.DataEntity)
	data.Data = resultSet
	db.PutEntity(policy.destKey, data)
	return resp.MakeIntReply(resultSet.Len())
}
func execZInterCard(db *DataBaseImpl, args cm.CmdLine) (result resp.Reply) {
	defer func() {
		if r := recover(); r != nil {
			result = &resp.SyntaxErrReply{}
			return
		}
	}()
	policy := new(zSetPolicy)
	if err := parsePolicy(db, args, policy); err != nil {
		return err
	}
	//TODO 优化
	resultSet := policy.zsets[0].Inter(policy.aggregate, policy.weights, policy.zsets[1:]...)
	return resp.MakeIntReply(to.Which(policy.withScores, resultSet.Len()*2, resultSet.Len()))
}
func execZScore(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	zset, _ := db.getAsZSet(key)
	if zset == nil {
		return resp.MakeNullBulkReply()
	}
	member := string(args[1])
	element, ok := zset.Get(member)
	if !ok {
		return resp.MakeNullBulkReply()
	}
	return resp.MakeBulkReply([]byte(strconv.FormatFloat(element.Score, 'f', -1, 64)))
}
func execZRange(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	zset, _ := db.getAsZSet(key)
	if zset == nil {
		return resp.MakeNullBulkReply()
	}
	rangePolicy, err := parseRangePolicy(args[1:])
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	result := zset.RangeByScore(rangePolicy.start, rangePolicy.stop, rangePolicy.offset, rangePolicy.count, rangePolicy.rev)
	bytes := make([][]byte, 0, to.Which(rangePolicy.withScores, len(result)*2, len(result)))
	for _, element := range result {
		bytes = to.Which(rangePolicy.withScores, append(bytes, []byte(element.Member), []byte(strconv.FormatFloat(element.Score, 'f', -1, 64))), append(bytes, []byte(element.Member)))
	}
	return resp.MakeMultiBulkReply(bytes)
}
func parseRangePolicy(args cm.CmdLine) (policy *zRangePolicy, err error) {
	if len(args) == 0 {
		return nil, nil
	}
	if string(args[len(args)-1]) == "WITHSCORES" {
		policy.withScores = true
		return nil, nil
	}
	flag := string(args[0])
	switch flag {
	case "BYSCORE":
		policy.isByLex = false
		return parseRangePolicy(args[1:])
	case "BYLEX":
		policy.isByLex = true
		return parseRangePolicy(args[1:])
	case "LIMIT":
		policy.offset, _ = strconv.ParseInt(string(args[1]), 10, 64)
		policy.count, _ = strconv.ParseInt(string(args[2]), 10, 64)
		return parseRangePolicy(args[3:])
	case "REV":
		policy.rev = true
		return parseRangePolicy(args[1:])
	default:
		start, err := sortedset.ParseScoreBorder(string(args[0]))
		if err != nil {
			return nil, err
		}
		stop, err := sortedset.ParseScoreBorder(string(args[1]))
		if err != nil {
			return nil, err
		}
		policy.start = start
		policy.stop = stop
		return parseRangePolicy(args[2:])
	}

}
func execZRangeStore(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	defer func() {
		if r := recover(); r != nil {
			return
		}
	}()
	dest := string(args[0])
	src := string(args[1])
	rangePolicy, err := parseRangePolicy(args[2:])
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	zset, _ := db.getAsZSet(src)
	if zset == nil {
		return resp.MakeIntReply(0)
	}
	result := zset.RangeByScore(rangePolicy.start, rangePolicy.stop, rangePolicy.offset, rangePolicy.count, rangePolicy.rev)
	destSets, isNew := db.getOrCreateZSet(dest)
	for _, element := range result {
		destSets.Add(element.Member, element.Score)
	}
	data := new(commoninterface.DataEntity)
	data.Data = destSets

	if isNew {
		db.PutEntity(dest, data)

	}
	return resp.MakeIntReply(destSets.Len())
}
func execZRem(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	zset, _ := db.getAsZSet(key)
	if zset == nil {
		return resp.MakeIntReply(0)
	}
	count := 0
	for i := 1; i < len(args); i++ {
		member := string(args[i])
		if zset.Remove(member) {
			count++
		}
	}
	return resp.MakeIntReply(int64(count))
}
func execZRank(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])

	zset, _ := db.getAsZSet(key)
	if zset == nil {
		return resp.MakeNullBulkReply()
	}
	member := string(args[1])
	rank, ok := zset.Rank(member)
	element, _ := zset.Get(member)
	score := element.Score
	if !ok {
		return resp.MakeNullBulkReply()
	}
	if string(args[len(args)-1]) == "WITHSCORES" {
		return resp.MakeMultiBulkReply([][]byte{[]byte(strconv.FormatInt(rank, 10)), []byte(strconv.FormatFloat(score, 'f', -1, 64))})
	}
	return resp.MakeIntReply(rank)
}
func execZCount(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	zset, _ := db.getAsZSet(key)
	if zset == nil {
		return resp.MakeIntReply(0)
	}
	min, err := sortedset.ParseScoreBorder(string(args[1]))
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	max, err := sortedset.ParseScoreBorder(string(args[2]))
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	return resp.MakeIntReply(zset.Count(min, max))
}
func execZRevRank(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	zset, _ := db.getAsZSet(key)
	if zset == nil {
		return resp.MakeNullBulkReply()
	}
	member := string(args[1])
	rank, ok := zset.Rank(member)
	element, _ := zset.Get(member)
	score := element.Score
	if !ok {
		return resp.MakeNullBulkReply()
	}
	if string(args[len(args)-1]) == "WITHSCORES" {
		return resp.MakeMultiBulkReply([][]byte{[]byte(strconv.FormatInt(rank, 10)), []byte(strconv.FormatFloat(score, 'f', -1, 64))})
	}
	return resp.MakeIntReply(rank)
}
func execZLexCount(db *DataBaseImpl, args cm.CmdLine) resp.Reply {
	key := string(args[0])
	zset, _ := db.getAsZSet(key)
	if zset == nil {
		return resp.MakeIntReply(0)
	}
	min := string(args[1])
	max := string(args[2])
	return resp.MakeIntReply(zset.LexCount(min, max))
}
func undoZAddCommands(db *DataBaseImpl, args cm.CmdLine) []cm.CmdLine {
	key := string(args[0])
	zset, _ := db.getAsZSet(key)
	if zset == nil {
		return nil
	}
	zsetMembers := getZsetMember(args[1:])
	return rollbackZsetMember(db, key, zsetMembers...)
}
func undoZRemCommands(db *DataBaseImpl, args cm.CmdLine) []cm.CmdLine {
	key := string(args[0])
	zset, _ := db.getAsZSet(key)
	if zset == nil {
		return nil
	}
	zsetMembers := getZsetMember(args[1:])
	return rollbackZsetMember(db, key, zsetMembers...)
}
func undoZIncrByCommands(db *DataBaseImpl, args cm.CmdLine) []cm.CmdLine {
	key := string(args[0])
	zset, _ := db.getAsZSet(key)
	if zset == nil {
		return nil
	}
	zsetMembers := getZsetMember(args[1:])
	return rollbackZsetMember(db, key, zsetMembers...)
}
func getZsetMember(args cm.CmdLine) []string {
	var members []string
	for i := 1; i < len(args); i += 2 {
		members = append(members, string(args[i]))
	}
	return members
}

// todo support zset
func init() {
	//RegisterCommand("zlexcount", execZLexCount, readFirstKey, nil, 3, ReadOnly)
	//RegisterCommand("zrange", execZRange, readFirstKey, nil, -3, ReadOnly)
	//RegisterCommand("zcard", execZCard, readFirstKey, nil, 1, ReadOnly)
	//RegisterCommand("zcount", execZCount, readFirstKey, nil, 3, ReadOnly)
	//RegisterCommand("zrank", execZRank, readFirstKey, nil, 2, ReadOnly)
	//RegisterCommand("zrevrank", execZRevRank, readFirstKey, nil, 2, ReadOnly)
	//RegisterCommand("zinter", execZInter, readFirstKey, nil, -3, ReadOnly)
	//RegisterCommand("zunion", execZUnion, readFirstKey, nil, -3, ReadOnly)
	//RegisterCommand("zdiff", execZDiff, readFirstKey, nil, -3, ReadOnly)
	//RegisterCommand("zintercard", execZInterCard, readFirstKey, nil, -3, ReadOnly)
	//RegisterCommand("zscore", execZScore, readFirstKey, nil, 2, ReadOnly)
	//
	//RegisterCommand("zadd", execZAdd, writeFirstKey, undoZAddCommands, -2, Write)
	//RegisterCommand("zincrby", execZIncrBy, writeFirstKey, undoZIncrByCommands, 3, Write)
	//RegisterCommand("zrem", execZRem, writeFirstKey, undoZRemCommands, -2, Write)
	//RegisterCommand("zinterstore", execZInterStore, writeFirstKey, rollbackFirstKey, -3, Write)
	//RegisterCommand("zunionstore", execZUnionStore, writeFirstKey, rollbackFirstKey, -3, Write)
	//RegisterCommand("zdiffstore", execZDiffStore, writeFirstKey, rollbackFirstKey, -3, Write)
	//RegisterCommand("zrangestore", execZRangeStore, writeFirstKey, rollbackFirstKey, -3, Write)
}
