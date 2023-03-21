package db

import (
	"mygodis/datadriver/sortedset"
	"mygodis/resp"
)

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
