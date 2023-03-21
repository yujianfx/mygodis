package db

import (
	"mygodis/datadriver/set"
	"mygodis/resp"
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
