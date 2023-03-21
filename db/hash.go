package db

import (
	"mygodis/datadriver/dict"
	"mygodis/resp"
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
