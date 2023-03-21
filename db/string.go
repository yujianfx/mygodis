package db

import (
	"mygodis/resp"
)

func (db *DataBaseImpl) getAsString(key string) ([]byte, resp.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	switch val := entity.Data.(type) {
	case []byte:
		return val, nil
	default:
		return nil, resp.MakeErrReply("value is not a string")
	}
}
