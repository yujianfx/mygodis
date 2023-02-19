package db

import (
	"mygodis/clientc"
	"mygodis/resp"
	"time"
)

type DataEntity struct {
	Data interface{}
}
type KeyEventCallback func(dbIndex int, key string, entity *DataEntity)
type DB interface {
	Exec(connection clientc.Connection, args [][]byte) (reply resp.Reply)
	AfterClientClose(connection clientc.Connection)
	Close()
}
type SimpleDBEngine interface {
	DB
	ExecWithLock(connection clientc.Connection, args [][]byte) (reply resp.Reply)
	ExecMulti(connection clientc.Connection, watching map[string]uint32, cmdLines [][]byte) (reply resp.Reply)
	GetUndoLogs(dbIndex int, cmdLine [][]byte) []CmdLine
	ForEach(dbIndex int, cb func(key string, data *DataEntity, expiration *time.Time) bool)
	RWLocks(dbIndex int, writeKeys []string, readKeys []string)
	RWUnLocks(dbIndex int, writeKeys []string, readKeys []string)
	GetDBSize(dbIndex int) (int, int)
	GetEntity(dbIndex int, key string) (*DataEntity, bool)
	GetExpiration(dbIndex int, key string) *time.Time
	SetKeyInsertedCallback(cb KeyEventCallback)
	SetKeyDeletedCallback(cb KeyEventCallback)
}
