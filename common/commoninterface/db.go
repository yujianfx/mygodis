package commoninterface

import (
	cm "mygodis/common"
	"mygodis/resp"
	"time"
)

type DataEntity struct {
	Data any
}
type KeyEventCallback func(dbIndex int, key string, entity *DataEntity)
type DB interface {
	Exec(connection Connection, args cm.CmdLine) (reply resp.Reply)
	AfterClientClose(connection Connection)
	Close()
}
type DBManage interface {
	FlushDB(dbIndex int) DB
	FlushAll()
}
type SimpleDBEngine interface {
	DB
	ExecWithLock(connection Connection, args [][]byte) (reply resp.Reply)
	ExecMulti(connection Connection, watching map[string]uint32, cmdLines [][]byte) (reply resp.Reply)
	GetUndoLogs(dbIndex int, cmdLine [][]byte) []cm.CmdLine
	ForEach(dbIndex int, cb func(key string, data *DataEntity, expiration *time.Time) bool)
	RWLocks(dbIndex int, writeKeys []string, readKeys []string)
	RWUnLocks(dbIndex int, writeKeys []string, readKeys []string)
	GetDBSize(dbIndex int) (int, int)
	GetEntity(dbIndex int, key string) (*DataEntity, bool)
	GetExpiration(dbIndex int, key string) *time.Time
	SetKeyInsertedCallback(cb KeyEventCallback)
	SetKeyDeletedCallback(cb KeyEventCallback)
}
