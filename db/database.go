package db

import (
	"mygodis/clientc"
	"mygodis/datadriver/dict"
	"mygodis/resp"
	errorsresp "mygodis/resp/error"
	"strings"
)

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
	lockerSize   = 1024
)

type DataBaseImpl struct {
	index      int
	data       dict.Dict
	ttlMap     dict.Dict
	versionMap dict.Dict
	//TODO: add lock
	//locker         *lock.Locks
	addAof         func(CmdLine)
	insertCallback KeyEventCallback
	deleteCallback KeyEventCallback
}

type ExecFunc func(db *DataBaseImpl, args [][]byte) resp.Reply
type PreFunc func(args [][]byte) ([]string, []string)
type CmdLine = [][]byte
type UndoFunc func(db *DataBaseImpl, args [][]byte) []CmdLine

func newDB() *DataBaseImpl {
	db := &DataBaseImpl{
		data:       dict.NewConcurrentDict(dataDictSize),
		ttlMap:     dict.NewConcurrentDict(ttlDictSize),
		versionMap: dict.NewConcurrentDict(dataDictSize),
		//locker:     lock.Make(lockerSize),
		addAof: func(line CmdLine) {},
	}
	return db
}
func newBasicDB() *DataBaseImpl {
	db := &DataBaseImpl{
		data:       dict.NewSimpleDict(dataDictSize),
		ttlMap:     dict.NewSimpleDict(ttlDictSize),
		versionMap: dict.NewSimpleDict(dataDictSize),
		//locker:     lock.Make(lockerSize),
		addAof: func(line CmdLine) {},
	}
	return db
}
func (dbi *DataBaseImpl) Exec(c clientc.Connection, cmd [][]byte) (reply resp.Reply) {
	s := strings.ToLower(string(cmd[0]))
	switch s {
	case "multi": //开启事务
		if len(cmd) != 1 {
			return errorsresp.MakeArgNumErrReply(s)
		}
	case "discard": //取消事务
		if len(cmd) != 1 {
			return errorsresp.MakeArgNumErrReply(s)
		}
	case "exec": //执行事务
		if len(cmd) != 1 {
			return errorsresp.MakeArgNumErrReply(s)
		}
	case "watch": //监视key
		if len(cmd) < 2 {
			return errorsresp.MakeArgNumErrReply(s)
		}
	}
	if c != nil && c.InMultiState() {
		//TODO:事务
	}

	return

}
