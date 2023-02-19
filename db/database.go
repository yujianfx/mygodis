package db

import (
	"mygodis/datadriver/dict"
	"mygodis/resp"
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
		data:       dict.MakeConcurrent(dataDictSize),
		ttlMap:     dict.MakeConcurrent(ttlDictSize),
		versionMap: dict.MakeConcurrent(dataDictSize),
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
