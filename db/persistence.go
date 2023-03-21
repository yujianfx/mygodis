package db

import (
	"fmt"
	"github.com/hdt3213/rdb/core"
	parse "github.com/hdt3213/rdb/parser"
	"mygodis/aof"
	"mygodis/common"
	cmi "mygodis/common/commoninterface"
	"mygodis/config"
	"mygodis/datadriver/dict"
	"mygodis/datadriver/list"
	"mygodis/datadriver/sortedset"
	"os"
)

func (stdDBM *StandaloneDatabaseManager) loadRDBFile() (err error) {
	rdbFile, err := os.Open(config.Properties.RDBFilename)
	if err != nil {
		return fmt.Errorf("open rdb file failed " + err.Error())
	}
	defer func() {
		_ = rdbFile.Close()
	}()
	decoder := core.NewDecoder(rdbFile)
	err = stdDBM.loadRDB(decoder)
	if err != nil {
		return fmt.Errorf("dump rdb file failed" + err.Error())
	}
	return nil
}
func (stdDBM *StandaloneDatabaseManager) selectDB(index int) (db *DataBaseImpl) {
	if index < 0 || index >= len(stdDBM.Dbs) {
		panic("invalid db index")
	}
	return stdDBM.Dbs[index].(*DataBaseImpl)
}
func (stdDBM *StandaloneDatabaseManager) loadRDB(dec *core.Decoder) (err error) {

	return dec.Parse(func(obj parse.RedisObject) bool {
		db := stdDBM.selectDB(obj.GetDBIndex())
		var entity *cmi.DataEntity
		switch obj.GetType() {
		case parse.StringType:
			str := obj.(*parse.StringObject)
			entity = &cmi.DataEntity{
				Data: str.Value,
			}
		case parse.ListType:
			listObj := obj.(*parse.ListObject)
			list := list.NewQuickList()
			for _, v := range listObj.Values {
				list.Add(v)
			}
			entity = &cmi.DataEntity{
				Data: list,
			}
		case parse.HashType:
			hashObj := obj.(*parse.HashObject)
			hash := dict.NewSimpleDict(8)
			for k, v := range hashObj.Hash {
				hash.Put(k, v)
			}
			entity = &cmi.DataEntity{
				Data: hash,
			}
		case parse.SetType:
			setObj := obj.(*parse.SetObject)
			set := dict.NewSimpleDict(8)
			for _, v := range setObj.Members {
				set.Put(string(v), nil)
			}
			entity = &cmi.DataEntity{
				Data: set,
			}
		case parse.ZSetType:
			zsetObj := obj.(*parse.ZSetObject)
			zset := sortedset.MakeZSet()
			for _, v := range zsetObj.Entries {
				zset.Add(v.Member, v.Score)
			}
			entity = &cmi.DataEntity{
				Data: zset,
			}
		}
		if entity != nil {
			db.PutEntity(obj.GetKey(), entity)
			if obj.GetExpiration() != nil {
				db.Expire(obj.GetKey(), *obj.GetExpiration())
			}
			db.addAof(aof.EntityToCmd(obj.GetKey(), entity).Args)
		}
		return true
	})
}
func (stdDBM *StandaloneDatabaseManager) AddAof(dbIndex int, line common.CmdLine) {
	if stdDBM.persister != nil {
		stdDBM.persister.SaveCmd(dbIndex, line)
	}
}
func (stdDBM *StandaloneDatabaseManager) bindPersister(persister *aof.Persister) {
	stdDBM.persister = persister
	for _, db := range stdDBM.Dbs {
		baseImpl := db.(DataBaseImpl)
		baseImpl.addAof = func(cmdLine common.CmdLine) {
			if config.Properties.AppendOnly {
				stdDBM.persister.SaveCmd(baseImpl.index, cmdLine)
			}
		}
	}
}
func MakeAuxiliaryServer() *StandaloneDatabaseManager {
	std := &StandaloneDatabaseManager{}
	std.Dbs = make([]any, config.Properties.Databases)
	for i := range std.Dbs {
		std.Dbs[i] = newBasicDB()
	}
	return std
}
func NewPersister(db cmi.StandaloneDBEngine, aofFileName string, load bool, fsync int8) (*aof.Persister, error) {
	return aof.NewPersister(db, aofFileName, load, fsync, func() cmi.StandaloneDBEngine {
		return MakeAuxiliaryServer()
	})
}
