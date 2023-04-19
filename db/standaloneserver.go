package db

import (
	"fmt"
	"mygodis/aof"
	cm "mygodis/common"
	"mygodis/common/commoninterface"
	"mygodis/config"
	"mygodis/util/cmdutil"
	"time"

	//"mygodis/db/cmd"
	logger "mygodis/log"
	"mygodis/resp"
	"os"
	"runtime"
	"strings"
)

type StandaloneServer struct {
	Dbs []any
	//TODO add pubsub
	//hub pubsub.Hub
	persister *aof.Persister
	role      uint32
	//TODO add replication
	//hooks
	insertCallBack commoninterface.KeyEventCallback
	deleteCallBack commoninterface.KeyEventCallback
}

func (d *StandaloneServer) ExecWithLock(connection commoninterface.Connection, args cm.CmdLine) (reply resp.Reply) {
	return d.selectDB(connection.GetDBIndex()).ExecWithLock(args)
}

func (d *StandaloneServer) ExecMulti(connection commoninterface.Connection, watching map[string]uint32, cmdLines []cm.CmdLine) (reply resp.Reply) {
	return ExecMulti(d.selectDB(connection.GetDBIndex()), connection, watching, cmdLines)
}

func (d *StandaloneServer) GetUndoLogs(dbIndex int, cmd cm.CmdLine) []cm.CmdLine {

	return GetUndoLogs(d.selectDB(dbIndex), cmd)
}

func (d *StandaloneServer) ForEach(dbIndex int, cb func(key string, data *commoninterface.DataEntity, expiration time.Time) bool) {
	d.selectDB(dbIndex).ForEach(cb)
}

func (d *StandaloneServer) RWLocks(dbIndex int, writeKeys []string, readKeys []string) {
	db := d.selectDB(dbIndex)
	db.RWLocks(writeKeys, readKeys)
}

func (d *StandaloneServer) RWUnLocks(dbIndex int, writeKeys []string, readKeys []string) {
	db := d.selectDB(dbIndex)
	db.RWUnLocks(writeKeys, readKeys)
}

func (d *StandaloneServer) GetDBSize(dbIndex int) (int, int) {
	db := d.selectDB(dbIndex)
	return db.data.Len(), db.ttlMap.Len()
}

func (d *StandaloneServer) GetEntity(dbIndex int, key string) (*commoninterface.DataEntity, bool) {
	db := d.selectDB(dbIndex)
	val, exists := db.data.Get(key)
	if !exists {
		return nil, false
	}
	entity, result := val.(*commoninterface.DataEntity)
	return entity, result
}

func (d *StandaloneServer) GetExpiration(dbIndex int, key string) time.Time {
	val, exists := d.selectDB(dbIndex).ttlMap.Get(key)
	if !exists {
		return time.Time{}
	}
	t, _ := val.(time.Time)
	return t
}

func (d *StandaloneServer) SetKeyInsertedCallback(cb commoninterface.KeyEventCallback) {
	d.insertCallBack = cb
}

func (d *StandaloneServer) SetKeyDeletedCallback(cb commoninterface.KeyEventCallback) {
	d.deleteCallBack = cb
}

func (d *StandaloneServer) GetDbInfo(infoType cm.InfoType) []cm.DBInfo {
	infos := make([]cm.DBInfo, 0)
	switch infoType {
	case cm.CLIENT_INFO:
		infos = append(infos, cm.DBInfo{InfoKey: "connected_clients", InfoValue: "0"})
		infos = append(infos, cm.DBInfo{InfoKey: "cluster_connections", InfoValue: "0"})
		return infos
	case cm.CLUSTER_INFO:
		infos = append(infos, cm.DBInfo{InfoKey: "cluster_enabled", InfoValue: "0"})
		return infos
	case cm.SERVER_INFO:
		infos = append(infos, cm.DBInfo{InfoKey: "version", InfoValue: "0.0.1"})
		infos = append(infos, cm.DBInfo{InfoKey: "mode", InfoValue: "standalone"})
		infos = append(infos, cm.DBInfo{InfoKey: "arch_bits", InfoValue: "64"})
		infos = append(infos, cm.DBInfo{InfoKey: "tcp_port", InfoValue: fmt.Sprintf("%d", config.Properties.Port)})
		infos = append(infos, cm.DBInfo{InfoKey: "process_id", InfoValue: fmt.Sprintf("%d", os.Getpid())})
		return infos
	case cm.MEMORY_INFO:
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		infos = append(infos, cm.DBInfo{InfoKey: "used_memory", InfoValue: fmt.Sprintf("%d", m.Alloc)})
		infos = append(infos, cm.DBInfo{InfoKey: "used_memory_rss", InfoValue: fmt.Sprintf("%d", m.Sys)})
		return infos
	case cm.CPU_INFO:
		infos = append(infos, cm.DBInfo{InfoKey: "used_cpu_sys", InfoValue: "0"})
		return infos
	case cm.PERSISTENCE_INFO:
		infos = append(infos, cm.DBInfo{InfoKey: "loading", InfoValue: "0"})
	case cm.STATS_INFO:
		infos = append(infos, cm.DBInfo{InfoKey: "total_connections_received", InfoValue: "0"})
		return infos
	case cm.REPLICATION_INFO:
		infos = append(infos, cm.DBInfo{InfoKey: "role", InfoValue: "master"})
		return infos
	case cm.ALL_INFO:
		infos = append(infos, d.GetDbInfo(cm.SERVER_INFO)...)
		infos = append(infos, d.GetDbInfo(cm.CLIENT_INFO)...)
		infos = append(infos, d.GetDbInfo(cm.CLUSTER_INFO)...)
		infos = append(infos, d.GetDbInfo(cm.MEMORY_INFO)...)
		infos = append(infos, d.GetDbInfo(cm.PERSISTENCE_INFO)...)
		infos = append(infos, d.GetDbInfo(cm.STATS_INFO)...)
		infos = append(infos, d.GetDbInfo(cm.REPLICATION_INFO)...)
		infos = append(infos, d.GetDbInfo(cm.CPU_INFO)...)
		return infos
	}
	return nil
}
func (d *StandaloneServer) FlushAll() resp.Reply {
	for md := range d.Dbs {
		dbi := NewDB()
		dbi.index = md
		d.Dbs[md] = dbi
	}
	d.AddAof(0, cmdutil.ToCmdLine("flushall"))
	return resp.MakeOkReply()

}
func (d *StandaloneServer) Exec(connection commoninterface.Connection, cmd cm.CmdLine) (reply resp.Reply) {
	defer func() {
		if r := recover(); r != nil {
			logger.Warn("server error", r)
			reply = resp.MakeErrReply("server error")
		}
	}()
	cmdName := strings.ToUpper(string(cmd[0]))
	switch cmdName {
	case "PING":
		return Ping()
	case "AUTH":
		return Auth(connection, cmd[1:])
	//case "slaveof":
	//TODO  return systemcd.SlaveOf(connection, cmd)
	case "SELECT":
		return Select(d, connection, cmd[1:])
	case "INFO":
		return Info(connection, d, cmd)
	//case "subscribe":
	//TODO  return systemcd.Subscribe(connection, cmd)
	//case "unsubscribe":
	//TODO  return systemcd.Unsubscribe(connection, cmd)
	//case "publish":
	//TODO  return systemcd.Publish(connection, cmd)
	//case "psubscribe":
	//TODO  return systemcd.PSubscribe(connection, cmd)
	//case "punsubscribe":
	//TODO  return systemcd.PUnsubscribe(connection, cmd)
	//case "pubsub":
	//TODO  return systemcd.PubSub(connection, cmd)
	case "flushall":
		return d.FlushAll()
	//case "rewriteaof":
	//TODO  return systemcd.RewriteAOF(connection, cmd)
	//case "bgrewriteaof":
	//TODO  return systemcd.BgRewriteAOF(connection, cmd)
	//case "save":
	//TODO  return systemcd.Save(connection, cmd)
	//case "bgsave":
	//TODO  return systemcd.BgSave(connection, cmd)
	//case "copy":
	//TODO  return systemcd.Copy(connection, cmd)
	//case "replconf":
	//TODO  return systemcd.ReplConf(connection, cmd)
	//case "psync":
	//TODO  return systemcd.PSync(connection, cmd)
	default:
		return d.selectDB(connection.GetDBIndex()).Exec(connection, cmd)
	}
}
func (d *StandaloneServer) AfterClientClose(connection commoninterface.Connection) {
	name := connection.Name()
	logger.Info("client close", name)
}
func (d *StandaloneServer) Close() {
	d.persister.Close()

}
func MakeStandaloneServer() *StandaloneServer {
	databaseCount := config.Properties.Databases
	manager := &StandaloneServer{
		Dbs: make([]any, databaseCount),
	}
	for md := range manager.Dbs {
		dbi := NewDB()
		dbi.index = md
		manager.Dbs[md] = dbi
	}
	//TODO 添加发布订阅
	appendOnly := config.Properties.AppendOnly
	if appendOnly {
		fsync := aof.Always
		switch config.Properties.AppendFsync {
		case "always":
			fsync = aof.Always
		case "everysec":
			fsync = aof.EverySec
		case "no":
			fsync = aof.No
		}
		aofPersister, err := NewPersister(manager, config.Properties.AppendFilename, true, fsync)
		if err != nil {
			logger.Fatal("open aofPersister file error: ", err)
		}
		manager.bindPersister(aofPersister)
	}
	if config.Properties.RDBFilename != "" {
		err := manager.loadRDBFile()
		if err != nil {
			logger.Error("load rdb file error: ", err)
		}
	}
	//TODO 添加主从复制
	return manager
}
