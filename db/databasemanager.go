package db

import (
	"fmt"
	cm "mygodis/common"
	"mygodis/common/commoninterface"
	"mygodis/config"
	//"mygodis/db/cmd"
	logger "mygodis/log"
	"mygodis/resp"
	"os"
	"runtime"
	"strings"
)

type StandaloneDatabaseManager struct {
	Dbs []any
	//TODO add pubsub
	//hub pubsub.Hub
	//TODO add aof
	//aof *aof.AOF
	role uint32
	//TODO add replication
	//slaveStatus  *slaveStatus
	//masterStatus *masterStatus
	//hooks
	insertCallBack commoninterface.KeyEventCallback
	deleteCallBack commoninterface.KeyEventCallback
}

func (d *StandaloneDatabaseManager) GetDbInfo(infoType cm.InfoType) []cm.DBInfo {
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
func (d *StandaloneDatabaseManager) FlushDB(dbIndex int) commoninterface.DB {
	return d.Dbs[dbIndex].(commoninterface.DB)
}
func (d *StandaloneDatabaseManager) FlushAll() {
	for i := 0; i < config.Properties.Databases; i++ {
		d.FlushDB(i)
	}
}
func (d *StandaloneDatabaseManager) Exec(connection commoninterface.Connection, cmd cm.CmdLine) (reply resp.Reply) {
	defer func() {
		if r := recover(); r != nil {
			logger.Warn("server error: %v", r)
			reply = resp.MakeErrReply("server error")
		}
	}()
	cmdName := strings.ToLower(string(cmd[0]))
	switch cmdName {
	case "ping":
		return Ping()
	case "auth":
		return Auth(connection, cmd[1:])
	case "slaveof":
		//TODO  return systemcd.SlaveOf(connection, cmd)
	case "select":

		return Select(connection, cmd[1:])
	case "info":
		return Info(connection, d, cmd)
	case "subscribe":
		//TODO  return systemcd.Subscribe(connection, cmd)
	case "unsubscribe":
		//TODO  return systemcd.Unsubscribe(connection, cmd)
	case "publish":
		//TODO  return systemcd.Publish(connection, cmd)
	case "psubscribe":
		//TODO  return systemcd.PSubscribe(connection, cmd)
	case "punsubscribe":
		//TODO  return systemcd.PUnsubscribe(connection, cmd)
	case "pubsub":
		//TODO  return systemcd.PubSub(connection, cmd)
	case "flushdb":
		return FlushDB(connection, d, cmd)
	case "flushall":
		return FlushAll(connection, d, cmd)
	case "rewriteaof":
		//TODO  return systemcd.RewriteAOF(connection, cmd)
	case "bgrewriteaof":
		//TODO  return systemcd.BgRewriteAOF(connection, cmd)
	case "save":
		//TODO  return systemcd.Save(connection, cmd)
	case "bgsave":
		//TODO  return systemcd.BgSave(connection, cmd)
	case "copy":
		//TODO  return systemcd.Copy(connection, cmd)
	case "replconf":
		//TODO  return systemcd.ReplConf(connection, cmd)
	case "psync":
		//TODO  return systemcd.PSync(connection, cmd)
	}
	return nil
}
func (d *StandaloneDatabaseManager) AfterClientClose(connection commoninterface.Connection) {
	//TODO implement me
}
func (d *StandaloneDatabaseManager) Close() {
	//TODO implement me

}
func MakeStandaloneServer() *StandaloneDatabaseManager {
	databaseCount := config.Properties.Databases
	manager := &StandaloneDatabaseManager{
		Dbs: make([]any, databaseCount),
	}
	for md := range manager.Dbs {
		dbi := NewDB()
		dbi.index = md
		manager.Dbs[md] = dbi
	}
	return manager
}
