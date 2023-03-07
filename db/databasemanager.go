package db

import (
	cm "mygodis/common"
	"mygodis/common/commoninterface"
	"mygodis/config"
	"mygodis/db/cmd/systemcmd"
	logger "mygodis/log"
	"mygodis/resp"
	"strings"
)

type DatabaseManager struct {
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

func (d DatabaseManager) FlushDB(dbIndex int) commoninterface.DB {
	return d.Dbs[dbIndex].(commoninterface.DB)
}
func (d DatabaseManager) FlushAll() {
	for i := 0; i < config.Properties.Databases; i++ {
		d.FlushDB(i)
	}
}
func (d DatabaseManager) Exec(connection commoninterface.Connection, cmd cm.CmdLine) (reply resp.Reply) {
	defer func() {
		if r := recover(); r != nil {
			logger.Warn("server error: %v", r)
			reply = resp.MakeErrReply("server error")
		}
	}()
	cmdName := strings.ToLower(string(cmd[0]))
	switch cmdName {
	case "ping":
		return systemcmd.Ping()
	case "auth":
		return systemcmd.Auth(connection, cmd[1:])
	case "slaveof":
		//TODO  return systemcmd.SlaveOf(connection, cmd)
	case "select":
		return systemcmd.Select(connection, cmd[1:])
	case "info":
		//TODO  return  systemcmd.Info(connection, cmd)
	case "subscribe":
		//TODO  return systemcmd.Subscribe(connection, cmd)
	case "unsubscribe":
		//TODO  return systemcmd.Unsubscribe(connection, cmd)
	case "publish":
		//TODO  return systemcmd.Publish(connection, cmd)
	case "psubscribe":
		//TODO  return systemcmd.PSubscribe(connection, cmd)
	case "punsubscribe":
		//TODO  return systemcmd.PUnsubscribe(connection, cmd)
	case "pubsub":
		//TODO  return systemcmd.PubSub(connection, cmd)
	case "flushdb":
		return systemcmd.FlushDB(connection, d, cmd)
	case "flushall":
		return systemcmd.FlushAll(connection, d, cmd)
	case "rewriteaof":
		//TODO  return systemcmd.RewriteAOF(connection, cmd)
	case "bgrewriteaof":
		//TODO  return systemcmd.BgRewriteAOF(connection, cmd)
	case "save":
		//TODO  return systemcmd.Save(connection, cmd)
	case "bgsave":
		//TODO  return systemcmd.BgSave(connection, cmd)
	case "copy":
		//TODO  return systemcmd.Copy(connection, cmd)
	case "replconf":
		//TODO  return systemcmd.ReplConf(connection, cmd)
	case "psync":
		//TODO  return systemcmd.PSync(connection, cmd)
	}
	return nil
}

func (d DatabaseManager) AfterClientClose(connection commoninterface.Connection) {
	//TODO implement me
}

func (d DatabaseManager) Close() {
	//TODO implement me

}

func MakeStandaloneServer() *DatabaseManager {
	databaseCount := config.Properties.Databases
	manager := &DatabaseManager{
		Dbs: make([]any, databaseCount),
	}
	for md := range manager.Dbs {
		dbi := NewDB()
		dbi.index = md
		manager.Dbs[md] = dbi
	}
	return manager
}
