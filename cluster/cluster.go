package cluster

import (
	"fmt"
	cm "mygodis/common"
	cmi "mygodis/common/commoninterface"
	"mygodis/config"
	"mygodis/datadriver/dict"
	"mygodis/db"
	"mygodis/lib/id"
	"mygodis/resp"
	"strings"
)

type Cluster struct {
	self               string
	nodes              dict.Dict
	db                 *db.StandaloneServer
	nodeConnectionPool *ConnectionPool
	transactions       dict.Dict
	idGenerator        *id.Snowflake
	ch                 *ConsistentHash
	epoch              int64
}

func (c *Cluster) AddClient(connection cmi.Connection) {
	c.db.AddClient(connection)
}

func (c *Cluster) RemoveClient(connection cmi.Connection) {
	c.db.RemoveClient(connection)
}

func (c *Cluster) dumpCluster() {
	fmt.Println()
	fmt.Printf("###############\n")
	fmt.Printf("self is:%s\n", c.self)
	fmt.Printf("nodes%v\n", c.nodes.Keys())
	serialize, _ := c.ch.Serialize()
	fmt.Printf("ConsistentHash : %s\n", string(serialize))
}
func (c *Cluster) Exec(connection cmi.Connection, args cm.CmdLine) (reply resp.Reply) {
	cmdName := strings.ToUpper(string(args[0]))
	switch cmdName {
	case "PING":
		return execPing(c)
	case "CPING":
		return execCPing()
	case "INFO":
		return execInfo(c, args[1:])
	}
	if cmdName == "CLUSTER" {
		return c.execCluster(connection, args[1:])
	}
	cmd, ok := DispatchCmd(cmdName)
	if !ok {
		return resp.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}

	return cmd(c, connection, args)
}

func execInfo(c *Cluster, cmd cm.CmdLine) resp.Reply {
	if len(cmd) == 1 {
		param := string(cmd[0])
		switch param {
		case "server":
			return resp.MakeMultiBulkReply(db.ServerInfo(c.db))
		case "client":
			return resp.MakeMultiBulkReply(db.ClientInfo(c.db))
		case "cluster":
			return resp.MakeMultiBulkReply([][]byte{
				[]byte("# Cluster"),
				[]byte(fmt.Sprintf("cluster_enabled: %v", config.Properties.ClusterEnable)),
				[]byte(fmt.Sprintf("cluster_node_count: %d", c.nodes.Len())),
				[]byte(fmt.Sprintf("cluster_nodes: %v", c.nodes.Keys())),
			})
		case "memory":
			return resp.MakeMultiBulkReply(db.MemoryInfo(c.db))
		case "persistence":
			return resp.MakeMultiBulkReply(db.PersistenceInfo(c.db))
		case "cpu":
			return resp.MakeMultiBulkReply(db.CpuInfo(c.db))
		}
	}
	return db.AllInfo(c.db)
}
func (c *Cluster) AfterClientClose(connection cmi.Connection) {
	c.db.AfterClientClose(connection)
}
func (c *Cluster) Close() {
	c.db.Close()
	c.nodeConnectionPool.Close()
}

func MakeCluster() *Cluster {
	cluster := &Cluster{
		nodes:              dict.NewConcurrentDict(),
		self:               config.Properties.Self,
		db:                 db.MakeStandaloneServer(),
		nodeConnectionPool: NewConnectionPool(),
		transactions:       dict.NewConcurrentDict(),
		ch:                 MakeConsistentHash(),
		idGenerator: func() *id.Snowflake {
			snowflake, err := id.NewSnowflake(config.Properties.DataCenterId, config.Properties.WorkerId)
			if err != nil {
				panic(err)
			}
			return snowflake
		}(),
	}
	cluster.ch.AddNode(cluster.self)

	return cluster
}
