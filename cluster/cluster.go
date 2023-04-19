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

func (c *Cluster) dumpCluster() {
	fmt.Println()
	fmt.Printf("###############\n")
	fmt.Printf("self is:%s\n", c.self)
	fmt.Printf("nodes%v\n", c.nodes.Keys())
	serialize, _ := c.ch.Serialize()
	fmt.Printf("ConsistentHash : %s\n", string(serialize))
}
func (c *Cluster) Exec(connection cmi.Connection, args cm.CmdLine) (reply resp.Reply) {

	cmdName := string(args[0])
	switch cmdName {
	case "PING":
		return execPing(c)
	case "CPING":
		return execCPing()
	}
	// auth
	if cmdName == "CLUSTER" {
		return c.execCluster(connection, args[1:])
	}
	cmd, ok := DispatchCmd(cmdName)
	if !ok {
		return resp.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}

	return cmd(c, connection, args)
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
