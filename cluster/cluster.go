package cluster

import (
	cm "mygodis/common"
	"mygodis/common/commoninterface"
	"mygodis/config"
	"mygodis/resp"
)

type Cluster struct {
	self string
}

func (c Cluster) Exec(connection commoninterface.Connection, args cm.CmdLine) (reply resp.Reply) {
	//TODO implement me
	panic("implement me")
}

func (c Cluster) AfterClientClose(connection commoninterface.Connection) {
	//TODO implement me
	panic("implement me")
}

func (c Cluster) Close() {
	//TODO implement me
	panic("implement me")
}

func MakeCluster() *Cluster {
	return &Cluster{
		self: config.Properties.Self,
	}
}
