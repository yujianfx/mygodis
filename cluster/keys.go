package cluster

import (
	cm "mygodis/common"
	"mygodis/resp"
	"mygodis/util/cmdutil"
)

func execFlushDb(cluster *Cluster, args cm.CmdLine) resp.Reply {
	result, errs := cluster.broadcast(cmdutil.ToCmdLineWithName("cflushdb"))
	if len(errs) == 0 && cluster.isAllOk(result) {
		return resp.MakeOkReply()
	}
	return resp.MakeErrReply("error occurs")
}
func execPing(cluster *Cluster) resp.Reply {
	result, errs := cluster.broadcast(cmdutil.ToCmdLineWithName("cping"))
	if len(errs) == 0 && cluster.isAllOk(result) {
		return resp.MakePongReply()
	}
	return resp.MakeErrReply("error occurs")
}
func execCPing() resp.Reply {
	return resp.MakePongReply()
}
