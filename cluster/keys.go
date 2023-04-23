package cluster

import (
	cm "mygodis/common"
	cmi "mygodis/common/commoninterface"
	"mygodis/resp"
	"mygodis/util/cmdutil"
	"mygodis/util/ternaryoperator"
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

func execKeys(cluster *Cluster, connection cmi.Connection, cmdLine cm.CmdLine) resp.Reply {
	reply := cluster.db.Exec(connection, cmdutil.ToCmdLineWithBytes("KEYS", cmdLine[1:]...))
	results, errs := cluster.broadcast(cmdutil.ToCmdLineWithBytes("CKEYS", cmdLine[1:]...))
	results[cluster.self] = reply
	if errs != nil || len(errs) != 0 || !cluster.isAllOk(results) {
		return resp.MakeErrReply("cluster error")
	}
	keys := make([][]byte, 0)
	for _, reply := range results {
		bulkReply, ok := reply.(*resp.MultiBulkReply)
		if ok {
			keys = append(keys, bulkReply.Args...)
		}
	}

	return ternaryoperator.Which(len(keys) == 0, resp.MakeMultiBulkReply([][]byte{}), resp.MakeMultiBulkReply(keys))
}
func execCKeys(cluster *Cluster, connection cmi.Connection, cmdLine cm.CmdLine) resp.Reply {
	cmdLine[0] = []byte("KEYS")
	reply := cluster.db.Exec(connection, cmdLine)
	return reply
}
func execDel(cluster *Cluster, connection cmi.Connection, line cm.CmdLine) resp.Reply {
	if len(line) < 2 {
		return resp.MakeErrReply("wrong number of arguments for 'del' command")
	}
	results := make(map[string]resp.Reply)
	keys := make([][]byte, 0, len(line)-1)
	for i := 1; i < len(line); i++ {
		key := line[i]
		keys = append(keys, key)
		node := cluster.ch.GetNode(key)
		if node != cluster.self {
			client := cluster.nodeConnectionPool.GetConnection(node)
			reply, err := client.Send(cmdutil.ToCmdLineWithBytes("DEL", line[i]))
			if err != nil {
				return resp.MakeErrReply(err.Error())
			}
			results[string(key)] = reply
			continue
		}
		reply := cluster.db.Exec(connection, cmdutil.ToCmdLineWithBytes("DEL", line[i]))
		results[string(key)] = reply
	}
	if len(results) == 0 {
		return resp.MakeErrReply("cluster error")
	}
	var count int64
	for _, key := range keys {
		reply, ok := results[string(key)]
		if !ok {
			return resp.MakeErrReply("cluster error")
		}
		if reply == nil {
			continue
		}
		count += reply.(*resp.IntReply).Code
	}
	return resp.MakeIntReply(count)
}
func execMGet(cluster *Cluster, connection cmi.Connection, cmdLine cm.CmdLine) resp.Reply {
	if len(cmdLine) < 2 {
		return resp.MakeErrReply("wrong number of arguments for 'mget' command")
	}
	results := make(map[string]resp.Reply)
	keys := make([][]byte, 0, len(cmdLine)-1)
	for i := 1; i < len(cmdLine); i++ {
		key := cmdLine[i]
		keys = append(keys, key)
		node := cluster.ch.GetNode(key)
		if node != cluster.self {
			client := cluster.nodeConnectionPool.GetConnection(node)
			reply, err := client.Send(cmdutil.ToCmdLineWithBytes("GET", cmdLine[i]))
			if err != nil {
				return resp.MakeErrReply(err.Error())
			}
			results[string(key)] = reply
			continue
		}
		reply := cluster.db.Exec(connection, cmdutil.ToCmdLineWithBytes("GET", cmdLine[i]))
		results[string(key)] = reply
	}
	if len(results) == 0 {
		return resp.MakeErrReply("cluster error")
	}
	bytes := make([][]byte, 0, len(results))
	for _, key := range keys {
		reply, ok := results[string(key)]
		if !ok {
			return resp.MakeErrReply("cluster error")
		}
		if reply == nil {
			continue
		}
		bytes = append(bytes, reply.(*resp.BulkReply).Arg)
	}

	return resp.MakeMultiBulkReply(bytes)
}
func execMSet0(cluster *Cluster, connection cmi.Connection, cmdLine cm.CmdLine) resp.Reply {
	if len(cmdLine) < 3 {
		return resp.MakeErrReply("wrong number of arguments for 'mset' command")
	}
	if len(cmdLine)%2 != 1 {
		return resp.MakeErrReply("wrong number of arguments for 'mset' command")
	}
	results := make(map[string]resp.Reply)
	for i := 1; i < len(cmdLine); i += 2 {
		key := cmdLine[i]
		node := cluster.ch.GetNode(key)
		set := ternaryoperator.Which(string(cmdLine[0]) == "MSETNX", "SETNX", "SET")
		if node != cluster.self {
			client := cluster.nodeConnectionPool.GetConnection(node)
			reply, err := client.Send(cmdutil.ToCmdLineWithBytes(set, cmdLine[i], cmdLine[i+1]))
			if err != nil {
				return resp.MakeErrReply(err.Error())
			}
			results[string(key)] = reply
			continue
		}
		reply := cluster.db.Exec(connection, cmdutil.ToCmdLineWithBytes(set, cmdLine[i], cmdLine[i+1]))
		results[string(key)] = reply
	}
	if len(results) == 0 || !cluster.isAllOk(results) {
		return resp.MakeErrReply("cluster error")
	}
	return resp.MakeOkReply()
}

var cmdContainer = make(map[string]CmdFunc)
var supportMulti = make(map[string]struct{})

func RegisterSupportMultiKey(cmdNames ...string) {
	for _, cmdName := range cmdNames {
		supportMulti[cmdName] = struct{}{}
	}
}
func IsSupportMulti(cmdName string) bool {
	_, ok := supportMulti[cmdName]
	return ok
}

func isMultiKeyCmd(cmdLine cm.CmdLine) bool {
	cmdName := string(cmdLine[0])
	switch cmdName {
	case "MSET", "MGET", "MSETNX":
		if len(cmdLine) < 3 {
			return false
		}
	case "DEL":
		if len(cmdLine) < 2 {
			return false
		}
	case "BITOP":
		if len(cmdLine) < 4 {
			return false
		}
	case "RPOPLPUSH", "SMOVE", "SDIFF", "SINTER", "SUNION", "SUNIONSTORE", "SDIFFSTORE", "SINTERSTORE", "ZINTERSTORE", "ZUNIONSTORE", "ZINTER", "ZUNION":
		return true
	}
	return false
}
func init() {

	RegisterCmd("EXISTS", defaultFunc)
	RegisterCmd("TTL", defaultFunc)
	RegisterCmd("PTTL", defaultFunc)
	RegisterCmd("TYPE", defaultFunc)
	RegisterCmd("EXPIRE", defaultFunc)
	RegisterCmd("EXPIREAT", defaultFunc)
	RegisterCmd("PEXPIRE", defaultFunc)
	RegisterCmd("PEXPIREAT", defaultFunc)
	RegisterCmd("PERSIST", defaultFunc)
	RegisterCmd("KEYS", execKeys)
	RegisterCmd("CKEYS", execCKeys)
}
