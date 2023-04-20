package cluster

import (
	cm "mygodis/common"
	cmi "mygodis/common/commoninterface"
	"mygodis/resp"
)

type CmdFunc func(cluster *Cluster, connection cmi.Connection, cmdLine cm.CmdLine) resp.Reply

func RegisterCmd(name string, cmd CmdFunc) {
	cmdContainer[name] = cmd
}
func DispatchCmd(name string) (CmdFunc, bool) {
	cmd, ok := cmdContainer[name]
	return cmd, ok
}

var (
	defaultFunc = func(cluster *Cluster, connection cmi.Connection, cmdLine cm.CmdLine) resp.Reply {
		if isMultiKeyCmd(cmdLine) {
			name := string(cmdLine[0])
			if IsSupportMulti(name) {
				switch name {
				case "MSET", "MSETNX":
					return execMSet0(cluster, connection, cmdLine)
				case "MGET":
					return execMGet(cluster, connection, cmdLine)
				case "DEL":
					return execDel(cluster, connection, cmdLine)
				}
			}
			return resp.MakeErrReply("not support multi key command " + name)
		}
		key := cmdLine[1]
		node := cluster.ch.GetNode(key)
		if node == cluster.self {
			reply := cluster.db.Exec(connection, cmdLine)
			return reply
		}
		client := cluster.nodeConnectionPool.GetConnection(node)
		reply, err := client.Send(cmdLine)
		if err != nil {
			return resp.MakeErrReply(err.Error())
		}
		return reply
	}
)

func init() {
	RegisterSupportMultiKey("MSET", "METNX", "MGET", "DEL")
	RegisterCmd("SET", defaultFunc)
	RegisterCmd("GETEX", defaultFunc)
	RegisterCmd("GET", defaultFunc)
	RegisterCmd("SETNX", defaultFunc)
	RegisterCmd("SETEX", defaultFunc)
	RegisterCmd("PSETEX", defaultFunc)
	RegisterCmd("MSET", defaultFunc)
	RegisterCmd("MGET", defaultFunc)
	RegisterCmd("MSETNX", defaultFunc)
	RegisterCmd("GETSET", defaultFunc)
	RegisterCmd("GETDEL", defaultFunc)
	RegisterCmd("INCR", defaultFunc)
	RegisterCmd("INCTBY", defaultFunc)
	RegisterCmd("INCRBYFLOAT", defaultFunc)
	RegisterCmd("DECR", defaultFunc)
	RegisterCmd("DECRBY", defaultFunc)
	RegisterCmd("STRLEN", defaultFunc)
	RegisterCmd("APPEND", defaultFunc)
	RegisterCmd("GETRANGE", defaultFunc)
	RegisterCmd("SETRANGE", defaultFunc)
	RegisterCmd("SETBIT", defaultFunc)
	RegisterCmd("GETBIT", defaultFunc)
	RegisterCmd("BITCOUNT", defaultFunc)
	RegisterCmd("BITOP", defaultFunc)
	RegisterCmd("LINDEX", defaultFunc)
	RegisterCmd("LLEN", defaultFunc)
	RegisterCmd("LPOP", defaultFunc)
	RegisterCmd("LPUSH", defaultFunc)
	RegisterCmd("LPUSHX", defaultFunc)
	RegisterCmd("LRANGE", defaultFunc)
	RegisterCmd("LREM", defaultFunc)
	RegisterCmd("LSET", defaultFunc)
	RegisterCmd("RPOP", defaultFunc)
	RegisterCmd("RPOPLPUSH", defaultFunc)
	RegisterCmd("RPUSH", defaultFunc)
	RegisterCmd("RPUSHX", defaultFunc)
	RegisterCmd("LTRIM", defaultFunc)
	RegisterCmd("HGET", defaultFunc)
	RegisterCmd("HSET", defaultFunc)
	RegisterCmd("HMSET", defaultFunc)
	RegisterCmd("HMGET", defaultFunc)
	RegisterCmd("HGETALL", defaultFunc)
	RegisterCmd("HDEL", defaultFunc)
	RegisterCmd("HEXISTS", defaultFunc)
	RegisterCmd("HINCRBY", defaultFunc)
	RegisterCmd("HINCRBYFLOAT", defaultFunc)
	RegisterCmd("HKEYS", defaultFunc)
	RegisterCmd("HLEN", defaultFunc)
	RegisterCmd("HSETNX", defaultFunc)
	RegisterCmd("HVALS", defaultFunc)
	RegisterCmd("SADD", defaultFunc)
	RegisterCmd("SCARD", defaultFunc)
	RegisterCmd("SDIFF", defaultFunc)
	RegisterCmd("SDIFFSTORE", defaultFunc)
	RegisterCmd("SINTER", defaultFunc)
	RegisterCmd("SINTERSTORE", defaultFunc)
	RegisterCmd("SISMEMBER", defaultFunc)
	RegisterCmd("SMOVE", defaultFunc)
	RegisterCmd("SPOP", defaultFunc)
	RegisterCmd("SRANDMEMBER", defaultFunc)
	RegisterCmd("SREM", defaultFunc)
	RegisterCmd("SUNION", defaultFunc)
	RegisterCmd("SUNIOMSTORE", defaultFunc)
}
