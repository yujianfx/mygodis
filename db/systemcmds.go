package db

import (
	"mygodis/common"
	"mygodis/common/commoninterface"
	"mygodis/config"
	"mygodis/resp"
	"mygodis/util/cmdutil"
	"strconv"
)

func Ping() resp.Reply {
	return resp.MakePongReply()

}
func Auth(c commoninterface.Connection, cmd common.CmdLine) resp.Reply {
	if len(cmd) != 1 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'auth' command")
	}
	if config.Properties.RequirePass == "" {
		return resp.MakeErrReply("ERR Client sent AUTH, but no password is set")
	}
	passwd := string(cmd[0])
	c.SetPassword(passwd)
	if config.Properties.RequirePass != passwd {
		return resp.MakeErrReply("ERR invalid password")
	}
	return resp.MakeOkReply()
}
func isAuthenticated(c commoninterface.Connection) bool {
	if config.Properties.RequirePass == "" {
		return true
	}
	return c.GetPassword() == config.Properties.RequirePass
}
func Select(d *StandaloneServer, connection commoninterface.Connection, cmd common.CmdLine) resp.Reply {
	if !isAuthenticated(connection) {
		return resp.MakeErrReply("NOAUTH Authentication required.")
	}
	if len(cmd) != 1 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'select' command")
	}
	s := string(cmd[0])
	dbIndex, err := strconv.Atoi(s)
	if err != nil {
		return resp.MakeErrReply("ERR value is not an integer ")
	}
	if dbIndex < 0 || dbIndex >= config.Properties.Databases {
		return resp.MakeErrReply("ERR value is  out of range")
	}
	d.AddAof(dbIndex, cmdutil.ToCmdLineWithName("select", s))
	return resp.MakeOkReply()
}
func Info(connection commoninterface.Connection, info commoninterface.DBInfo, cmd common.CmdLine) resp.Reply {
	dbInfoToStringBytes := func(dbInfo []common.DBInfo) [][]byte {
		var result [][]byte
		for _, v := range dbInfo {
			result = append(result, []byte(v.InfoKey+":"+v.InfoValue))

		}
		return result
	}
	if !isAuthenticated(connection) {
		return resp.MakeErrReply("NOAUTH Authentication required.")
	}
	if len(cmd) <= 1 {
		infos := info.GetDbInfo(common.ALL_INFO)
		bytes := dbInfoToStringBytes(infos)
		return resp.MakeMultiBulkReply(bytes)
	}
	param := string(cmd[1])
	switch param {
	case "server":
		infos := info.GetDbInfo(common.SERVER_INFO)
		bytes := dbInfoToStringBytes(infos)
		return resp.MakeMultiBulkReply(bytes)
	case "client":
		infos := info.GetDbInfo(common.CLIENT_INFO)
		bytes := dbInfoToStringBytes(infos)

		return resp.MakeMultiBulkReply(bytes)
	case "cluster":
		infos := info.GetDbInfo(common.CLUSTER_INFO)
		bytes := dbInfoToStringBytes(infos)
		return resp.MakeMultiBulkReply(bytes)
	case "memory":
		infos := info.GetDbInfo(common.MEMORY_INFO)
		bytes := dbInfoToStringBytes(infos)

		return resp.MakeMultiBulkReply(bytes)
	case "persistence":
		infos := info.GetDbInfo(common.PERSISTENCE_INFO)
		bytes := dbInfoToStringBytes(infos)
		return resp.MakeMultiBulkReply(bytes)
	case "stats":
		infos := info.GetDbInfo(common.STATS_INFO)
		bytes := dbInfoToStringBytes(infos)
		return resp.MakeMultiBulkReply(bytes)
	case "replication":
		infos := info.GetDbInfo(common.REPLICATION_INFO)
		bytes := dbInfoToStringBytes(infos)
		return resp.MakeMultiBulkReply(bytes)
	case "cpu":
		infos := info.GetDbInfo(common.CPU_INFO)
		bytes := dbInfoToStringBytes(infos)
		return resp.MakeMultiBulkReply(bytes)

	}
	return resp.MakeNullBulkReply()
}
