package systemcmd

import (
	"mygodis/common"
	"mygodis/common/commoninterface"
	"mygodis/config"
	"mygodis/resp"
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
func Select(connection commoninterface.Connection, cmd common.CmdLine) resp.Reply {
	if !isAuthenticated(connection) {
		return resp.MakeErrReply("NOAUTH Authentication required.")
	}
	if len(cmd) != 1 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'select' command")
	}
	dbIndex, err := strconv.Atoi(string(cmd[0]))
	if err != nil {
		return resp.MakeErrReply("ERR value is not an integer ")
	}
	if dbIndex < 0 || dbIndex >= config.Properties.Databases {
		return resp.MakeErrReply("ERR value is  out of range")
	}
	return resp.MakeOkReply()
}
func FlushDB(connection commoninterface.Connection, dbm commoninterface.DBManage, cmd common.CmdLine) resp.Reply {
	if !isAuthenticated(connection) {
		return resp.MakeErrReply("NOAUTH Authentication required.")
	}
	if len(cmd) != 1 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'flushdb' command")
	}
	dbIndex, err := strconv.Atoi(string(cmd[0]))
	if err != nil {
		return resp.MakeErrReply("ERR value is not an integer ")
	}
	if dbIndex < 0 || dbIndex >= config.Properties.Databases {
		return resp.MakeErrReply("ERR value is  out of range")
	}
	dbm.FlushDB(dbIndex)
	return resp.MakeOkReply()
}
func FlushAll(connection commoninterface.Connection, dbm commoninterface.DBManage, cmd common.CmdLine) resp.Reply {
	if !isAuthenticated(connection) {
		return resp.MakeErrReply("NOAUTH Authentication required.")
	}
	if len(cmd) != 0 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'flushall' command")
	}
	dbm.FlushAll()
	return resp.MakeOkReply()
}
