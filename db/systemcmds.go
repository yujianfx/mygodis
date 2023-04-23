package db

import (
	"fmt"
	"mygodis/common"
	"mygodis/common/commoninterface"
	"mygodis/config"
	"mygodis/resp"
	"mygodis/util/cmdutil"
	"runtime"
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
func Info(connection commoninterface.Connection, d *StandaloneServer, cmd common.CmdLine) resp.Reply {
	if !isAuthenticated(connection) {
		return resp.MakeErrReply("NOAUTH Authentication required.")
	}
	if len(cmd) == 2 {
		param := string(cmd[1])
		switch param {
		case "server":
			return resp.MakeMultiBulkReply(ServerInfo(d))
		case "client":
			return resp.MakeMultiBulkReply(ClientInfo(d))
		case "cluster":
			return resp.MakeMultiBulkReply([][]byte{
				[]byte("# Cluster"),
			})
		case "memory":
			return resp.MakeMultiBulkReply(MemoryInfo(d))
		case "persistence":
			return resp.MakeMultiBulkReply(PersistenceInfo(d))
		case "cpu":
			return resp.MakeMultiBulkReply(CpuInfo(d))
		}
	}
	return AllInfo(d)
}

func ClientInfo(d *StandaloneServer) [][]byte {
	results := make([][]byte, 0)
	clients := 0
	d.activeConn.Range(func(key, value any) bool {
		clients++
		return true
	})
	results = append(results, []byte("# Clients:"))
	results = append(results, []byte(fmt.Sprintf("connected_clients:%d", clients)))
	results = append(results, []byte(fmt.Sprintf("maxclients:%d", config.Properties.MaxClients)))
	return results
}
func ServerInfo(d *StandaloneServer) [][]byte {
	results := make([][]byte, 0)
	results = append(results, []byte("# Server:"))
	results = append(results, []byte(fmt.Sprintf("server_addr:%s:%d", config.Properties.Bind, config.Properties.Port)))
	results = append(results, []byte(fmt.Sprintf("datacenter_id:%d", config.Properties.DataCenterId)))
	results = append(results, []byte(fmt.Sprintf("worker_id:%d", config.Properties.WorkerId)))
	return results
}
func MemoryInfo(d *StandaloneServer) [][]byte {
	results := make([][]byte, 0)
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	currentMemoryUsage := mem.Alloc
	results = append(results, []byte("# Memory:"))
	results = append(results, []byte(fmt.Sprintf("used_memory:%d", currentMemoryUsage)))
	return results
}
func PersistenceInfo(d *StandaloneServer) [][]byte {
	results := make([][]byte, 0)
	results = append(results, []byte("# Persistence:"))
	results = append(results, []byte(fmt.Sprintf("aof_enabled:%t", config.Properties.AppendOnly)))
	results = append(results, []byte(fmt.Sprintf("aof_file:%s", config.Properties.AppendFilename)))
	results = append(results, []byte(fmt.Sprintf("aof_size:%d", d.persister.AofSize())))
	return results
}
func CpuInfo(d *StandaloneServer) [][]byte {
	results := make([][]byte, 0)
	numCPU := runtime.NumCPU()
	results = append(results, []byte("# cpu:"))
	results = append(results, []byte(fmt.Sprintf("num_cpu:%d", numCPU)))
	return results
}
func AllInfo(d *StandaloneServer) resp.Reply {
	results := make([][]byte, 0)
	results = append(results, ServerInfo(d)...)
	results = append(results, ClientInfo(d)...)
	results = append(results, MemoryInfo(d)...)
	results = append(results, PersistenceInfo(d)...)
	results = append(results, CpuInfo(d)...)
	return resp.MakeMultiBulkReply(results)
}
