package server

import (
	"context"
	"io"
	"mygodis/clientc"
	"mygodis/cluster"
	"mygodis/common/commoninterface"
	"mygodis/config"
	"mygodis/dashboard"
	"mygodis/db"
	logger "mygodis/log"
	"mygodis/parse"
	"mygodis/resp"
	"net"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown Error\r\n")
)

type Handler struct {
	activeConn *sync.Map
	db         commoninterface.DB
	closing    atomic.Bool
}

func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Load() {
		_ = conn.Close()
		return
	}
	connection := clientc.NewConn(conn)
	h.activeConn.Store(connection, nil)
	h.db.AddClient(connection)
	payloadCh := parse.Parse(conn)
	for payload := range payloadCh {
		if payload.Err != nil {
			if payload.Err == io.EOF || payload.Err == io.ErrUnexpectedEOF || strings.Contains(payload.Err.Error(), "use of closed network connection") {
				h.closeConnection(connection)
				logger.Info("connection closed: " + connection.RemoteAddr())
				return
			}
			errReply := resp.MakeErrReply(payload.Err.Error())
			_, werr := conn.Write(errReply.ToBytes())
			if werr != nil {
				h.closeConnection(connection)
				logger.Error("write error: " + werr.Error())
				return
			}
			continue
		}
		if payload.Data == nil {
			logger.Error("payload data is nil")
			continue
		}
		reply, ok := payload.Data.(*resp.MultiBulkReply)
		if !ok {
			logger.Error("payload data is not MultiBulkReply")
			continue
		}

		execResult := h.db.Exec(connection, reply.Args)
		if execResult != nil {
			num, err := connection.Write(execResult.ToBytes())
			if err != nil || num == 0 {
				h.closeConnection(connection)
				logger.Error("write error: " + err.Error())
				return
			}
		} else {
			num, err := connection.Write(unknownErrReplyBytes)
			if err != nil || num == 0 {
				h.closeConnection(connection)
				logger.Error("write error: " + err.Error())
				return
			}
		}

	}
}

func (h *Handler) Close() error {
	h.closing.Swap(true)
	h.activeConn.Range(func(key, value any) bool {
		h.closeConnection(key.(commoninterface.Connection))
		return true
	})
	h.db.Close()
	return nil
}

func (h *Handler) closeConnection(connection commoninterface.Connection) {
	err := connection.Close()
	if err != nil {
		return
	}
	h.db.RemoveClient(connection)
	h.db.AfterClientClose(connection)
	h.activeConn.Delete(connection)
}
func (h *Handler) Clients() *sync.Map {
	return h.activeConn
}

func MakeHandler() *Handler {
	var dbi commoninterface.DB
	clusterEnable := config.Properties.ClusterEnable

	if clusterEnable {
		dbi = cluster.MakeCluster()
		logger.Info("start with cluster mode")
	} else {
		dbi = db.MakeStandaloneServer()
		go initDashBoard()
		logger.Info("start with standalone mode")
	}
	return &Handler{
		db:         dbi,
		activeConn: new(sync.Map),
	}

}
func initDashBoard() {
	dashboard.DefaultDashboard.Start()
}
