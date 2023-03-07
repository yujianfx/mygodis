package server

import (
	"context"
	"io"
	"mygodis/clientc"
	"mygodis/cluster"
	"mygodis/common/commoninterface"
	"mygodis/config"
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
	activeConn sync.Map // *client -> placeholder
	db         commoninterface.DB
	closing    atomic.Bool // refusing new client and new request
}

func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Load() {
		_ = conn.Close()
		return
	}
	connection := clientc.NewConn(conn)
	h.activeConn.Store(connection, nil)
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
	connection.Close()
	h.db.AfterClientClose(connection)
	h.activeConn.Delete(connection)
}

func MakeHandler() *Handler {
	var dbi commoninterface.DB
	clusterEnable := config.Properties.ClusterEnable
	if clusterEnable {
		dbi = cluster.MakeCluster()
	} else {
		dbi = db.MakeStandaloneServer()
	}
	return &Handler{
		db: dbi,
	}

}
