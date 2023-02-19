package server

import (
	"mygodis/db"
	"sync"
	"sync/atomic"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

type Handler struct {
	activeConn sync.Map // *client -> placeholder
	db         db.DataBaseImpl
	closing    atomic.Bool // refusing new client and new request
}
