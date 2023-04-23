package commoninterface

import (
	"context"
	"net"
	"sync"
)

type HandleFunc func(ctx context.Context, conn net.Conn)

type Handler interface {
	Handle(ctx context.Context, conn net.Conn)
	Close() error
	Clients() *sync.Map
}
