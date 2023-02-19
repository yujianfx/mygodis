package clientc

import (
	"mygodis/lib/sync/wait"
	logger "mygodis/log"
	"net"
	"sync"
	"time"
)

const (
	flagSlave = uint64(1 << iota)

	flagMaster

	flagMulti
)

type ClientConnection struct {
	conn net.Conn

	// wait until finish sending data, used for graceful shutdown
	wt wait.Wait

	// lock while server sending response
	mu    sync.Mutex
	flags uint64

	// subscribing channels
	subs map[string]bool

	// password may be changed by CONFIG command during runtime, so store the password
	password string

	// queued commands for `multi`
	queue    [][][]byte
	watching map[string]uint32
	txErrors []error

	// selected db
	selectedDB int
}

var connPool = sync.Pool{
	New: func() any {
		return &ClientConnection{}
	},
}

func (c *ClientConnection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}
func (c *ClientConnection) Close() error {
	c.wt.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	c.subs = nil
	c.password = ""
	c.queue = nil
	c.watching = nil
	c.txErrors = nil
	c.selectedDB = 0
	connPool.Put(c)
	return nil
}
func NewConn(conn net.Conn) *ClientConnection {
	c, ok := connPool.Get().(*ClientConnection)
	if !ok {
		logger.Error("connection pool make wrong type")
		return &ClientConnection{
			conn: conn,
		}
	}
	c.conn = conn
	return c
}

func (c *ClientConnection) Write(bytes []byte) (int, error) {
	if len(bytes) == 0 {
		return 0, nil
	}
	c.wt.Add(1)
	defer c.wt.Done()
	return c.conn.Write(bytes)
}

func (c *ClientConnection) SetPassword(s string) {
	c.password = s
}

func (c *ClientConnection) GetPassword() string {
	return c.password
}

func (c *ClientConnection) Subscribe(channel string) {
	//TODO implement me
	panic("implement me")
}

func (c *ClientConnection) UnSubscribe(channel string) {
	//TODO implement me
	panic("implement me")
}

func (c *ClientConnection) SubsCount() int {
	return len(c.subs)
}

func (c *ClientConnection) GetChannels() []string {
	channels := make([]string, 0, len(c.subs))
	for channel := range c.subs {
		channels = append(channels, channel)
	}
	return channels
}

func (c *ClientConnection) InMultiState() bool {
	return c.flags&flagMulti > 0
}

func (c *ClientConnection) SetMultiState(b bool) {
	if !b {
		c.watching = nil
		c.txErrors = nil
		c.queue = nil
		c.flags &= ^flagMulti
		return
	}
	c.flags |= flagMulti
}

func (c *ClientConnection) GetQueuedCmdLine() [][][]byte {
	return c.queue
}

func (c *ClientConnection) EnqueueCmd(i [][]byte) {
	c.queue = append(c.queue, i)
}

func (c *ClientConnection) ClearQueuedCmds() {
	c.queue = nil
}

func (c *ClientConnection) GetWatching() map[string]uint32 {
	if c.watching == nil {
		return make(map[string]uint32)
	}
	return c.watching
}

func (c *ClientConnection) AddTxError(err error) {
	c.txErrors = append(c.txErrors, err)
}

func (c *ClientConnection) GetTxErrors() []error {
	return c.txErrors
}

func (c *ClientConnection) GetDBIndex() int {
	return c.selectedDB
}

func (c *ClientConnection) SelectDB(i int) {
	c.selectedDB = i
}

func (c *ClientConnection) SetSlave() {
	c.flags |= flagSlave
}

func (c *ClientConnection) IsSlave() bool {
	return c.flags&flagSlave > 0
}

func (c *ClientConnection) SetMaster() {
	c.flags |= flagMaster
}

func (c *ClientConnection) IsMaster() bool {
	return c.flags&flagMaster > 0
}

func (c *ClientConnection) Name() string {
	if c.conn != nil {
		return c.conn.RemoteAddr().String()
	}
	return "nil"
}
