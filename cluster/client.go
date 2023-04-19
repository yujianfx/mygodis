package cluster

import (
	cm "mygodis/common"
	"mygodis/parse"
	"mygodis/resp"
	"net"
	"sync"
	"time"
)

const timeout = 3 * time.Second
const bufferSize = 1 << 12

type Client struct {
	conn  net.Conn
	addr  string
	bytes *sync.Pool
}

func MakeClient(addr string) *Client {
	pool := &sync.Pool{New: func() any {
		return make([]byte, bufferSize)
	}}
	r := &Client{addr: addr, bytes: pool}
	return r
}
func (c *Client) Start() error {
	conn, err := net.DialTimeout("tcp", c.addr, timeout)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}
func (c *Client) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}
func (c *Client) Send(cmd cm.CmdLine) (resp.Reply, error) {
	if err := c.conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	if err := c.writeCmd(cmd); err != nil {
		return nil, err
	}
	bytes := c.bytes.Get().([]byte)
	defer c.bytes.Put(bytes)
	if n, err := c.conn.Read(bytes); err != nil {
		return nil, err
	} else {
		payload, err := parse.ParseOne(bytes[:n])
		if err != nil {
			return nil, err
		}
		return payload, nil
	}

}
func (c *Client) writeCmd(cmd cm.CmdLine) error {
	multiBulk := resp.MakeMultiBulkReply(cmd)
	_, err := c.conn.Write(multiBulk.ToBytes())
	return err
}
