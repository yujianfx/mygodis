package cluster

import (
	"mygodis/config"
	"mygodis/lib/pool"
	logger "mygodis/log"
	"mygodis/util/cmdutil"
)

type ConnectionPool struct {
	cps map[string]*pool.Pool
}

var cpConfig = pool.Config{
	MaxIdle:   8,
	MaxActive: 16,
}

func NewConnectionPool() *ConnectionPool {
	c := &ConnectionPool{
		cps: make(map[string]*pool.Pool),
	}
	peers := config.Properties.Peers
	peers = append(peers, config.Properties.Self)
	for _, peer := range peers {
		factory := func() (any, error) {
			client := MakeClient(peer)
			err := client.Start()
			if err != nil {
				return nil, err
			}
			if config.Properties.RequirePass != "" {
				client.Send(cmdutil.ToCmdLineWithName("AUTH", config.Properties.RequirePass))
			}
			return client, nil
		}
		finalizer := func(x any) {
			client := x.(*Client)
			client.Close()
		}
		c.cps[peer] = pool.NewPool(factory, finalizer, cpConfig)
	}
	return c
}
func (p *ConnectionPool) GetConnection(targetNode string) *Client {
	obj, ok := p.cps[targetNode]
	if !ok {
		factory := func() (any, error) {
			client := MakeClient(targetNode)
			err := client.Start()
			if err != nil {
				return nil, err
			}
			if config.Properties.RequirePass != "" {
				client.Send(cmdutil.ToCmdLineWithName("AUTH", config.Properties.RequirePass))
			}
			return client, nil
		}
		finalizer := func(x any) {
			client := x.(*Client)
			client.Close()
		}
		newPool := pool.NewPool(factory, finalizer, cpConfig)
		p.cps[targetNode] = newPool
		obj = p.cps[targetNode]
		logger.Info("new connection pool for node", targetNode)
	}
	client, err := obj.Get()
	if err != nil {
		logger.Error(err.Error())
	}
	return client.(*Client)
}
func (p *ConnectionPool) AddConnection(newNodes ...string) {
	for _, newNode := range newNodes {
		if _, ok := p.cps[newNode]; ok {
			continue
		}
		factory := func() (any, error) {
			client := MakeClient(newNode)
			err := client.Start()
			if err != nil {
				return nil, err
			}
			if config.Properties.RequirePass != "" {
				client.Send(cmdutil.ToCmdLineWithName("AUTH", config.Properties.RequirePass))
			}
			return client, err
		}
		finalizer := func(x any) {
			client := x.(*Client)
			client.Close()
		}
		p.cps[newNode] = pool.NewPool(factory, finalizer, cpConfig)
	}
}
func (p *ConnectionPool) Close() {
	for _, poolItem := range p.cps {
		poolItem.Close()
	}
}
