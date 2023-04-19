package pool

import (
	"errors"
	"sync"
)

type Factory func() (any, error)
type Finalizer func(any)

var ErrClosed = errors.New("pool closed")

type Config struct {
	MaxIdle   uint
	MaxActive uint
}
type Pool struct {
	Config
	factory     Factory
	finalizer   Finalizer
	idles       chan any
	activeCount uint
	mu          sync.Mutex
	cond        *sync.Cond
	closed      bool
}

func NewPool(factory Factory, finalizer Finalizer, cfg Config) *Pool {
	p := &Pool{
		factory:   factory,
		finalizer: finalizer,
		idles:     make(chan any, cfg.MaxIdle),
		Config:    cfg,
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}
func (pool *Pool) getOnNoIdle() (any, error) {
	for pool.activeCount >= pool.MaxActive {
		pool.cond.Wait()
	}
	pool.activeCount++
	pool.mu.Unlock()
	x, err := pool.factory()
	if err != nil {
		pool.mu.Lock()
		pool.activeCount--
		pool.mu.Unlock()
		return nil, err
	}
	return x, nil
}
func (pool *Pool) Get() (any, error) {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return nil, ErrClosed
	}
	select {
	case item := <-pool.idles:
		pool.mu.Unlock()
		return item, nil
	default:
		return pool.getOnNoIdle()
	}
}
func (pool *Pool) Put(x any) {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		pool.finalizer(x)
		return
	}
	select {
	case pool.idles <- x:
		pool.mu.Unlock()
		return
	default:
		pool.mu.Unlock()
		pool.activeCount--
		pool.finalizer(x)
		pool.mu.Lock()
		pool.cond.Broadcast()
		pool.mu.Unlock()
	}
}
func (pool *Pool) Close() {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return
	}
	pool.closed = true
	close(pool.idles)
	pool.mu.Unlock()
	pool.Drain()
}
func (pool *Pool) Drain() {
	pool.mu.Lock()
	for pool.idles != nil && len(pool.idles) > 0 {
		x := <-pool.idles
		pool.activeCount--
		pool.finalizer(x)
	}
	pool.mu.Unlock()
}
