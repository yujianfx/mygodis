package wait

import (
	"sync"
	"time"
)

type Wait struct {
	wg sync.WaitGroup
}

func MakeWait() *Wait {
	return &Wait{
		wg: sync.WaitGroup{},
	}
}

func (w *Wait) Add(delta int) {
	w.wg.Add(delta)
}

func (w *Wait) Done() {
	w.wg.Done()
}
func (w *Wait) Wait() {
	w.wg.Wait()
}
func (w *Wait) WaitWithTimeout(timeout time.Duration) bool {
	c := make(chan struct{}, 1)
	go func() {
		defer close(c)
		w.Wait()
		c <- struct{}{}
	}()
	select {
	case <-c:
		return true //正常完成
	case <-time.After(timeout): //超时
		return false
	}

}
func (w *Wait) DoWithTimeOut(do func(), duration time.Duration) bool {
	c := make(chan struct{}, 1)
	go func() {
		defer close(c)
		do()
		c <- struct{}{}
	}()
	select {
	case <-c:
		return true //正常完成
	case <-time.After(duration): //超时
		return false
	}

}
