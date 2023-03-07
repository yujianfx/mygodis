package tcp

import (
	"context"
	logger "mygodis/log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Config struct {
	Address    string        `yaml:"address"`
	MaxConnect uint32        `yaml:"max-connect"`
	Timeout    time.Duration `yaml:"timeout"`
}

func ListenAndServeWithSignal(config *Config, handler Handler) error {
	closeC := make(chan struct{})
	sigC := make(chan os.Signal)
	signal.Notify(sigC, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigC
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeC <- struct{}{}
		}
	}()
	listener, err := net.Listen("tcp", config.Address)
	if err != nil {
		return err
	}
	logger.Info("bind: %s, start listening...", config.Address)
	listenAndServe(listener, handler, closeC)
	return nil
}
func listenAndServe(listener net.Listener, handler Handler, closeC <-chan struct{}) {
	errorC := make(chan error)
	defer func() {
		close(errorC)
		_ = listener.Close()
		_ = handler.Close()
	}()
	go func() {
		select {
		case <-closeC:
			logger.Info("get exit signal")
		case er := <-errorC:
			logger.Error("error: %s", er)
		}
		logger.Info("server closed")
	}()

	ctx := context.Background()
	wt := sync.WaitGroup{}
	for {
		accept, err := listener.Accept()
		if err != nil {
			errorC <- err
			break
		}
		logger.Info("accept: %s", accept.RemoteAddr())
		wt.Add(1)
		go func() {
			defer wt.Done()
			handler.Handle(ctx, accept)
		}()
	}
	wt.Wait()
}
