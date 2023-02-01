package tcp

import (
	logger "mygodis/log"
	"net"
	"os"
	"os/signal"
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
}
