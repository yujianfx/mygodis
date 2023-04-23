package main

import (
	"fmt"
	"mygodis/config"
	logger "mygodis/log"
	"mygodis/server"
	"mygodis/tcp"
	"os"
)

var defaultProperties = &config.ServerProperties{
	Bind:       "0.0.0.0",
	Port:       6379,
	AppendOnly: false,
	MaxClients: 1024,
}

func main() {
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "MYGODIS",
		Ext:        "log",
		TimeFormat: "2023-03-03",
	})
	envConfig := os.Getenv("MYGODISCONFIG")
	if envConfig == "" {
		if fileExists("redis.conf") {
			config.SetupConfig("redis.conf")
		} else {
			config.Properties = defaultProperties
		}
	} else {
		config.SetupConfig(envConfig)
	}
	addr := fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port)
	tcpConfig := tcp.Config{
		Address: addr,
	}
	handler := server.MakeHandler()

	err := tcp.ListenAndServeWithSignal(&tcpConfig, handler)
	if err != nil {
		logger.Error(err)
	}
}
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}
