package db

import (
	"mygodis/common"
	"mygodis/config"
	"testing"
)

func TestStandaloneDatabaseManager_AddAof(t *testing.T) {
	config.SetupConfig("E:\\golangPropjects\\mygodis\\redis.conf")
	server := MakeStandaloneServer()
	line := common.CmdLine{
		[]byte("set"), []byte("a"), []byte("1"),
	}
	server.AddAof(0, line)
}
