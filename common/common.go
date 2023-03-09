package common

type CmdLine [][]byte
type DBInfo struct {
	InfoKey   string
	InfoValue string
}

const (
	SERVER_INFO = InfoType(1) << iota
	CLIENT_INFO
	CLUSTER_INFO
	MEMORY_INFO
	PERSISTENCE_INFO
	STATS_INFO
	REPLICATION_INFO
	CPU_INFO
	ALL_INFO
)

type InfoType uint
