package commoninterface

type Hash interface {
	HashCode(key []byte) uint64
}
