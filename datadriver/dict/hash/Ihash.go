package hash

type Hash interface {
	HashCode(key []byte) uint64
}
