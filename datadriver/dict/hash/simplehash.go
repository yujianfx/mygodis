package hash

type SimpleHash struct {
}

func (s *SimpleHash) HashCode(key []byte) uint64 {
	hash := 0
	for _, c := range key {
		hash = hash*31 + int(c)
	}
	return uint64(hash)
}
