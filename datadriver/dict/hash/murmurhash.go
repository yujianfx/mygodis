package hash

import logger "mygodis/log"

type MurmurHash struct {
	seed uint32
}

func (mh MurmurHash) SetSeed(seed uint32) {
	mh.seed = seed
}

func (mh *MurmurHash) HashCode(key []byte) uint64 {
	if mh.seed == 0 {
		logger.Warn("MurmurHash seed is 0, use default seed 0x1234ABCD")
		mh.seed = 0x1234ABCD
	}
	const (
		m    = 0x5bd1e995
		r    = 24
		mask = 0xffffffff
	)
	length := uint32(len(key))
	h := mh.seed ^ length
	var (
		k uint32
	)
	for length >= 4 {
		k = uint32(key[0]) | uint32(key[1])<<8 | uint32(key[2])<<16 | uint32(key[3])<<24
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
		key = key[4:]
		length -= 4
	}
	switch length {
	case 3:
		h ^= uint32(key[2]) << 16
		fallthrough
	case 2:
		h ^= uint32(key[1]) << 8
		fallthrough
	case 1:
		h ^= uint32(key[0])
		h *= m
	}
	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return uint64(h & mask)
}
