package atomic

import "sync/atomic"

type Boolean uint32

func (b *Boolean) Get() bool {
	return atomic.LoadUint32((*uint32)(b)) != 0
}
func (b *Boolean) set(value bool) {
	if value {
		atomic.StoreUint32((*uint32)(b), 1)
		return
	}
	atomic.StoreUint32((*uint32)(b), 0)
}
