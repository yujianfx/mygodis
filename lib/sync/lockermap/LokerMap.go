package lockermap

import "sync"

type LockerMap struct {
	locks map[string]*sync.RWMutex
}

func NewLockerMap(size int) *LockerMap {
	return &LockerMap{
		locks: make(map[string]*sync.RWMutex, size),
	}
}
func (lm *LockerMap) WLock(key string) {
	lm.locks[key].Lock()
}
func (lm *LockerMap) RLock(key string) {
	lm.locks[key].RLock()
}
func (lm *LockerMap) WUnLock(key string) {
	lm.locks[key].Unlock()
}
func (lm *LockerMap) RUnLock(key string) {
	lm.locks[key].RUnlock()
}
func (lm *LockerMap) WLockBatch(keys ...string) {
	for _, key := range keys {
		lm.locks[key].Lock()
	}
}
func (lm *LockerMap) RLockBatch(keys ...string) {
	for _, key := range keys {
		lm.locks[key].RLock()
	}
}
func (lm *LockerMap) WUnLockBatch(keys ...string) {
	for _, key := range keys {
		lm.locks[key].Unlock()
	}
}
func (lm *LockerMap) RUnLockBatch(keys ...string) {
	for _, key := range keys {
		lm.locks[key].RUnlock()
	}
}
func (lm *LockerMap) lockOrUnLockBatch0(writeKeys []string, readKeys []string, lock bool) {
	var wg sync.WaitGroup

	// Deduplicate keys
	keysMap := make(map[string]struct{})
	deduplicatedKeys := make([]string, 0, len(writeKeys)+len(readKeys))
	for _, key := range writeKeys {
		if _, ok := keysMap[key]; !ok {
			keysMap[key] = struct{}{}
			deduplicatedKeys = append(deduplicatedKeys, key)
		}
	}
	for _, key := range readKeys {
		if _, ok := keysMap[key]; !ok {
			keysMap[key] = struct{}{}
			deduplicatedKeys = append(deduplicatedKeys, key)
		}
	}
	// Lock write locks
	for _, key := range deduplicatedKeys {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			if lock {
				lm.lock(writeKeys, k)
			}
			lm.unlock(writeKeys, k)
		}(key)
	}

	wg.Wait()
}
func (lm *LockerMap) RWLockBatch(write []string, read []string) {
	lm.lockOrUnLockBatch0(write, read, true)
}
func (lm *LockerMap) URWLockBatch(write []string, read []string) {
	lm.lockOrUnLockBatch0(write, read, false)
}
func contains(keys []string, key string) bool {
	for _, k := range keys {
		if k == key {
			return true
		}
	}
	return false
}
func (lm *LockerMap) lock(keys []string, key string) {
	if contains(keys, key) {
		lm.WLock(key)
	} else {
		lm.RLock(key)
	}
}
func (lm *LockerMap) unlock(keys []string, key string) {
	if contains(keys, key) {
		lm.WUnLock(key)
	} else {
		lm.RUnLock(key)
	}
}
