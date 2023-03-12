package dict

import (
	"container/list"
	"math/rand"
	"mygodis/datadriver/dict/hash"
	"sync"
)

const (
	SimpleHash = uint32(1) << iota
	MurmurHash
)
const (
	MinBucketCount       = 4
	DefaultExpansionLoad = 0.8
	DefaultShrinkageLoad = 0.1
	MaxBucketCount       = 1 << 30
)
const (
	NotRehash = iota << 1
	ReHashExpansion
	ReHashShrinkage
)

var initCapacity = 0
var hashs = map[uint32]hash.Hash{
	SimpleHash: &hash.SimpleHash{},
	MurmurHash: &hash.MurmurHash{},
}

type MapEntry struct {
	key string
	val any
}
type ConcurrentDict struct {
	capacity      int
	size          int
	oldTable      []bucket
	newTable      []bucket
	expansionLoad float32
	shrinkageLoad float32
	rehashing     bool
	reHashAction  uint16 // 0: not rehashing, 2: rehashing and expansion 4: rehashing and shrinkage
	rehashIndex   int
	rehashFactor  float32
	hash          hash.Hash
	rwLock        sync.RWMutex
}

func (hm *ConcurrentDict) Get(key string) (val any, exists bool) {
	return hm.get(key)
}

func (hm *ConcurrentDict) Len() int {
	return hm.size
}

func (hm *ConcurrentDict) PutIfAbsent(key string, val any) (result int) {
	if _, ok := hm.get(key); ok {
		result = 0
		return
	}
	return hm.Put(key, val)
}

func (hm *ConcurrentDict) PutIfExists(key string, val any) (result int) {
	if _, ok := hm.get(key); ok {
		return hm.Put(key, val)
	}
	result = 0
	return
}

func (hm *ConcurrentDict) Remove(key string) (val any, result int) {
	return hm.remove(key)
}
func (hm *ConcurrentDict) ForEach(consumer Consumer) {
	randIndex := rand.Intn(hm.capacity)
	for i := 0; i < hm.capacity; i++ {
		index := (randIndex + i) % hm.capacity
		if hm.rehashing {
			if index >= hm.rehashIndex {
				hm.oldTable[index].forEach(consumer)
			} else {
				hm.newTable[index].forEach(consumer)
			}
		}
		hm.oldTable[index].forEach(consumer)
	}
}

func (hm *ConcurrentDict) Keys() []string {
	keys := make([]string, hm.size)
	allocFunc := func(key string, val any) bool {
		keys = append(keys, key)
		return true
	}
	hm.ForEach(allocFunc)
	return keys
}

func (hm *ConcurrentDict) RandomKeys(limit int) []string {
	count := 0
	keys := make([]string, limit)
	allocFunc := func(key string, val any) bool {
		keys = append(keys, key)
		count++
		if count >= limit {
			return false
		}
		return true
	}
	hm.ForEach(allocFunc)
	return keys
}

func (hm *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	count := 0
	keySet := make(map[string]struct{})
	keys := make([]string, limit)
	allocFunc := func(key string, val any) bool {
		//如果key不存在keys中，将其加入到keys中
		keySet[key] = struct{}{}
		count++
		if count >= limit {
			return false
		}
		return true
	}
	hm.ForEach(allocFunc)
	for key := range keySet {
		keys = append(keys, key)
	}
	return keys
}

func (hm *ConcurrentDict) Clear() {
	hm.rwLock.Lock()
	defer hm.rwLock.Unlock()
	hm.oldTable = make([]bucket, hm.capacity)
	for i := range hm.oldTable {
		hm.oldTable[i] = new(listBucket)
	}
	hm.newTable = nil
	hm.size = 0
	hm.rehashing = false
	hm.rehashIndex = 0
	hm.rehashFactor = 0
}

func NewConcurrentDict(capacity int) *ConcurrentDict {
	if capacity < MinBucketCount {
		capacity = MinBucketCount
	}
	if capacity > MaxBucketCount {
		capacity = MaxBucketCount
	}
	initCapacity = capacity
	h := &ConcurrentDict{
		capacity:      capacity,
		oldTable:      make([]bucket, capacity),
		expansionLoad: DefaultExpansionLoad,
		shrinkageLoad: DefaultShrinkageLoad,
		rehashing:     false,
		rehashIndex:   0,
		rehashFactor:  0,
	}
	if capacity < MaxBucketCount>>1 { //达到最大容量时，不再分配新的空间
		h.newTable = make([]bucket, capacity<<1)
		for i := range h.newTable {
			h.newTable[i] = new(listBucket)
		}
	}
	h.hash = hashs[SimpleHash]
	for i := range h.oldTable {
		h.oldTable[i] = new(listBucket)
	}
	return h
}
func (hm *ConcurrentDict) Put(key string, val any) int {
	// 如果当前正在进行rehash操作，将元素插入到新哈希表中
	if hm.rehashing {
		return hm.addEntry(key, val, hm.newTable)
	}
	// 否则将元素插入到旧哈希表中
	hm.rehashFactor = float32(hm.size) / float32(hm.capacity)
	if hm.rehashFactor > hm.expansionLoad || hm.rehashFactor < hm.shrinkageLoad {
		//如果负载因子超过阈值，开始进行rehash操作 并且容量没有达到最大值或初始值
		if hm.capacity <= MaxBucketCount>>1 && hm.capacity >= initCapacity<<1 {
			if hm.rehashFactor >= hm.expansionLoad {
				hm.reHashAction = ReHashExpansion
			} else {
				hm.reHashAction = ReHashShrinkage
			}
			hm.rehashing = true
			hm.rehashIndex = 0
			return hm.addEntry(key, val, hm.newTable)
		}
		return hm.addEntry(key, val, hm.oldTable)
	}
	return hm.addEntry(key, val, hm.oldTable)
}
func (hm *ConcurrentDict) get(key string) (any, bool) {
	if hm.rehashing {
		hm.rehashMove()
	}
	hash := hm.hash.HashCode([]byte(key))
	index := hash % uint64(hm.capacity)
	oldBucket := hm.oldTable[index]
	oldget := oldBucket.get(key)
	if hm.reHashAction == ReHashExpansion {
		index = hash % uint64(hm.capacity<<1)
	} else {
		index = hash % uint64(hm.capacity>>1)
	}
	newBucket := hm.newTable[index]
	newget := newBucket.get(key)
	if oldget != nil && newget != nil {
		oldBucket.remove(key)
		return newget, true
	}

	if oldget != nil && newget == nil {
		return oldget, true
	}
	return oldget, false
}

func (hm *ConcurrentDict) addEntry(key string, val any, table []bucket) int {
	// 计算key的哈希值和数组下标
	hash := hm.hash.HashCode([]byte(key))
	index := uint64(0)
	if hm.rehashing {
		if hm.reHashAction == ReHashExpansion {
			index = hash % uint64(hm.capacity<<1)
		} else {
			if hm.capacity>>1 == 0 {
			}
			index = hash % uint64(hm.capacity>>1)
		}
	} else {
		index = hash % uint64(hm.capacity)
	}
	entry := new(MapEntry)
	entry.key = key
	entry.val = val
	// 将元素插入到链表头部
	result := table[index].put(key, val)
	hm.size++
	// 如果当前正在进行rehash操作，将元素从旧哈希表移动到新哈希表中
	if hm.rehashing {
		hm.rehashMove()
	}
	if result {
		return 1
	}
	return 0
}

type bucket interface {
	remove(key string) any
	get(key string) any
	put(key string, val any) bool
	forEach(consumer func(key string, val any) bool)
	keys() []string
	entries() []*MapEntry
	clear()
}
type listBucket struct {
	list.List
}

func (l *listBucket) remove(key string) any {
	for e := l.List.Front(); e != nil; e = e.Next() {
		entry := e.Value.(*MapEntry)
		if entry.key == key {
			l.Remove(e)
			return entry.val
		}
	}
	return nil
}

func (l *listBucket) get(key string) any {
	for e := l.List.Front(); e != nil; e = e.Next() {
		entry := e.Value.(*MapEntry)
		if entry.key == key {
			return entry.val
		}
	}
	return nil
}

func (l *listBucket) put(key string, val any) bool {
	v := &MapEntry{
		key: key,
		val: val,
	}
	result := l.List.PushFront(v) == nil
	return !result
}

func (l *listBucket) forEach(consumer func(key string, val any) bool) {
	head := l.List
	for e := head.Front(); e != nil; e = e.Next() {
		entry := e.Value.(*MapEntry)
		if !consumer(entry.key, entry.val) {
			break
		}
	}
}

func (l *listBucket) keys() []string {
	head := l.List
	keys := make([]string, 0, head.Len())
	for e := head.Front(); e != nil; e = e.Next() {
		entry := e.Value.(*MapEntry)
		keys = append(keys, entry.key)
	}
	return keys
}

func (l *listBucket) entries() []*MapEntry {
	head := l.List
	entries := make([]*MapEntry, 0, head.Len())
	for e := head.Front(); e != nil; e = e.Next() {
		entry := e.Value.(*MapEntry)
		entries = append(entries, entry)
	}
	return entries
}

func (l *listBucket) clear() {
	l.Init()
}
func (hm *ConcurrentDict) rehashMove() {
	// 逐步将元素从旧哈希表移动到新哈希表中
	for hm.rehashIndex < hm.capacity && hm.oldTable[hm.rehashIndex] == nil {
		hm.rehashIndex++
	}
	if hm.rehashIndex >= hm.capacity {
		// 所有元素都已经移动完成，将新哈希表替换旧哈希表
		hm.oldTable = hm.newTable
		if hm.capacity >= MaxBucketCount || hm.capacity <= MinBucketCount {
			hm.reHashAction = NotRehash
			hm.rehashing = false
			hm.rehashIndex = 0
			hm.rehashFactor = 1
			return
		}
		newCapacity := 0
		if hm.reHashAction == ReHashExpansion {
			newCapacity = hm.capacity << 1
			hm.rehashFactor *= 0.5
		} else {
			newCapacity = hm.capacity >> 1
			hm.rehashFactor *= 2
		}
		hm.newTable = make([]bucket, newCapacity)
		for i := range hm.newTable {
			hm.newTable[i] = new(listBucket)
		}
		hm.capacity = newCapacity
		hm.rehashing = false
		hm.rehashIndex = 0
		hm.reHashAction = NotRehash
	} else {
		// 将节点插入新哈希表
		oldBucket := hm.oldTable[hm.rehashIndex]
		oldBucket.forEach(func(key string, val any) bool {
			hash := hm.hash.HashCode([]byte(key))
			newIndex := uint64(0)
			if hm.reHashAction == ReHashExpansion {
				newIndex = hash % uint64(hm.capacity<<1)
			} else {
				newIndex = hash % uint64(hm.capacity>>1)
			}
			newBucket := hm.newTable[newIndex]
			newBucket.put(key, val)
			return true
		})
		hm.rehashIndex++
	}
}

func (hm *ConcurrentDict) remove(key string) (any, int) {
	if hm.rehashing {
		hm.rehashMove()
	}
	hash := hm.hash.HashCode([]byte(key))
	index := hash % uint64(hm.capacity)
	bucket := hm.oldTable[index]
	val := bucket.remove(key)
	if val != nil {
		hm.size--
		return val, 1
	}
	if hm.rehashing {
		if hm.reHashAction == ReHashExpansion {
			index = hash % uint64(hm.capacity<<1)
		} else {
			index = hash % uint64(hm.capacity>>1)
		}
		bucket = hm.newTable[index]
		val = bucket.remove(key)
		if val != nil {
			hm.size--
			return val, 1
		}
	}
	return nil, 0
}
