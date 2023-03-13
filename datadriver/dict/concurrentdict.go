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
	reHashTriggerCount int
	reHashFinishCount  int
	capacity           int
	size               int
	oldTable           []bucket
	newTable           []bucket
	expansionLoad      float32
	shrinkageLoad      float32
	rehashing          bool
	reHashAction       uint16 // 0: not rehashing, 2: rehashing and expansion 4: rehashing and shrinkage
	rehashIndex        int
	rehashFactor       float32
	hash               hash.Hash
	rwLock             sync.RWMutex
}
type bucket interface {
	remove(key string) any
	get(key string) any
	add(key string, val any) bool
	forEach(consumer func(key string, val any) bool)
	keys() []string
	entries() []*MapEntry
	clear()
}
type listBucket struct {
	list.List
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
	h.hash = hashs[SimpleHash]
	for i := range h.oldTable {
		h.oldTable[i] = new(listBucket)
	}
	return h
}
func (hm *ConcurrentDict) Put(key string, val any) int {
	result := 0
	if hm.rehashing {
		result = hm.addEntry(key, val, &hm.newTable)
		return result
	}
	result = hm.addEntry(key, val, &hm.oldTable)
	hm.rehashFactor = float32(hm.size) / float32(hm.capacity)
	if hm.rehashFactor > hm.expansionLoad || hm.rehashFactor < hm.shrinkageLoad { //如果负载因子超过阈值，开始进行rehash操作 并且容量没有达到最大值或初始值
		hm.reHashInit()
	}
	return result
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
	if hm.rehashing {
		return hm.remove(key)
	}
	hm.rehashFactor = float32(hm.size) / float32(hm.capacity)
	if hm.rehashFactor > hm.expansionLoad || hm.rehashFactor < hm.shrinkageLoad { //如果负载因子超过阈值，开始进行rehash操作
		hm.reHashInit()
	}
	return hm.remove(key)
}
func (hm *ConcurrentDict) ForEach(consumer Consumer) {
	if hm.rehashing {
		hm.forEachTable(&hm.oldTable, consumer)
		hm.forEachTable(&hm.newTable, consumer)
	} else {
		hm.forEachTable(&hm.oldTable, consumer)
	}
}
func (hm *ConcurrentDict) Keys() []string {
	keys := make([]string, 0, hm.size)
	allocFunc := func(key string, val any) bool {
		keys = append(keys, key)
		return true
	}
	hm.ForEach(allocFunc)
	return keys
}
func (hm *ConcurrentDict) RandomKeys(limit int) (result []string) {
	if hm.rehashing {
		tableKeys := hm.collectTableKeys(&hm.oldTable, limit)
		if len(tableKeys) == limit {
			result = append(result, tableKeys...)
			return result
		}
		tableKeys = hm.collectTableKeys(&hm.newTable, limit-len(result))
		result = append(result, tableKeys...)
		return result
	}
	tableKeys := hm.collectTableKeys(&hm.oldTable, limit)
	result = append(result, tableKeys...)

	return result
}
func (hm *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	return hm.RandomKeys(limit)
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
func (hm *ConcurrentDict) get(key string) (result any, ok bool) {
	hash := hm.hash.HashCode([]byte(key))
	currentIndex := hash % uint64(hm.capacity)
	if hm.rehashing {
		newIndex := uint64(0)
		if hm.reHashAction == ReHashExpansion {
			newIndex = hash % uint64(hm.capacity<<1)
		} else {
			newIndex = hash % uint64(hm.capacity>>1)
		}
		oldBucket := hm.oldTable[currentIndex]
		if oldBucket != nil {
			result = oldBucket.get(key)
			hm.reHash()
			return result, result != nil
		}
		newBucket := hm.newTable[newIndex]
		if newBucket != nil {
			if newBucket.get(key) != nil {
				result = newBucket.get(key)
				hm.reHash()
				return result, result != nil
			}
		}
		return nil, false
	}
	oldBucket := hm.oldTable[currentIndex]
	result = oldBucket.get(key)
	return result, result != nil
}
func (hm *ConcurrentDict) addEntry(key string, val any, table *[]bucket) int {
	hash := hm.hash.HashCode([]byte(key))
	index := uint64(0)
	if hm.rehashing {
		if hm.reHashAction == ReHashExpansion {
			index = hash % uint64(hm.capacity<<1)
		} else {
			index = hash % uint64(hm.capacity>>1)
		}
	} else {
		index = hash % uint64(hm.capacity)
	}
	entry := new(MapEntry)
	entry.key = key
	entry.val = val
	if (*table)[index] == nil {
		(*table)[index] = new(listBucket)
	}
	result := (*table)[index].add(key, val)
	hm.size++
	if hm.rehashing {
		hm.reHashTriggerCount++
		hm.reHash()
	}
	if result {
		return 1
	}
	return 0
}
func (hm *ConcurrentDict) finishRehash() {
	hm.oldTable = hm.newTable
	hm.newTable = nil
	if hm.reHashAction == ReHashExpansion {
		hm.capacity = hm.capacity << 1
		hm.rehashFactor *= 0.5
	} else {
		hm.capacity = hm.capacity >> 1
		hm.rehashFactor *= 2
	}
	hm.rehashing = false
	hm.rehashIndex = 0
	hm.reHashAction = NotRehash
}
func (hm *ConcurrentDict) reHashInit() {
	hm.rehashing = true
	hm.rehashIndex = 0
	newCapacity := 0
	if hm.rehashFactor >= hm.expansionLoad {
		hm.reHashAction = ReHashExpansion
		newCapacity = hm.capacity << 1
	} else {
		hm.reHashAction = ReHashShrinkage
		newCapacity = hm.capacity >> 1
	}
	if hm.newTable == nil {
		hm.newTable = make([]bucket, newCapacity)
	}
}
func (hm *ConcurrentDict) reHashMove() {
	oldBucket := hm.oldTable[hm.rehashIndex]
	oldBucket.forEach(func(key string, val any) bool {
		hash := hm.hash.HashCode([]byte(key))
		newIndex := uint64(0)
		if hm.reHashAction == ReHashExpansion {
			newIndex = hash % uint64(hm.capacity<<1)
		} else {
			newIndex = hash % uint64(hm.capacity>>1)
		}
		if hm.newTable[newIndex] == nil {
			hm.newTable[newIndex] = new(listBucket)
		}
		hm.newTable[newIndex].add(key, val)
		return true
	})
	hm.oldTable[hm.rehashIndex] = nil
	hm.rehashIndex++
}
func (hm *ConcurrentDict) remove(key string) (result any, state int) {
	removeFromBucket := func(bkt bucket) (any, int) {
		val := bkt.remove(key)
		if val != nil {
			hm.size--
			return val, 1
		}
		return nil, 0
	}
	hash := hm.hash.HashCode([]byte(key))
	currentIndex := hash % uint64(hm.capacity)
	if hm.rehashing {
		newIndex := uint64(0)
		if hm.reHashAction == ReHashExpansion {
			newIndex = hash % uint64(hm.capacity<<1)
		} else {
			newIndex = hash % uint64(hm.capacity>>1)
		}
		oldBucket := hm.oldTable[currentIndex]
		if oldBucket != nil {
			return removeFromBucket(oldBucket)
		}
		newBucket := hm.newTable[newIndex]
		if newBucket != nil {
			return removeFromBucket(newBucket)
		}
		return nil, 0
	}
	result, state = removeFromBucket(hm.oldTable[currentIndex])
	return
}
func (hm *ConcurrentDict) collectTableKeys(bktes *[]bucket, limit int) (keys []string) {
	count := 0
	tLen := len(*bktes)
	randIndex := rand.Int31n(int32(tLen))
	for i := 0; i < tLen; i++ {
		index := (i + int(randIndex)) % tLen
		bkt := (*bktes)[index]
		if bkt != nil {
			allocatedKeys := bkt.keys()
			length := len(allocatedKeys)
			if length+count < limit {
				count += length
				keys = append(keys, allocatedKeys...)
			} else {
				allocatedKeys = allocatedKeys[:limit-count]
				keys = append(keys, allocatedKeys...)
				return keys
			}
		}
	}
	return keys
}
func (hm *ConcurrentDict) reHash() {
	//找到下一个不为空的桶
	for hm.rehashIndex < hm.capacity && hm.oldTable[hm.rehashIndex] == nil {
		hm.rehashIndex++
	}
	if hm.rehashIndex >= hm.capacity {
		hm.reHashFinishCount++
		hm.finishRehash()
	} else {
		hm.reHashMove()
	}
}
func (hm *ConcurrentDict) forEachTable(table *[]bucket, consumer Consumer) {
	n := rand.Int31n(int32(len(*table)))
	for i := 0; i < len(*table); i++ {
		index := (i + int(n)) % len(*table)
		bkt := (*table)[index]
		if bkt != nil {
			bkt.forEach(consumer)
		}
	}
}
func (l *listBucket) remove(key string) any {
	for e := l.List.Front(); e != nil; e = e.Next() {
		entry := e.Value.(*MapEntry)
		if entry.key == key {
			return l.Remove(e).(*MapEntry).val
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
func (l *listBucket) add(key string, val any) bool {
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
		consumer(entry.key, entry.val)
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
