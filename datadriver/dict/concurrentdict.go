package dict

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"reflect"
	"strconv"
	"sync"
)

const (
	dictMinSize = 4
	dictMaxSize = 1 << 30
)
const (
	dictExpandFactor = 0.8
	dictShrinkFactor = 0.1
)

type ConcurrentDict struct {
	ht          [2]dictht // 使用两个哈希表实现渐进式rehash
	reHashIndex int64     // 下一个要重哈希的桶的索引。-1表示没有进行重哈希
	mu          sync.RWMutex
}

func (d *ConcurrentDict) Get(key string) (val any, exists bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	find := d.dictFind(key)
	if find == nil {
		return nil, false
	}
	return find.value, true
}

func (d *ConcurrentDict) Len() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return int(d.ht[0].used + d.ht[1].used)
}

func (d *ConcurrentDict) Put(key string, val any) (result int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	dictDelete := d.dictDelete(key)
	if dictDelete {
		d.dictAdd(key, val)
		return 0
	}
	d.dictAdd(key, val)
	return 1
}

func (d *ConcurrentDict) PutIfAbsent(key string, val any) (result int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	find := d.dictFind(key)
	if find == nil {
		d.dictAdd(key, val)
		return 1
	}
	return 0
}

func (d *ConcurrentDict) PutIfExists(key string, val any) (result int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	find := d.dictFind(key)
	if find != nil {
		find.value = val
		return 1
	}
	return 0
}

func (d *ConcurrentDict) Remove(key string) (val any, result int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	find := d.dictFind(key)
	if find == nil {
		return nil, 0
	}
	d.dictDelete(key)
	return find.value, 1
}

func (d *ConcurrentDict) ForEach(consumer Consumer) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	for _, ht := range d.ht {
		for _, entry := range ht.table {
			for entry != nil {
				consumer(entry.key.(string), entry.value)
				entry = entry.next
			}
		}
	}
}

func (d *ConcurrentDict) Keys() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	keys := make([]string, 0, d.Len())
	d.ForEach(func(key string, val any) bool {
		keys = append(keys, key)
		return true
	})
	return keys
}

func (d *ConcurrentDict) RandomKeys(limit int) []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	keys := d.Keys()
	if len(keys) <= limit {
		return keys
	}
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})
	return keys[:limit]
}

func (d *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	keys := make([]string, 0, limit)
	d.ForEach(func(key string, val any) bool {
		if len(keys) >= limit {
			return false
		}
		keys = append(keys, key)
		return true
	})
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})
	return keys
}

func (d *ConcurrentDict) Clear() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.clear()
}

// dictht是一个单独的哈希表
type dictht struct {
	table    []*dictEntry // 哈希表
	size     uint64       // 哈希表的大小
	sizemask uint64       // 用于从键的哈希值中获取表中索引的掩码
	used     uint64       // 哈希表中的条目数
}

// dictEntry是哈希表的节点
type dictEntry struct {
	key   any        // 条目的键
	value any        // 条目的值
	next  *dictEntry // 同一桶中的下一个条目
}

func (d *ConcurrentDict) dictFactor() float64 {
	return float64(d.ht[0].used) / float64(d.ht[0].size)
}

// dictRehash将一些桶从旧表重新哈希到新表中。
// 如果仍然需要重新哈希，则返回true，如果重新哈希已完成，则返回false n是要重哈希的桶的数量。
func (d *ConcurrentDict) dictRehash(n int) bool {
	if d.reHashIndex == -1 {
		return false
	}
	for n > 0 {
		ht0 := &d.ht[0]
		ht1 := &d.ht[1]
		if ht0.used == 0 {
			ht0, ht1 = ht1, ht0
			d.ht[0], d.ht[1] = *ht0, *ht1
			d.reHashIndex = -1
			break
		}
		for uint64(d.reHashIndex) < ht0.sizemask && ht0.table[d.reHashIndex] == nil {
			d.reHashIndex++
		}

		entry := ht0.table[d.reHashIndex]
		for entry != nil {
			next := entry.next

			idx := dictHashFunction(entry.key) & ht1.sizemask
			entry.next = ht1.table[idx]
			ht1.table[idx] = entry
			ht1.used++
			ht0.used--

			entry = next
		}

		ht0.table[d.reHashIndex] = nil
		d.reHashIndex++
		n--
	}
	return d.reHashIndex != -1
}
func (d *ConcurrentDict) dictAdd(key, value any) bool {
	if d.reHashIndex != -1 {
		hash := dictHashFunction(key)
		ht := &d.ht[1]
		idx := hash & ht.sizemask

		for entry := ht.table[idx]; entry != nil; entry = entry.next {
			if reflect.DeepEqual(entry.key, key) {
				return false
			}
		}
		newEntry := &dictEntry{
			key:   key,
			value: value,
			next:  ht.table[idx],
		}
		ht.table[idx] = newEntry
		ht.used++
		d.dictRehash(1)
		return true
	}
	hash := dictHashFunction(key)
	ht := &d.ht[0]
	idx := hash & ht.sizemask

	for entry := ht.table[idx]; entry != nil; entry = entry.next {
		if reflect.DeepEqual(entry.key, key) {
			return false
		}
	}
	newEntry := &dictEntry{
		key:   key,
		value: value,
		next:  ht.table[idx],
	}
	ht.table[idx] = newEntry
	ht.used++

	if d.dictFactor() > dictExpandFactor && d.reHashIndex == -1 {
		targetSize := ht.size << 1
		if targetSize < dictMinSize {
			targetSize = dictMinSize
		}
		if targetSize > dictMaxSize {
			targetSize = dictMaxSize
		}
		d.dictResize(targetSize)
	}
	return true
}
func (d *ConcurrentDict) dictFind(key any) *dictEntry {
	if d.reHashIndex != -1 {
		d.dictRehash(1)
	}
	hash := dictHashFunction(key)
	for table := 0; table <= 1; table++ {
		ht := &d.ht[table]
		idx := hash & ht.sizemask
		entry := ht.table[idx]
		for entry != nil {
			if reflect.DeepEqual(entry.key, key) {
				return entry
			}
			entry = entry.next
		}
		if d.reHashIndex == -1 {
			break
		}
	}

	return nil
}
func (d *ConcurrentDict) dictDelete(key any) bool {
	if d.reHashIndex != -1 {
		d.dictRehash(1)
	}
	hash := dictHashFunction(key)
	for table := 0; table <= 1; table++ {
		ht := &d.ht[table]
		idx := hash & ht.sizemask
		entry := ht.table[idx]
		prev := (*dictEntry)(nil)
		for entry != nil {
			if reflect.DeepEqual(entry.key, key) {
				if prev == nil {
					ht.table[idx] = entry.next
				} else {
					prev.next = entry.next
				}
				if d.reHashIndex == -1 && d.dictFactor() < dictShrinkFactor {
					newSize := ht.size >> 1
					if newSize < dictMinSize {
						newSize = dictMinSize
					}
					d.dictResize(newSize)
				}
				ht.used--
				return true
			}
			prev = entry
			entry = entry.next
		}
		if d.reHashIndex == -1 {
			break
		}
	}

	return false
}
func (d *ConcurrentDict) dictResize(size uint64) {
	// 如果两个哈希表中只有一个是非空的,则扩展充满的表。否则创建一个新的表。
	if d.ht[0].used == 0 {
		d.ht[0].size = size
		d.ht[0].table = make([]*dictEntry, size)
		d.ht[0].sizemask = size - 1
		d.ht[0].used = 0
		d.reHashIndex = 0
		return
	}
	if d.ht[1].used == 0 {
		d.ht[1].size = size
		d.ht[1].table = make([]*dictEntry, size)
		d.ht[1].sizemask = size - 1
		d.ht[1].used = 0
		d.reHashIndex = 0
		return
	}
	newHT := &dictht{
		table:    make([]*dictEntry, size),
		sizemask: size - 1,
		used:     0,
		size:     size,
	}
	d.ht[0] = *newHT
}
func (d *ConcurrentDict) dictPrintStats() {
	ht0 := &d.ht[0]
	ht1 := &d.ht[1]
	total := ht0.used + ht1.used
	fmt.Println("########################")
	fmt.Println("哈希表统计信息:")
	fmt.Println("rehash索引:", d.reHashIndex)
	fmt.Printf("负载因子:%f", float64(ht0.used)/float64(ht0.size))
	println("总数:", total)
	println("表0大小:", ht0.size, "使用:", ht0.used)
	println("表1大小:", ht1.size, "使用:", ht1.used)
}
func NewConcurrentDict() *ConcurrentDict {
	d := &ConcurrentDict{}
	d.reHashIndex = -1
	d.ht[0] = dictht{
		table:    make([]*dictEntry, dictMinSize),
		size:     dictMinSize,
		sizemask: dictMinSize - 1,
		used:     0,
	}
	return d
}
func dictHashFunction(key any) uint64 {
	h := fnv.New64a()
	switch k := key.(type) {
	case int:
		h.Write([]byte(strconv.Itoa(k)))
	case string:
		h.Write([]byte(k))
	// 更多类型...
	default:
		panic(fmt.Sprintf("unsupported key type: %s", reflect.TypeOf(key)))
	}
	return h.Sum64()
}
func (d *ConcurrentDict) dictGetRandomKey() *dictEntry {
	if d.reHashIndex != -1 {
		d.dictRehash(1)
	}
	ht := &d.ht[0]
	if ht.used == 0 {
		ht = &d.ht[1]
		if ht.used == 0 {
			return nil
		}
	}
	for {
		idx := rand.Intn(int(ht.size))
		entry := ht.table[idx]
		if entry != nil {
			return entry
		}
	}
}
func (d *ConcurrentDict) forEach(fn func(key any, value any)) {
	if d.reHashIndex != -1 {
		d.dictRehash(1)
	}
	for table := 0; table <= 1; table++ {
		ht := &d.ht[table]
		for _, entry := range ht.table {
			for entry != nil {
				fn(entry.key, entry.value)
				entry = entry.next
			}
		}
	}
}
func (d *ConcurrentDict) keys() []any {
	keys := make([]any, 0)
	d.forEach(func(key any, value any) {
		keys = append(keys, key)
	})
	return keys
}
func (d *ConcurrentDict) values() []any {
	values := make([]any, 0)
	d.forEach(func(key any, value any) {
		values = append(values, value)
	})
	return values
}
func (d *ConcurrentDict) clear() {
	d.reHashIndex = -1
	d.ht[0] = dictht{
		table:    make([]*dictEntry, dictMinSize),
		size:     dictMinSize,
		sizemask: dictMinSize - 1,
		used:     0,
	}
}
