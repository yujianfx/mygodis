package dict

import (
	"math/rand"
	"mygodis/datadriver/dict/hash"
)

type ConcurrentDict struct {
	buckets     []*LinkedList
	bucketCount uint64
	size        uint64 // size of values
	cap         uint64 // capacity of buckets
	index       uint64 // index of buckets
	hash        hash.Hash
}

const (
	SimpleHash = uint32(0) << iota
	MurmurHash
)
const (
	InitBucketCount = 8
	MaxListLength   = 64 // max length  for rehash
)

var hashmap = map[uint32]hash.Hash{
	SimpleHash: &hash.SimpleHash{},
	MurmurHash: &hash.MurmurHash{},
}

type LinkedList struct {
	head *LinkedNode
	tail *LinkedNode
	size uint32
}
type LinkedNode struct {
	preNode  *LinkedNode
	nextNode *LinkedNode
	entity   *Entity
}
type Entity struct {
	key string
	val any
}

func newEntity(key string, val any) *Entity {
	return &Entity{key: key, val: val}
}

func (e *Entity) Key() string {
	return e.key
}

func (e *Entity) SetKey(key string) {
	e.key = key
}

func (e *Entity) Val() any {
	return e.val
}

func (e *Entity) SetVal(val any) {
	e.val = val
}

func NewConcurrentDict(size int) *ConcurrentDict {
	return &ConcurrentDict{
		bucketCount: uint64(size/MaxListLength + 1),
		buckets:     make([]*LinkedList, uint64(size/MaxListLength+1)),
		size:        0,
		cap:         uint64(size/MaxListLength+1) * MaxListLength,
		index:       0,
		hash:        hashmap[SimpleHash],
	}
}
func (cd *ConcurrentDict) SetHash(hashType uint32) {
	cd.hash = hashmap[hashType]
}
func (l *LinkedList) insertHeader(entity *Entity) {
	node := &LinkedNode{
		preNode:  nil,
		nextNode: nil,
		entity:   entity,
	}
	if l.head == nil {
		l.head = node
		l.tail = node
		return
	}
	node.nextNode = l.head.nextNode
	l.head.nextNode.preNode = node
	l.head.nextNode = node
	node.preNode = l.head
	l.size++
}
func (l *LinkedList) deleteNodeByKey(key string) (re *Entity) {
	node := l.head
	for node != nil {
		if node.entity.key == key {
			if node.preNode == nil {
				re = l.removeHead()
				return re
			}
			if node.nextNode == nil {
				re = l.removeTail()
				return re
			}
			re = node.entity
			node.preNode.nextNode = node.nextNode
			node.nextNode.preNode = node.preNode
			l.size--
			return re
		}
		node = node.nextNode
	}
	return nil
}
func (l *LinkedList) removeHead() (re *Entity) {
	if l.head == nil {
		return nil
	}
	if l.head.nextNode == nil {
		re = l.head.entity
		l.head = nil
		l.tail = nil
		return re
	}
	re = l.head.entity
	l.head = l.head.nextNode
	l.head.preNode = nil
	l.size--
	return re
}
func (l *LinkedList) removeTail() (re *Entity) {
	if l.tail == nil {
		return
	}
	if l.tail.preNode == nil {
		re = l.tail.entity
		l.head = nil
		l.tail = nil
		return
	}
	re = l.tail.entity
	l.tail = l.tail.preNode
	l.tail.nextNode = nil
	l.size--
	return re
}
func (cd *ConcurrentDict) getBucketIndex(bytes []byte) uint64 {
	return cd.hash.HashCode(bytes) % cd.bucketCount
}
func (cd *ConcurrentDict) Get(key string) (val any, exists bool) {
	bucketIndex := cd.getBucketIndex([]byte(key))
	if cd.buckets[bucketIndex] == nil {
		return nil, false
	}
	node := cd.buckets[bucketIndex].head
	for node != nil {
		if node.entity.key == key {
			return node.entity.val, true
		}
		node = node.nextNode
	}
	return nil, false
}
func (cd *ConcurrentDict) Len() int {
	return int(cd.size)
}

func (cd *ConcurrentDict) put0(key string, val any) {
	entity := newEntity(key, val)
	bucketIndex := cd.getBucketIndex([]byte(key))
	if cd.buckets[bucketIndex] == nil {
		cd.buckets[bucketIndex] = &LinkedList{}
	}
	cd.buckets[bucketIndex].insertHeader(entity)
}
func (cd *ConcurrentDict) Put(key string, val any) (result int) {
	result = 0
	entity := newEntity(key, val)
	bucketIndex := cd.getBucketIndex([]byte(key))
	if _, exists := cd.Get(key); !exists {
		result = 1
	}
	if cd.buckets[bucketIndex] == nil {
		cd.buckets[bucketIndex] = &LinkedList{}
	}
	cd.buckets[bucketIndex].insertHeader(entity)
	return result
}

func (cd *ConcurrentDict) PutIfAbsent(key string, val any) (result int) {
	if _, exist := cd.Get(key); exist {
		return 0
	}
	cd.put0(key, val)
	return 1
}

func (cd *ConcurrentDict) PutIfExists(key string, val any) (result int) {
	if _, exist := cd.Get(key); !exist {
		return 0
	}
	cd.put0(key, val)
	return 1
}

func (cd *ConcurrentDict) Remove(key string) (val any, result int) {
	bucketIndex := cd.getBucketIndex([]byte(key))
	if cd.buckets[bucketIndex] == nil {
		return nil, 0
	}
	entity := cd.buckets[bucketIndex].deleteNodeByKey(key)
	return entity.val, 1
}

func (cd *ConcurrentDict) ForEach(consumer Consumer) {
	intn := rand.Intn(int(cd.bucketCount))
	for i := 0; i < int(cd.bucketCount); i++ {
		if cd.buckets[intn] != nil {
			node := cd.buckets[intn].head
			for node != nil {
				consumer(node.entity.key, node.entity.val)
				node = node.nextNode
			}
		}
		intn = (intn + 1) % int(cd.bucketCount)
	}
}

func (cd *ConcurrentDict) Keys() []string {
	intn := rand.Intn(int(cd.bucketCount))
	keys := make([]string, 0)
	for i := 0; i < int(cd.bucketCount); i++ {
		if cd.buckets[intn] != nil {
			node := cd.buckets[intn].head
			for node != nil {
				keys = append(keys, node.entity.key)
				node = node.nextNode
			}
		}
		intn = (intn + 1) % int(cd.bucketCount)
	}
	return keys
}

func (cd *ConcurrentDict) RandomKeys(limit int) []string {
	intn := rand.Intn(int(cd.bucketCount))
	keys := make([]string, 0)
	for i := 0; i < int(cd.bucketCount); i++ {
		if cd.buckets[intn] != nil {
			node := cd.buckets[intn].head
			for node != nil {
				keys = append(keys, node.entity.key)
				node = node.nextNode
			}
		}
		intn = (intn + 1) % int(cd.bucketCount)
	}
	if len(keys) < limit {
		return keys
	}
	return keys[:limit]
}

func (cd *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	intn := rand.Intn(int(cd.bucketCount))
	keys := make([]string, 0)
	for i := 0; i < int(cd.bucketCount); i++ {
		if cd.buckets[intn] != nil {
			node := cd.buckets[intn].head
			for node != nil {
				keys = append(keys, node.entity.key)
				node = node.nextNode
			}
		}
		intn = (intn + 1) % int(cd.bucketCount)
	}
	if len(keys) < limit {
		return keys
	}
	return keys[:limit]
}

func (cd *ConcurrentDict) Clear() {
	for i := 0; i < int(cd.bucketCount); i++ {
		cd.buckets[i] = nil
	}
}
