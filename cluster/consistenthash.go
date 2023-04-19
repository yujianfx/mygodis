package cluster

import (
	"encoding/json"
	"hash/crc64"
	"sort"
	"strings"
)

const capacity = 1 << 14

type ChFunction func(key []byte) uint64
type ConsistentHash struct {
	ChFunc ChFunction `json:"-"`
	Nodes  []uint64
	ChMap  map[uint64]string
}

func MakeConsistentHash() *ConsistentHash {
	ch := &ConsistentHash{
		ChFunc: func(key []byte) uint64 {
			return crc64.Checksum(key, crc64.MakeTable(crc64.ECMA))
		},
		ChMap: make(map[uint64]string),
	}
	return ch
}

func (ch *ConsistentHash) AddNode(node string) {
	position := ch.getPosition([]byte(node))
	ch.Nodes = append(ch.Nodes, position)
	sort.Slice(ch.Nodes, func(i, j int) bool {
		return ch.Nodes[i] < ch.Nodes[j]
	})
	ch.ChMap[position] = node
}

func LoadFrom(chBytes []byte) (*ConsistentHash, error) {
	ch := &ConsistentHash{}
	err := json.Unmarshal(chBytes, &ch)
	ch.ChFunc = func(key []byte) uint64 {
		return crc64.Checksum(key, crc64.MakeTable(crc64.ECMA))
	}
	return ch, err
}

// Serialize this consistent hash
func (ch *ConsistentHash) Serialize() ([]byte, error) {
	chBytes, err := json.Marshal(ch)
	return chBytes, err
}

func (ch *ConsistentHash) getPartitionKey(key []byte) []byte {
	s := string(key)
	indexL := strings.Index(s, "{")
	indexR := strings.Index(s, "}")
	if indexL == -1 || indexR == -1 {
		return key
	}
	return []byte(s[indexL+1 : indexR])
}

func (ch *ConsistentHash) GetNode(key []byte) string {
	position := ch.getPosition(ch.getPartitionKey(key))
	search := sort.Search(len(ch.Nodes), func(i int) bool {
		return ch.Nodes[i] >= position
	})
	if search == len(ch.Nodes) {
		return ch.ChMap[ch.Nodes[0]]
	}
	return ch.ChMap[ch.Nodes[search]]
}

func (ch *ConsistentHash) find(position uint64) int {
	i := sort.Search(len(ch.Nodes), func(i int) bool {
		return ch.Nodes[i] == position
	})
	if i >= len(ch.Nodes) {
		return 0
	}
	return i
}

func (ch *ConsistentHash) nextNode(position uint64) (node string, code uint64) {
	find := ch.find(position)
	if find+1 == len(ch.Nodes) {
		return ch.ChMap[0], ch.Nodes[0]
	}
	next := ch.Nodes[find+1]
	return ch.ChMap[next], next
}

func (ch *ConsistentHash) preNode(position uint64) (node string, code uint64) {
	find := ch.find(position)
	if find == 0 {
		pre := ch.Nodes[len(ch.Nodes)-1]
		return ch.ChMap[pre], pre
	}
	pre := ch.Nodes[find-1]
	return ch.ChMap[pre], pre
}

func (ch *ConsistentHash) getPosition(key []byte) uint64 {
	hCode := ch.ChFunc(key)
	return (hCode + 1) % capacity
}
func (ch *ConsistentHash) GetNodes() []string {
	var nodes []string
	for _, v := range ch.ChMap {
		nodes = append(nodes, v)
	}
	return nodes
}
