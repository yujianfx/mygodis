package cluster

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
)

func TestConsistentHash_GetNode(t *testing.T) {
	ch := MakeConsistentHash()
	ch.AddNode("node0")
	ch.AddNode("node1")
	ch.AddNode("node2")
	t.Log("哈希环的结构是：")
	for _, node := range ch.Nodes {
		fmt.Printf("%s->", ch.ChMap[node])
	}
	fmt.Println()
	for _, node := range ch.Nodes {
		fmt.Printf("%d->\n", node)
	}
	fmt.Println()
	for i := 0; i < 8; i++ {
		key := "key" + strconv.Itoa(int(rand.Int31n(16384)))
		position := ch.getPosition([]byte(key))
		node := ch.GetNode([]byte(key))
		fmt.Printf("key %s ->%d,落在节点%s上\n", key, position, node)
	}
	for i := 16384; i < 16390; i++ {
		key := "key" + strconv.Itoa(int(rand.Int31n(16384)))
		position := ch.getPosition([]byte(key))
		node := ch.GetNode([]byte(key))
		fmt.Printf("key %s ->%d,落在节点%s上\n", key, position, node)
	}

}

func TestLoadFrom(t *testing.T) {
	ch := MakeConsistentHash()
	ch.AddNode("node0")
	ch.AddNode("node1")
	ch.AddNode("node2")
	t.Log("哈希环的结构是：")
	for _, node := range ch.Nodes {
		fmt.Printf("%s->", ch.ChMap[node])
	}
	fmt.Println()
	for _, node := range ch.Nodes {
		fmt.Printf("%d->\n", node)
	}
	fmt.Println()
	serialize, err := ch.Serialize()
	if err != nil {
		t.Error(err)
	}
	ch2, err := LoadFrom(serialize)
	if err != nil {
		t.Error(err)
	}
	t.Log("哈希环的结构是：")
	for _, node := range ch2.Nodes {
		fmt.Printf("%s->", ch2.ChMap[node])
	}
	fmt.Println()
	for _, node := range ch2.Nodes {
		fmt.Printf("%d->\n", node)
	}
	fmt.Println()

	ch2.AddNode("node3")
	t.Log("哈希环的结构是：")
	for _, node := range ch2.Nodes {
		fmt.Printf("%s->", ch2.ChMap[node])
	}
	fmt.Println()
	for _, node := range ch2.Nodes {
		fmt.Printf("%d->\n", node)
	}
	fmt.Println()

}
