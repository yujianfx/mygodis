package sortedset

import (
	"fmt"
	"math/rand"
)

const (
	zslMaxLevel = 32
	zslP        = 0.25
)

type zskiplistLevel struct {
	forward *zskiplistNode
	span    uint32
}
type zskiplistNode struct {
	ele      any
	score    float64
	backward *zskiplistNode
	level    []zskiplistLevel
}
type zskiplist struct {
	header *zskiplistNode
	tail   *zskiplistNode
	length uint32
	level  int16
}

func makeSkipList() *zskiplist {
	return &zskiplist{
		level:  1,
		length: 0,
		header: createNode(zslMaxLevel, 0, ""),
	}

}
func createNode(level int16, score float64, ele any) *zskiplistNode {
	node := &zskiplistNode{
		ele:   ele,
		score: score,
		level: make([]zskiplistLevel, level),
	}
	return node
}
func (z *zskiplist) insert(score float64, ele any) *zskiplistNode {
	currentNode := z.header
	update := make([]*zskiplistNode, zslMaxLevel)
	rank := make([]uint32, zslMaxLevel)
	for i := z.level - 1; i >= 0; i-- {
		if i == z.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for currentNode.level[i].forward != nil && (currentNode.level[i].forward.score < score || (currentNode.level[i].forward.score == score && currentNode.level[i].forward.ele.(string) < ele.(string))) {
			rank[i] += currentNode.level[i].span
			currentNode = currentNode.level[i].forward
		}
		update[i] = currentNode
	}
	level := z.randomLevel()
	if level > z.level {
		for i := z.level; i < level; i++ {
			rank[i] = 0
			update[i] = z.header
			update[i].level[i].span = z.length
		}
		z.level = level
	}
	currentNode = createNode(level, score, ele)
	for i := int16(0); i < level; i++ {
		currentNode.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = currentNode
		currentNode.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = rank[0] - rank[i] + 1
	}
	for i := level; i < z.level; i++ {
		update[i].level[i].span++
	}
	if update[0] == z.header {
		currentNode.backward = nil
	} else {
		currentNode.backward = update[0]
	}
	if currentNode.level[0].forward != nil {
		currentNode.level[0].forward.backward = currentNode
	} else {
		z.tail = currentNode
	}
	z.length++
	return currentNode
}
func (z *zskiplist) randomLevel() int16 {
	level := uint8(1)
	for (rand.Float64() < zslP) && (level < zslMaxLevel) {
		level++
	}
	return int16(level)
}
func (z *zskiplist) deleteNode(x *zskiplistNode, update []*zskiplistNode) {
	for i := int16(0); i < z.level; i++ {
		if update[i].level[i].forward == x {
			update[i].level[i].span += x.level[i].span - 1
			update[i].level[i].forward = x.level[i].forward
		} else {
			update[i].level[i].span -= 1
		}
	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x.backward
	} else {
		z.tail = x.backward
	}
	for z.level > 1 && z.header.level[z.level-1].forward == nil {
		z.level--
	}
	z.length--
}
func (z *zskiplist) find(score float64, ele any) *zskiplistNode {
	x := z.header
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score && x.level[i].forward.ele.(string) < ele.(string))) {
			x = x.level[i].forward
		}
	}
	x = x.level[0].forward
	if x != nil && x.score == score && x.ele.(string) == ele.(string) {
		return x
	} else {
		return nil
	}
}
func (z *zskiplist) rangeByScore(min, max float64) []*zskiplistNode {
	x := z.header
	var nodes []*zskiplistNode
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < min ||
				(x.level[i].forward.score == min && x.level[i].forward.score <= max)) {
			x = x.level[i].forward
		}
	}
	x = x.level[0].forward
	for x != nil && x.score <= max {
		nodes = append(nodes, x)
		x = x.level[0].forward
	}
	return nodes
}
func (z *zskiplist) delete(score float64, ele any) bool {
	x := z.header
	update := make([]*zskiplistNode, zslMaxLevel)
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score && x.level[i].forward.ele.(string) < ele.(string))) {
			x = x.level[i].forward
		}
		update[i] = x
	}
	x = x.level[0].forward
	if x != nil && x.score == score && x.ele.(string) == ele.(string) {
		z.deleteNode(x, update)
		return true
	}
	return false
}
func (z *zskiplist) deleteRangeByScore(min, max float64) {
	x := z.header
	update := make([]*zskiplistNode, zslMaxLevel)
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < min ||
				(x.level[i].forward.score == min && x.level[i].forward.score <= max)) {
			x = x.level[i].forward
		}
		update[i] = x
	}
	x = x.level[0].forward
	for x != nil && x.score <= max {
		z.deleteNode(x, update)
		x = x.level[0].forward
	}
}
func (z *zskiplist) len() int {
	return int(z.length)
}
func (z *zskiplist) print() {
	x := z.header
	for x != nil {
		fmt.Printf("%v ", x.ele)
		x = x.level[0].forward
	}
	fmt.Println()
}
func (z *zskiplist) getIndex(elem string, score float64) int {
	x := z.header
	var rank int
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score && x.level[i].forward.ele.(string) < elem)) {
			rank += int(x.level[i].span)
			x = x.level[i].forward
		}
	}
	if x != nil && x.ele.(string) == elem {
		return rank
	} else {
		return -1
	}
}
func (z *zskiplist) getByIndex(index int) *zskiplistNode {
	x := z.header
	var rank int
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(rank+int(x.level[i].span)) <= index {
			rank += int(x.level[i].span)
			x = x.level[i].forward
		}
		if rank == index {
			return x
		}
	}
	return nil
}
func (z *zskiplist) hasInRange(min, max *ScoreBorder) bool {
	if min == nil && max == nil || (min.Value == max.Value && min.Exclude == max.Exclude) || (z.tail == nil || !min.less(z.tail.score)) || (z.header == nil || !max.greater(z.header.score)) {
		return false
	}
	return true
}
func (z *zskiplist) getFirstInScoreRange(min, max *ScoreBorder) *zskiplistNode {
	if !z.hasInRange(min, max) {
		return nil
	}
	x := z.header
	var rank int
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < min.Value ||
				(x.level[i].forward.score == min.Value && x.level[i].forward.score <= max.Value)) {
			rank += int(x.level[i].span)
			x = x.level[i].forward
		}
	}
	if x != nil && x.score <= max.Value {
		return x
	} else {
		return nil
	}
}
func (z *zskiplist) removeRangeByIndex(start, end int) (result zskiplistNode) {
	if start < 0 || end < 0 || start > end {
		return
	}
	x := z.header
	var rank int
	update := make([]*zskiplistNode, zslMaxLevel)
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(rank+int(x.level[i].span)) <= start {
			rank += int(x.level[i].span)
			x = x.level[i].forward
		}
		update[i] = x
	}
	x = x.level[0].forward
	for x != nil && rank <= end {
		z.deleteNode(x, update)
		x = x.level[0].forward
		rank++
	}
	return
}
