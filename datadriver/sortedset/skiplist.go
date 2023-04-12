package sortedset

import (
	"math/rand"
)

const (
	zslMaxLevel = 32
	zslP        = 0.25
)

type Element struct {
	Member string
	Score  float64
}
type zskiplistLevel struct {
	forward *zskiplistNode
	span    uint32
}
type zskiplistNode struct {
	elem     *Element
	backward *zskiplistNode
	level    []zskiplistLevel
}
type zskiplist struct {
	header *zskiplistNode
	tail   *zskiplistNode
	length int64
	level  int16
}

func (z *zskiplist) hasInRange(min, max *ScoreBorder) bool {
	if min == nil && max == nil || (min.Value == max.Value && min.Exclude == max.Exclude) || (z.tail == nil || min.greater(z.tail.elem.Score)) || (z.header == nil || max.less(z.header.elem.Score)) {
		return false
	}
	return true
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
		elem:  &Element{Member: ele.(string), Score: score},
		level: make([]zskiplistLevel, level),
	}
	return node
}
func (z *zskiplist) insert(score float64, ele string) *zskiplistNode {
	currentNode := z.header
	update := make([]*zskiplistNode, zslMaxLevel)
	rank := make([]uint32, zslMaxLevel)
	for i := z.level - 1; i >= 0; i-- {
		if i == z.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for currentNode.level[i].forward != nil && (currentNode.level[i].forward.elem.Score < score || (currentNode.level[i].forward.elem.Score == score && currentNode.level[i].forward.elem.Member < ele)) {
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
			update[i].level[i].span = uint32(z.length)
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
func (z *zskiplist) find(score float64, elem string) *zskiplistNode {
	x := z.header
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.elem.Score < score ||
				(x.level[i].forward.elem.Score == score && x.level[i].forward.elem.Member < elem)) {
			x = x.level[i].forward
		}
	}
	x = x.level[0].forward
	if x != nil && x.elem.Score == score && x.elem.Member == elem {
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
			(x.level[i].forward.elem.Score < min ||
				(x.level[i].forward.elem.Score == min && x.level[i].forward.elem.Score <= max)) {
			x = x.level[i].forward
		}
	}
	x = x.level[0].forward
	for x != nil && x.elem.Score <= max {
		nodes = append(nodes, x)
		x = x.level[0].forward
	}
	return nodes
}
func (z *zskiplist) delete(score float64, ele string) bool {
	x := z.header
	update := make([]*zskiplistNode, zslMaxLevel)
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.elem.Score < score ||
				(x.level[i].forward.elem.Score == score && x.level[i].forward.elem.Member < ele)) {
			x = x.level[i].forward
		}
		update[i] = x
	}
	x = x.level[0].forward
	if x != nil && x.elem.Score == score && x.elem.Member == ele {
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
			(x.level[i].forward.elem.Score < min ||
				(x.level[i].forward.elem.Score == min && x.level[i].forward.elem.Score <= max)) {
			x = x.level[i].forward
		}
		update[i] = x
	}
	x = x.level[0].forward
	for x != nil && x.elem.Score <= max {
		z.deleteNode(x, update)
		x = x.level[0].forward
	}
}
func (z *zskiplist) len() int {
	return int(z.length)
}
func (z *zskiplist) getIndex(elem string, score float64) int64 {
	x := z.header
	var rank int64
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.elem.Score < score ||
				(x.level[i].forward.elem.Score == score && x.level[i].forward.elem.Member < elem)) {
			rank += int64(x.level[i].span)
			x = x.level[i].forward
		}
	}
	x = x.level[0].forward
	if x != nil && x.elem.Member == elem {
		return rank
	} else {
		return -1
	}
}
func (z *zskiplist) getByIndex(index int64) *zskiplistNode {
	x := z.header
	var rank int64
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(rank+int64(x.level[i].span)) <= index {
			rank += int64(x.level[i].span)
			x = x.level[i].forward
		}
		if rank == index {
			return x
		}
	}
	return nil
}
func (z *zskiplist) removeRangeByIndex(start, end int64) (result []*Element) {
	if start < 0 || end < 0 || start > end {
		return
	}
	result = make([]*Element, 0, end-start+1)
	x := z.header
	var rank int64
	update := make([]*zskiplistNode, zslMaxLevel)
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(rank+int64(x.level[i].span)) <= start {
			rank += int64(x.level[i].span)
			x = x.level[i].forward
		}
		update[i] = x
	}
	x = x.level[0].forward
	for x != nil && rank <= end {
		result = append(result, x.elem)
		z.deleteNode(x, update)
		x = x.level[0].forward
		rank++
	}
	return
}
func (z *zskiplist) getNodeIndexByScore(score float64) *zskiplistNode {
	x := z.header
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.elem.Score < score ||
				(x.level[i].forward.elem.Score == score && x.level[i].forward.elem.Member < "")) {
			x = x.level[i].forward
		}
	}
	return x
}

func (z *zskiplist) reMoveRangeByBorder(min, max *ScoreBorder, limit int64) (result []*Element) {
	if !z.hasInRange(min, max) {
		return
	}

	var traversed int64
	node := z.getNodeIndexByScore(min.Value)

	result = make([]*Element, 0)
	for node != nil && !max.less(node.elem.Score) && traversed < limit {
		next := node.level[0].forward
		result = append(result, node.elem)
		z.delete(node.elem.Score, node.elem.Member)
		node = next
		traversed++
	}
	return
}

func (z *zskiplist) forEachInScoreBorder(min, max *ScoreBorder, offset, limit int64, desc bool, consumer func(element *Element) bool) {
	if !z.hasInRange(min, max) {
		return
	}

	var traversed int64
	node := z.getNodeIndexByScore(min.Value)

	for node != nil && !max.less(node.elem.Score) {
		if traversed >= offset && traversed < (offset+limit) {
			if !consumer(node.elem) {
				return
			}
		}
		traversed++

		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
	}
}

func (z *zskiplist) count(min *ScoreBorder, max *ScoreBorder) int64 {
	if !z.hasInRange(min, max) {
		return 0
	}

	var count int64
	node := z.getNodeIndexByScore(min.Value)

	for node != nil && !max.less(node.elem.Score) {
		count++
		node = node.level[0].forward
	}
	return count
}
