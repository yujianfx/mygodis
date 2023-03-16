package sortedset

import "strconv"

type ZSet struct {
	dict map[string]*Element
	zsl  *zskiplist
}

func MakeZSet() *ZSet {
	return &ZSet{
		dict: make(map[string]*Element),
		zsl:  makeSkipList(),
	}
}
func (zSet *ZSet) Add(member string, score float64) {
	element, ok := zSet.dict[member]
	zSet.dict[member] = &Element{
		Member: member,
		Score:  score,
	}
	if ok {
		if score != element.Score {
			zSet.zsl.delete(element.Score, member)
			zSet.zsl.insert(score, member)
		}
		return
	}
	zSet.zsl.insert(score, member)
}
func (zSet *ZSet) Len() int64 {
	return int64(len(zSet.dict))
}
func (zSet *ZSet) Get(member string) (element *Element, ok bool) {
	element, ok = zSet.dict[member]
	if !ok {
		return nil, false
	}
	return element, true
}
func (zSet *ZSet) Remove(member string) bool {
	v, ok := zSet.dict[member]
	if ok {
		zSet.zsl.delete(v.Score, member)
		delete(zSet.dict, member)
		return true
	}
	return false
}
func (zSet *ZSet) getIndex(member string, desc bool) int64 {
	elem, ok := zSet.dict[member]
	if !ok {
		return -1
	}
	index := zSet.zsl.getIndex(member, elem.Score)
	if desc {
		index = zSet.zsl.length - index
	}
	return index
}
func (zSet *ZSet) ForEach(start int64, stop int64, desc bool, consumer func(element *Element) bool) {
	size := zSet.Len()
	if start < 0 || start >= size {
		panic("start index out of range [0, size) but got " + strconv.FormatInt(start, 10))
	}
	if stop < 0 || stop > size {
		panic("stop index out of range [0, size] but got " + strconv.FormatInt(stop, 10))
	}
	if start > stop {
		panic("start index must less than stop index but got start " + strconv.FormatInt(start, 10) + " and stop " + strconv.FormatInt(stop, 10))
	}
	var zNode *zskiplistNode
	if desc {
		zNode = zSet.zsl.tail
		if start > 0 {
			zNode = zSet.zsl.getByIndex(zSet.zsl.length - start)
		}
	} else {
		zNode = zSet.zsl.header.level[0].forward
		if start > 0 {
			zNode = zSet.zsl.getByIndex(start + 1)
		}
	}
	for i := start; i < stop; i++ {
		if !consumer(&Element{
			Member: zNode.elem.Member,
			Score:  zNode.elem.Score,
		}) {
			break
		}
		if desc {
			zNode = zNode.backward
		} else {
			zNode = zNode.level[0].forward
		}
	}
}
func (zSet *ZSet) Range(start, end int64, desc bool) (result []*Element) {
	if start > end {
		return nil
	}
	length := end - start
	result = make([]*Element, 0, length)
	zSet.ForEach(start, end, desc, func(element *Element) bool {
		result = append(result, element)
		return true
	})
	return result
}
func (zSet *ZSet) Count(min, max *ScoreBorder) int64 {
	if min == nil || max == nil || min.Value > max.Value {
		return 0
	}
	count := int64(0)
	minNode := zSet.zsl.getFirstInScoreRange(min, max)
	maxNode := zSet.zsl.getLastInScoreRange(min, max)
	if minNode == nil || maxNode == nil {
		return 0
	}
	for node := minNode; node != nil && node != maxNode; node = node.level[0].forward {
		count++
	}
	return count
}
func (zSet *ZSet) ForeachByScore(min, max *ScoreBorder, offset, limit int64, desc bool, consumer func(element *Element) bool) {
	minNode := zSet.zsl.getFirstInScoreRange(min, max)
	maxNode := zSet.zsl.getLastInScoreRange(min, max)
	count := int64(0)
	if minNode == nil || maxNode == nil {
		return
	}
	if desc {
		minNode, maxNode = maxNode, minNode
	}
	for minNode != nil && offset > 0 {
		if desc {
			minNode = minNode.backward
		} else {
			minNode = minNode.level[0].forward
		}
		offset--
	}
	for minNode != nil && minNode != maxNode && count < limit {
		if !consumer(&Element{
			Member: minNode.elem.Member,
			Score:  minNode.elem.Score,
		}) {
			break
		}
		count++
		if desc {
			minNode = minNode.backward
		} else {
			minNode = minNode.level[0].forward
		}
	}
}
func (zSet *ZSet) RangeByScore(min, max *ScoreBorder, offset, limit int64, desc bool) (result []Element) {
	if min.Value > max.Value {
		return nil
	}
	result = make([]Element, 0, limit)
	zSet.ForeachByScore(min, max, offset, limit, desc, func(element *Element) bool {
		result = append(result, *element)
		return true
	})
	return result
}
func (zSet *ZSet) RemoveByScore(min, max *ScoreBorder) int64 {
	result := zSet.zsl.reMoveRangeByScore(min, max, 0)
	for _, v := range result {
		delete(zSet.dict, v.Member)
	}
	return int64(len(result))

}
func (zSet *ZSet) PopMin(count int64) []*Element {
	if count <= 0 {
		return nil
	}
	result := make([]*Element, 0, count)
	elements := zSet.zsl.removeRangeByIndex(0, count)
	for _, v := range elements {
		delete(zSet.dict, v.Member)
		result = append(result, v)
	}
	return result
}
func (zSet *ZSet) PopMax(count int64) []*Element {
	if count <= 0 {
		return nil
	}
	result := make([]*Element, 0, count)
	elements := zSet.zsl.removeRangeByIndex(int64(int(zSet.Len()-count)), int64(int(zSet.Len())))
	for _, v := range elements {
		delete(zSet.dict, v.Member)
		result = append(result, v)
	}
	return result
}
func (zSet *ZSet) RemoveByIndex(start, stop int64) int64 {
	if start < 0 || stop < 0 {
		return 0
	}
	if start > stop {
		return 0
	}
	result := zSet.zsl.removeRangeByIndex(start, stop)
	for _, v := range result {
		delete(zSet.dict, v.Member)
	}
	return int64(len(result))
}
