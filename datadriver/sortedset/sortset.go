package sortedset

import (
	"sort"
	"strconv"
)

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

type zSetWithWeight struct {
	weight float64
	zSet   *ZSet
}

func (zSet *ZSet) Add(member string, score float64) bool {
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
		return true
	}
	return zSet.zsl.insert(score, member) != nil
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
	return zSet.zsl.count(min, max)
}
func (zSet *ZSet) ForeachByScore(min, max *ScoreBorder, offset, limit int64, desc bool, consumer func(element *Element) bool) {

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
	elements := zSet.zsl.reMoveRangeByBorder(min, max, zSet.Len())
	for _, v := range elements {
		delete(zSet.dict, v.Member)
	}
	return int64(len(elements))

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
func (zSet *ZSet) clone() *ZSet {
	result := MakeZSet()
	zSet.ForEach(0, zSet.Len(), false, func(element *Element) bool {
		result.Add(element.Member, element.Score)
		return true
	})
	return result
}
func (zSet *ZSet) Union(aggregate string, weight []float64, sets ...*ZSet) (result *ZSet) {
	if len(sets) == 0 {
		return zSet.clone()
	}
	zsww := make([]*zSetWithWeight, len(sets))
	for i, v := range sets {
		zsww[i] = &zSetWithWeight{
			weight: weight[i],
			zSet:   v,
		}
	}
	return unionSets(aggregate, zsww)
}
func (zSet *ZSet) Inter(aggregate string, weight []float64, sets ...*ZSet) (result *ZSet) {
	if len(sets) == 0 {
		return zSet.clone()
	}
	zsww := make([]*zSetWithWeight, len(sets))
	for i, v := range sets {
		zsww[i] = &zSetWithWeight{
			weight: weight[i],
			zSet:   v,
		}
	}
	return interSets(aggregate, zsww)
}
func (zSet *ZSet) Diff(sets ...*ZSet) (result *ZSet) {
	if len(sets) == 0 {
		return zSet.clone()
	}
	zsww := make([]*zSetWithWeight, len(sets))
	for i, v := range sets {
		zsww[i] = &zSetWithWeight{
			zSet: v,
		}
	}
	return diffSets(zsww)
}
func (zSet *ZSet) Rank(member string) (int64, bool) {
	if element, ok := zSet.Get(member); ok {
		return zSet.zsl.getIndex(member, element.Score), true
	}
	return -1, false
}
func (zSet *ZSet) LexCount(min string, max string) int64 {

	maxE, _ := zSet.Get(max)
	minE, _ := zSet.Get(min)
	if min[0] == '-' {
		minElem := zSet.zsl.getByIndex(zSet.Len() - 1)
		min = minElem.elem.Member
	}
	if max[0] == '+' {
		maxElem := zSet.zsl.getByIndex(0)
		max = maxElem.elem.Member
	}
	return zSet.zsl.getIndex(max, maxE.Score) - zSet.zsl.getIndex(min, minE.Score)
}
func diffSets(sets []*zSetWithWeight) (result *ZSet) {
	result = sets[0].zSet.clone()
	for i := 1; i < len(sets); i++ {
		setWithWeight := sets[i]
		setWithWeight.zSet.ForEach(0, setWithWeight.zSet.Len(), false, func(element *Element) bool {
			if _, ok := result.Get(element.Member); ok {
				result.Remove(element.Member)
			}
			return true
		})
	}
	return result
}
func unionSets(aggregate string, sets []*zSetWithWeight) (result *ZSet) {
	sort.Slice(sets, func(i, j int) bool {
		return sets[i].zSet.Len() < sets[j].zSet.Len()
	})
	result = sets[0].zSet.clone()
	for i := 1; i < len(sets); i++ {
		setWithWeight := sets[i]
		setWithWeight.zSet.ForEach(0, setWithWeight.zSet.Len(), false, func(element *Element) bool {
			if member, ok := result.Get(element.Member); ok {
				member.Score = destScore(member.Score, element.Score, setWithWeight.weight, aggregate)
			} else {
				result.Add(element.Member, element.Score*setWithWeight.weight)
			}
			return true
		})
	}
	return result
}
func interSets(aggregate string, sets []*zSetWithWeight) (result *ZSet) {
	result = MakeZSet()
	// 优先使用最小的集合
	sort.Slice(sets, func(i, j int) bool {
		return sets[i].zSet.Len() < sets[j].zSet.Len()
	})
	// 用最小的集合遍历
	sets[0].zSet.ForEach(0, sets[0].zSet.Len(), false, func(element *Element) bool {
		score := element.Score * sets[0].weight
		inResult := true
		for i := 1; i < len(sets); i++ {
			setWithWeight := sets[i]
			if member, ok := setWithWeight.zSet.Get(element.Member); ok {
				score = destScore(score, member.Score, setWithWeight.weight, aggregate)
			} else {
				inResult = false
				break
			}
		}
		if inResult {
			result.Add(element.Member, score)
		}
		return true
	})
	return result
}
func destScore(source, term, weight float64, aggregate string) float64 {
	return aggregateParse(aggregate)(source, term*weight)
}
func aggregateParse(AGGREGATE string) func(a, b float64) float64 {
	switch AGGREGATE {
	case "SUM":
		return func(a, b float64) float64 {
			return a + b
		}
	case "MIN":
		return func(a, b float64) float64 {
			if a < b {
				return a
			}
			return b
		}
	case "MAX":
		return func(a, b float64) float64 {
			if a > b {
				return a
			}
			return b
		}
	default:
		return func(a, b float64) float64 {
			return a + b
		}
	}
}
