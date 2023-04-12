package list

import "container/list"

const PAGE_SIZE = 16

// todo 有bug
type QuickList struct {
	data *list.List
	size int
}
type iterator struct {
	node   *list.Element
	offset int
	ql     *QuickList
}

func NewQuickList() *QuickList {
	l := &QuickList{
		data: list.New(),
		size: 0,
	}
	return l
}
func (ql *QuickList) find(index int) *iterator {
	if ql == nil {
		panic("list is nil")
	}
	if index < 0 || index >= ql.size {
		panic("index out of bound")
	}
	var n *list.Element
	var page []any
	var pageBeg int
	if index < ql.size/2 {
		n = ql.data.Front()
		pageBeg = 0
		for {
			page = n.Value.([]any)
			if index < pageBeg+len(page) {
				return &iterator{n, index - pageBeg, ql}
			}
			pageBeg += len(page)
			n = n.Next()
		}
	}
	n = ql.data.Back()
	pageBeg = ql.size
	for {
		page = n.Value.([]any)
		if index >= pageBeg-len(page) {
			return &iterator{n, index - pageBeg + len(page), ql}
		}
		pageBeg -= len(page)
		n = n.Prev()
	}
}

func (ql *QuickList) Add(val any) {
	ql.size++
	if ql.data.Len() == 0 {
		page := make([]any, 0, PAGE_SIZE)
		page = append(page, val)
		ql.data.PushBack(page)
		return
	}
	backNode := ql.data.Back()
	backPage := backNode.Value.([]any)
	if len(backPage) == cap(backPage) {
		page := make([]any, 0, PAGE_SIZE)
		page = append(page, val)
		ql.data.PushBack(page)
		return
	}
	backPage = append(backPage, val)
	backNode.Value = backPage
}
func (ql *QuickList) Get(index int) (val any) {
	itr := ql.find(index)
	return itr.node.Value.([]any)[itr.offset]
}

func (ql *QuickList) Set(index int, val any) {
	itr := ql.find(index)
	itr.node.Value.([]any)[itr.offset] = val
}

func (ql *QuickList) Insert(index int, val any) {
	itr := ql.find(index)
	page := itr.node.Value.([]any)
	if len(page) < cap(page) {
		newPage := make([]any, 0, PAGE_SIZE)
		newPage = append(newPage, page[itr.offset:]...)
		page = append(page[:itr.offset], val)
		page = append(page, newPage...)
		itr.node.Value = page
		ql.size++
		return
	}
	newPage := make([]any, 0, PAGE_SIZE)
	newPage = append(newPage, page[itr.offset:]...)
	page = append(page[:itr.offset], val)
	itr.node.Value = page
	ql.size++
	ql.data.InsertAfter(newPage, itr.node)

}

func (ql *QuickList) Remove(index int) (val any) {
	if ql == nil {
		panic("list is nil")
	}
	if index < 0 || index >= ql.size {
		panic("index out of bound")
	}
	var n *list.Element
	var page []any
	var pageBeg int
	if index < ql.size/2 {
		n = ql.data.Front()
		pageBeg = 0
		for {
			page = n.Value.([]any)
			if index < pageBeg+len(page) {
				val = page[index-pageBeg]
				copy(page[index-pageBeg:], page[index-pageBeg+1:])
				page = page[:len(page)-1]
				ql.size--
				n.Value = page
				if len(page) == 0 {
					ql.data.Remove(n)
				}
				return
			}
			pageBeg += len(page)
			n = n.Next()
		}
	}
	n = ql.data.Back()
	pageBeg = ql.size
	for {
		page = n.Value.([]any)
		if index >= pageBeg-len(page) {
			val = page[index-pageBeg+len(page)]
			copy(page[index-pageBeg+len(page):], page[index-pageBeg+len(page)+1:])
			page = page[:len(page)-1]
			ql.size--
			n.Value = page
			if len(page) == 0 {
				ql.data.Remove(n)
			}
			return
		}
		pageBeg -= len(page)
		n = n.Prev()
	}
}

func (ql *QuickList) RemoveLast() (val any) {
	if ql == nil {
		panic("list is nil")
	}
	if ql.size == 0 {
		panic("list is empty")
	}
	ql.size--
	n := ql.data.Back()
	page := n.Value.([]any)
	val = page[len(page)-1]
	page = page[:len(page)-1]
	n.Value = page
	if len(page) == 0 {
		ql.data.Remove(n)
	}
	return
}
func (itr *iterator) hasNext() bool {
	page := itr.node.Value.([]any)
	if itr.offset < len(page)-1 {
		itr.offset++ // next element in page
		return true

	}
	if itr.node == itr.ql.data.Back() {
		itr.offset = len(page)
		return false
	}
	itr.node = itr.node.Next() // next page
	itr.offset = 0
	return true
}
func (itr *iterator) hasPrev() bool {
	if itr.offset > 0 {
		itr.offset--
		return true
	}
	if itr.node == itr.ql.data.Front() {
		itr.offset = -1
		return false
	}
	itr.node = itr.node.Prev()
	itr.offset = len(itr.node.Value.([]any)) - 1
	return true
}
func (ql *QuickList) RemoveAllByVal(expected Expected) int {
	find := ql.find(0)
	count := 0
	for find.hasNext() {
		if expected(find.node.Value.([]any)[find.offset]) {
			ql.Remove(find.offset)
			count++
		}
	}
	return count
}

func (ql *QuickList) RemoveByVal(expected Expected, count int) int {
	find := ql.find(0)
	count = 0
	for find.hasNext() {
		if expected(find.node.Value.([]any)[find.offset]) {
			ql.Remove(find.offset)
			count++
			if count == count {
				return count
			}
		}
	}
	return count
}

func (ql *QuickList) ReverseRemoveByVal(expected Expected, count int) int {
	find := ql.find(ql.size - 1)
	count = 0
	for find.hasPrev() {
		if expected(find.node.Value.([]any)[find.offset]) {
			ql.Remove(find.offset)
			count++
			if count == count {
				return count
			}
		}
	}
	return count
}

func (ql *QuickList) Len() int {
	return ql.size
}

func (ql *QuickList) ForEach(consumer Consumer) {
	if ql == nil {
		return
	}
	itr := ql.find(0)
	index := 0
	for itr.hasNext() {
		if !consumer(index, itr.node.Value.([]any)[itr.offset]) {
			break
		}
		index++
	}
}

func (ql *QuickList) Contains(expected Expected) bool {
	if ql == nil {
		return false
	}
	itr := ql.find(0)
	for itr.hasNext() {
		if expected(itr.node.Value.([]any)[itr.offset]) {
			return true
		}
	}
	return false
}

func (ql *QuickList) Range(start int, stop int) []any {
	if ql == nil {
		return nil
	}
	if start < 0 {
		start = 0
	}
	if stop > ql.size {
		stop = ql.size
	}
	if start >= stop {
		return nil
	}
	itr := ql.find(start)
	result := make([]any, 0, stop-start)
	for itr.hasNext() && start < stop {
		result = append(result, itr.node.Value.([]any)[itr.offset])
		start++
	}
	return result
}
