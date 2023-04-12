package list

type LinkedList struct {
	first *node
	last  *node
	size  int
}

type node struct {
	val  any
	prev *node
	next *node
}

func (l *LinkedList) Add(val any) {
	if l == nil {
		l = NewLikedList()
	}
	n := &node{
		val: val,
	}
	if l.last == nil {
		l.first = n
		l.last = n
	} else {
		n.prev = l.last
		l.last.next = n
		l.last = n
	}
	l.size++
}
func NewLikedList() *LinkedList {
	return &LinkedList{}
}

func (l *LinkedList) find(index int) *node {
	if l == nil {
		return nil
	}
	if index < 0 || index >= l.size {
		return nil
	}
	if index < l.size/2 {
		n := l.first
		for i := 0; i < index; i++ {
			n = n.next
		}
		return n
	} else {
		n := l.last
		for i := l.size - 1; i > index; i-- {
			n = n.prev
		}
		return n
	}
}

func (l *LinkedList) Get(index int) (val any) {
	if l == nil {
		l = NewLikedList()
		return nil
	}
	if index < 0 || index >= l.size {
		return nil
	}
	return l.find(index).val
}

func (l *LinkedList) Set(index int, val any) {
	if l == nil {
		l = NewLikedList()
	}
	if index < 0 || index >= l.size {
		panic("index out of bound")
	}
	l.find(index).val = val
}

func (l *LinkedList) Insert(index int, val any) {
	if l == nil {
		l = NewLikedList()
	}
	if index < 0 || index >= l.size {
		panic("index out of bound")
	}
	n := &node{
		val: val,
	}
	if index == 0 {
		n.next = l.first
		l.first.prev = n
		l.first = n
	} else if index == l.size-1 {
		n.prev = l.last
		l.last.next = n
		l.last = n
	} else {
		prev := l.find(index - 1)
		next := prev.next
		prev.next = n
		n.prev = prev
		n.next = next
		next.prev = n
	}
	l.size++
}

func (l *LinkedList) Remove(index int) (val any) {
	if l == nil {
		l = NewLikedList()
	}
	if index < 0 || index >= l.size {
		return nil
	}
	n := l.find(index)
	if index == 0 {
		l.first = n.next
		if l.first != nil {
			l.first.prev = nil
		}
	} else if index == l.size-1 {
		l.last = n.prev
		if l.last != nil {
			l.last.next = nil
		}
	} else {
		prev := n.prev
		next := n.next
		prev.next = next
		next.prev = prev
	}
	l.size--
	return n.val
}

func (l *LinkedList) RemoveLast() (val any) {
	if l == nil {
		panic("list is nil")
	}
	if l.size == 0 {
		panic("list is empty")
	}
	return l.Remove(l.size - 1)
}

func (l *LinkedList) RemoveAllByVal(expected Expected) int {
	if l == nil {
		panic("list is nil")
	}
	if l.size == 0 {
		return 0
	}
	count := 0
	n := l.first
	for n != nil {
		if expected(n.val) {
			if n.prev == nil {
				l.first = n.next
				if l.first != nil {
					l.first.prev = nil
				}
			} else if n.next == nil {
				l.last = n.prev
				if l.last != nil {
					l.last.next = nil
				}
			} else {
				prev := n.prev
				next := n.next
				prev.next = next
				next.prev = prev
			}
			l.size--
			count++
		}
		n = n.next
	}
	return count
}

func (l *LinkedList) RemoveByVal(expected Expected, count int) int {
	if l == nil {
		panic("list is nil")
	}
	if l.size == 0 {
		return 0
	}
	if count <= 0 {
		return 0
	}
	c := 0
	n := l.first
	for n != nil {
		if expected(n.val) {
			if n.prev == nil {
				l.first = n.next
				if l.first != nil {
					l.first.prev = nil
				}
			} else if n.next == nil {
				l.last = n.prev
				if l.last != nil {
					l.last.next = nil
				}
			} else {
				prev := n.prev
				next := n.next
				prev.next = next
				next.prev = prev
			}
			l.size--
			c++
			if c == count {
				break
			}
		}
		n = n.next
	}
	return c
}

func (l *LinkedList) ReverseRemoveByVal(expected Expected, count int) int {
	if l == nil {
		panic("list is nil")
	}
	if l.size == 0 {
		return 0
	}
	if count <= 0 {
		return 0
	}
	c := 0
	n := l.last
	for n != nil {
		if expected(n.val) {
			if n.prev == nil {
				l.first = n.next
				if l.first != nil {
					l.first.prev = nil
				}
			} else if n.next == nil {
				l.last = n.prev
				if l.last != nil {
					l.last.next = nil
				}
			} else {
				prev := n.prev
				next := n.next
				prev.next = next
				next.prev = prev
			}
			l.size--
			c++
			if c == count {
				break
			}
		}
		n = n.prev
	}
	return c
}

func (l *LinkedList) Len() int {
	if l == nil {
		return 0
	}
	return l.size
}

func (l *LinkedList) ForEach(consumer Consumer) {
	if l == nil {
		return
	}
	n := l.first
	index := 0
	for n != nil {
		consumer(index, n.val)
		n = n.next
		index++
	}
}

func (l *LinkedList) Contains(expected Expected) bool {
	if l == nil {
		return false
	}
	n := l.first
	for n != nil {
		if expected(n.val) {
			return true
		}
		n = n.next
	}
	return false
}

func (l *LinkedList) Range(start int, stop int) []any {
	if l == nil {
		l = NewLikedList()
		return []any{}
	}
	if start < 0 {
		start = 0
	}
	if stop > l.size {
		stop = l.size
	}
	if start >= stop {
		return []any{}
	}
	r := make([]any, stop-start)
	n := l.find(start)
	for i := start; i < stop; i++ {
		r[i-start] = n.val
		n = n.next
	}
	return r
}
func (l *LinkedList) RemoveBatch(start int, stop int) []any {
	if l == nil {
		l = NewLikedList()
		return []any{}
	}
	if start < 0 {
		start = 0
	}
	if stop > l.size {
		stop = l.size
	}
	if start >= stop {
		return []any{}
	}
	r := make([]any, stop-start+1)
	n := l.find(start)
	for i := start; i <= stop; i++ {
		r[i-start] = n.val
		if n.prev == nil {
			l.first = n.next
			if l.first != nil {
				l.first.prev = nil
			}
		} else if n.next == nil {
			l.last = n.prev
			if l.last != nil {
				l.last.next = nil
			}
		} else {
			prev := n.prev
			next := n.next
			prev.next = next
			next.prev = prev
		}
		l.size--
		n = n.next
	}
	return r
}
