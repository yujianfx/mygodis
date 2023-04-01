package set

import "mygodis/datadriver/dict"

type Set struct {
	dict dict.ConcurrentDict
}

func Make(members ...string) *Set {
	set := &Set{dict: dict.NewSimpleDict(4)}
	for _, member := range members {
		set.Add(member)
	}
	return set
}
func (s *Set) Add(elem string) int {
	return s.dict.Put(elem, nil)
}
func (s *Set) Remove(elem string) int {
	_, ret := s.dict.Remove(elem)
	return ret
}
func (s *Set) Has(elem string) bool {
	_, exists := s.dict.Get(elem)
	return exists
}
func (s *Set) Len() int {
	return s.dict.Len()
}
func (s *Set) ToSlice() []string {
	slice := make([]string, s.Len())
	i := 0
	s.dict.ForEach(func(key string, val interface{}) bool {
		if i < len(slice) {
			slice[i] = key
		} else {
			slice = append(slice, key)
		}
		i++
		return true
	})
	return slice
}
func (s *Set) ForEach(consumer func(elem string) bool) {
	s.dict.ForEach(func(key string, val interface{}) bool {
		return consumer(key)
	})
}
func (s *Set) InsertSet(another *Set) {
	another.ForEach(func(elem string) bool {
		s.Add(elem)
		return true
	})
}
func (s *Set) RemoveSet(another *Set) {
	another.ForEach(func(elem string) bool {
		s.Remove(elem)
		return true
	})
}
func (s *Set) IsSubset(another *Set) bool {
	if s.Len() > another.Len() {
		return false
	}
	ret := true
	s.ForEach(func(elem string) bool {
		if !another.Has(elem) {
			ret = false
			return false
		}
		return true
	})
	return ret
}
func (s *Set) Union(another *Set) *Set {
	union := Make()
	s.ForEach(func(elem string) bool {
		union.Add(elem)
		return true
	})
	another.ForEach(func(elem string) bool {
		union.Add(elem)
		return true
	})
	return union
}
func (s *Set) Diff(another *Set) *Set {
	diff := Make()
	s.ForEach(func(elem string) bool {
		if !another.Has(elem) {
			diff.Add(elem)
		}
		return true
	})
	return diff
}
func (s *Set) Inter(another *Set) *Set {
	inter := Make()
	s.ForEach(func(elem string) bool {
		if another.Has(elem) {
			inter.Add(elem)
		}
		return true
	})
	return inter
}
func (s *Set) RandomMembers(limit int) []string {
	return s.dict.RandomKeys(limit)
}
