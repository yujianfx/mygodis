package list

import (
	"fmt"
	"testing"
)

func TestQuickList_Add(t *testing.T) {
	t.Run("test_add", func(t *testing.T) {
		ql := NewQuickList()
		for i := 0; i < 10; i++ {
			ql.Add(i)
		}
		ql.ForEach(func(i int, val any) bool {
			fmt.Printf("index: %d, val: %d\n", i, val.(int))
			return true
		})
	})

}

func TestQuickList_Get(t *testing.T) {
	ql := NewQuickList()
	for i := 0; i < 10; i++ {
		ql.Add(i)
	}
	t.Run("test_get", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			fmt.Printf("index: %d, val: %d\n", i, ql.Get(i).(int))
		}
	})
}

func TestQuickList_Set(t *testing.T) {
	ql := NewQuickList()
	for i := 0; i < 10; i++ {
		ql.Add(i)
	}
	t.Run("test_set", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			ql.Set(i, i+1)
		}
		ql.ForEach(func(i int, val any) bool {
			fmt.Printf("index: %d, val: %d\n", i, val.(int))
			return true
		})
	})
}

func TestQuickList_Insert(t *testing.T) {
	ql := NewQuickList()
	for i := 0; i < 10; i++ {
		ql.Add(i)
	}
	t.Run("test_insert", func(t *testing.T) {
		for ti := 0; ti < 10; ti++ {
			ql.Insert(ti, ti+10)
		}
		ql.ForEach(func(i int, val any) bool {
			fmt.Printf("index: %d, val: %d\n", i, val.(int))
			return true
		})
	})

}

func TestQuickList_Contains(t *testing.T) {
	ql := NewQuickList()
	for i := 0; i < 10; i++ {
		ql.Add(i)
	}
	t.Run("test_contains", func(t *testing.T) {
		fmt.Printf("contains 5: %v\n", ql.Contains(func(a any) bool {
			return a.(int) == 5
		}))
	})
	t.Run("test_contains", func(t *testing.T) {
		fmt.Printf("contains 15: %v\n", ql.Contains(func(a any) bool {
			return a.(int) == 15
		}))
	})
}
