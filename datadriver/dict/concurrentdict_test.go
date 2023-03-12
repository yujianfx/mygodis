package dict

import (
	"fmt"
	"testing"
)

func Test_listBucket_put(t *testing.T) {
	testCount := 4096
	dict := NewConcurrentDict(4)
	successCount := 0
	for i := 0; i < testCount; i++ {
		if dict.Put(fmt.Sprintf("%d", i), i) > 0 {
			successCount++
		}
	}
	if successCount != testCount {
		t.Errorf("Put failed, expected %d,but got %d", testCount, successCount)

	}
}

func TestConcurrentDict_Get(t *testing.T) {
	testCount := 40960
	dict := NewConcurrentDict(4)
	for i := 0; i < testCount; i++ {
		dict.Put(fmt.Sprintf("%d", i), i)
	}
	successCount := 0
	t.Run("Get", func(t *testing.T) {
		for i := 0; i < testCount; i++ {
			if val, exists := dict.Get(fmt.Sprintf("%d", i)); exists && val == i {
				successCount++
			}
		}
	})
	if successCount != testCount {
		t.Errorf("Get failed, expected %d,but got %d", testCount, successCount)
	}
}
func TestConcurrentDict_GOGet(t *testing.T) {
	testCount := 40960
	m := make(map[string]interface{})
	for i := 0; i < testCount; i++ {
		m[fmt.Sprintf("%d", i)] = i
	}
	successCount := 0
	t.Run("Get", func(t *testing.T) {
		for i := 0; i < testCount; i++ {
			if val, exists := m[fmt.Sprintf("%d", i)]; exists && val == i {
				successCount++
			}
		}
	})
	if successCount != testCount {
		t.Errorf("Get failed, expected %d,but got %d", testCount, successCount)
	}
}
