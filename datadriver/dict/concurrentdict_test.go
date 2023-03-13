package dict

import (
	"fmt"
	"testing"
)

var testCount = 64
var dict = NewConcurrentDict(4)

func loadData() {
	for i := 0; i < testCount; i++ {
		dict.Put(fmt.Sprintf("key%d", i), i)
	}
}
func TestConcurrentDict_Put(t *testing.T) {
	loadData()
	for i := 0; i < testCount; i++ {
		dict.Put(fmt.Sprintf("%d", i), i)
	}
	fmt.Printf("rehash trigger count: %d, rehash finish count: %d\n", dict.reHashTriggerCount, dict.reHashFinishCount)
	fmt.Println(dict.size)
}
func TestConcurrentDict_Get(t *testing.T) {
	loadData()
	successCount := 0
	fmt.Printf("rehash trigger count: %d, rehash finish count: %d\n", dict.reHashTriggerCount, dict.reHashFinishCount)
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
func TestConcurrentDict_Remove(t *testing.T) {
	loadData()
	successCount := 0
	t.Run("Remove", func(t *testing.T) {
		for i := 0; i < testCount; i++ {
			if v, _ := dict.Remove(fmt.Sprintf("%d", i)); v != nil {
				successCount++
			}
		}
	})
	dict.Put("cao", "cao")
	dict.Remove("cao")
	val, _ := dict.Get("cao")
	fmt.Println("get", val)
	successCount = 0
	t.Run("Get", func(t *testing.T) {
		for i := 0; i < testCount; i++ {
			if val, exists := dict.Get(fmt.Sprintf("%d", i)); exists && val == i {
				successCount++
			}
		}
	})
	if successCount == testCount {
		t.Errorf("Get failed, expected %d,but got %d", testCount, successCount)
	}
}
func TestConcurrentDict_Keys(t *testing.T) {
	loadData()
	keys := dict.Keys()
	if len(keys) != testCount || len(keys) != dict.Len() {
		t.Errorf("Keys failed, expected %d,but got %d", testCount, len(keys))
	}
}
func TestConcurrentDict_RandomKeys(t *testing.T) {
	loadData()

	keys := dict.RandomKeys(10)
	if len(keys) != 10 {
		t.Errorf("RandomKeys failed, expected %d,but got %d", 10, len(keys))
	}
}
