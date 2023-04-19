package dict

import (
	"fmt"
	"sync"
	"testing"
)

var testCount = 409600
var dict = NewConcurrentDict()

func loadData() {
	for i := 0; i < testCount; i++ {
		dict.Put(fmt.Sprintf("key%d", i), i)
	}
}
func TestConcurrentDict_Get2(t *testing.T) {
	concurrentDict := NewConcurrentDict()
	concurrentDict.Put("a", 666)
	concurrentDict.Put("b", 777)
	concurrentDict.Put("c", 888)
	concurrentDict.Put("a", 999)
	concurrentDict.ForEach(func(key string, value interface{}) bool {
		fmt.Println(key, value)
		return true
	})

}
func TestConcurrentDict_Put(t *testing.T) {
	loadData()
	for i := 0; i < testCount; i++ {
		dict.Put(fmt.Sprintf("%d", i), i)
	}
}
func TestConcurrentDict_Get(t *testing.T) {
	loadData()
	successCount := 0
	t.Run("Get", func(t *testing.T) {
		for i := 0; i < testCount; i++ {
			val, exists := dict.Get(fmt.Sprintf("key%d", i))
			if exists && val == i {
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
func TestConcurrentDict_GOPut(t *testing.T) {
	m := make(map[string]interface{})
	for i := 0; i < testCount; i++ {
		m[fmt.Sprintf("%d", i)] = i
	}
}
func FuzzConcurrentDict_Put(f *testing.F) {
	dict := NewConcurrentDict()
	f.Add([]byte("key1"), []byte("value1"))
	f.Add([]byte("key2"), []byte("value2"))
	f.Add([]byte("key3"), []byte("value3"))
	f.Fuzz(func(t *testing.T, key, value []byte) {
		dict.Put(string(key), string(value))
		if val, exists := dict.Get(string(key)); !exists || val != string(value) {
			t.Errorf("key: %s, value: %s", string(key), string(value))
			t.Errorf("get failed, expected %s,but got %s", string(value), val)
		}
	})
}
func BenchmarkConcurrentDict_Put(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dict.Put(fmt.Sprintf("%d", i), i)
	}
}
func TestConcurrentSafety(t *testing.T) {
	dict := NewConcurrentDict()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dict.Put("key", "value")
			dict.Get("key")
			dict.Remove("key")
		}()
	}

	// 等待所有 goroutine 完成
	wg.Wait()
}
func BenchmarkConcurrentSafety(b *testing.B) {
	dict := NewConcurrentDict()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				dict.Put("key", "value")
				dict.Get("key")
				dict.Remove("key")
			}()
		}
		wg.Wait()
	}
}
