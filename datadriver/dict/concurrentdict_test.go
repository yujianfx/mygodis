package dict

import (
	"fmt"
	"testing"
)

func TestEntity_SetKey(t *testing.T) {

	t.Run("TestEntity_SetKey", func(t *testing.T) {
		concurrentDict := NewConcurrentDict(16384)
		concurrentDict.Put("key", "value")
		concurrentDict.Put("key1", "value1")
		concurrentDict.Put("key2", "value2")
		val, _ := concurrentDict.Get("key2")
		val1, _ := concurrentDict.Get("key1")
		val2, _ := concurrentDict.Get("key")
		fmt.Println(val)
		fmt.Println(val1)
		fmt.Println(val2)
	})

}
