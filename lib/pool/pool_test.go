package pool

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestPool_Get(t *testing.T) {
	pool := NewPool(func() (any, error) {
		return rand.Int31n(16384), nil
	}, func(a any) {
		fmt.Printf("finalizer called%v", a)
	},
		Config{
			MaxActive: 8,
			MaxIdle:   4,
		})

	t.Run("get", func(t *testing.T) {
		for i := 0; i < 16; i++ {
			go func() {
				for i := 0; i < 8; i++ {
					x, err := pool.Get()
					if err != nil {
						t.Error(err)
						return
					}
					fmt.Println(x)
					pool.Put(x)
				}
			}()
		}

	})
}
