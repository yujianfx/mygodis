package rand

import (
	"fmt"
	"testing"
)

func TestRandString(t *testing.T) {
	for i := 0; i < 10; i++ {
		fmt.Println(RandString(10))
	}

}

func TestRandHexString(t *testing.T) {
	for i := 0; i < 10; i++ {
		fmt.Println(RandHexString(10))
	}
}
