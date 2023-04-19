package wait

import (
	"fmt"
	"testing"
	"time"
)

func TestWait_WaitWithTimeout(t *testing.T) {
	wait := MakeWait()
	wait.Add(1)
	go func() {

		for i := 0; i < 10; i++ {
			fmt.Println(i)
			time.Sleep(time.Second)
		}
		wait.Done()
	}()
	ok := wait.WaitWithTimeout(5 * time.Second)
	if !ok {
		t.Log("timeout")
	}
}

func TestWait_DoWithTimeOut(t *testing.T) {
	wait := MakeWait()
	ok1 := wait.DoWithTimeOut(func() {
		for i := 0; i < 5; i++ {
			fmt.Println(i)
			time.Sleep(time.Second)
		}
	}, time.Second*10)
	if !ok1 {
		t.Log("timeout")
	}
}
