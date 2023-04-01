package delay

import (
	"fmt"
	"testing"
	"time"
)

func TestDelay(t *testing.T) {
	stop := make(chan struct{})
	tests := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		3 * time.Second,
		4 * time.Second,
		5 * time.Second,
		6 * time.Second,
		7 * time.Second,
		8 * time.Second,
		9 * time.Second,
		10 * time.Second,
	}
	for _, d := range tests {
		t.Run(d.String(), func(t *testing.T) {
			Delay(d, d.String(), func() {
				fmt.Println("delay:", d)
			})
		})
	}
	Delay(tests[len(tests)-1], tests[len(tests)-1].String(), func() {
		fmt.Println("delay:", tests[len(tests)-1])
		stop <- struct{}{}
	})
	select {
	case <-stop:
	}
}

func TestAt(t *testing.T) {
	execCount := 0
	stop := make(chan struct{})
	tests := []time.Duration{
		1 * time.Second,
		1 * time.Second,
		1 * time.Second,
		1 * time.Second,
		1 * time.Second,
		1 * time.Second,
		//1 * time.Second,
		//1 * time.Second,
		//9 * time.Second,
		//10 * time.Second,
	}
	for _, d := range tests {
		t.Run(d.String(), func(t *testing.T) {
			At(time.Now().Add(d), d.String(), func() {
				execCount++
			})
		})
	}
	At(time.Now().Add(tests[len(tests)-1]), tests[len(tests)-1].String(), func() {
		stop <- struct{}{}
	})
	select {
	case <-stop:
		fmt.Println("execCount:", execCount)
	}

}

func TestCancel(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test",
			args: args{
				key: "test",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			At(time.Now().Add(5*time.Second), tt.args.key, func() {
				println("at 5s")
			})
			Cancel(tt.args.key)
			time.Sleep(10 * time.Second)
		})
	}
}
