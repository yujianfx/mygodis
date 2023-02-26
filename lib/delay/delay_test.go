package delay

import (
	"testing"
	"time"
)

func TestDelay(t *testing.T) {
	Delay(5*time.Second, "test", func() {
		println("delay 5s")
	},
	)

	time.Sleep(10 * time.Second)
}

func TestAt(t *testing.T) {
	type args struct {
		at  time.Time
		key string
		job func()
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test",
			args: args{
				at:  time.Now().Add(5 * time.Second),
				key: "test",
				job: func() {
					println("at 5s")
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			At(tt.args.at, tt.args.key, tt.args.job)
			time.Sleep(10 * time.Second)
		})
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
