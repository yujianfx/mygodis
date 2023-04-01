package delay

import (
	"testing"
	"time"
)

func FuzzTimeWheel_getPositionAndCircle(f *testing.F) {
	f.Add(int64(995757456748547))
	f.Fuzz(func(t *testing.T, delay int64) {
		tw := NewTimeWheel(time.Second, 8)
		tk := &task{
			delay: time.Duration(delay),
		}
		p, c := tw.getPositionAndCircle(tk)
		if p < 0 || p > 7 {
			t.Errorf("position out of range: %d", p)
		}
		if c < 0 {
			t.Errorf("circle out of range: %d", c)
		}
	})

}
