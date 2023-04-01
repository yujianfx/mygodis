package delay

import "time"

var timeWheel = NewTimeWheel(50*time.Millisecond, 8)

func init() {
	timeWheel.Start()
}

func Delay(duration time.Duration, key string, job func()) {
	timeWheel.AddTask(duration, key, job)
}

func At(at time.Time, key string, job func()) {
	timeWheel.AddTask(at.Sub(time.Now()), key, job)
}
func Cancel(key string) {
	timeWheel.RemoveTask(key)
}
