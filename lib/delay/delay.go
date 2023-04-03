package delay

import "time"

var timeWheel = NewTimeWheel()

func init() {
	timeWheel.Start()
}

func Delay(duration time.Duration, key string, job func()) {
	timeWheel.Add(key, time.Now().Add(duration), job)
}

func At(at time.Time, key string, job func()) {
	timeWheel.Add(key, at, job)
}
func Cancel(key string) {
	timeWheel.Remove(key)
}
