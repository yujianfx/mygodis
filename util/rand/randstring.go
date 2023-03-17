package rand

import (
	"math/rand"
	"time"
)

var rander = rand.New(rand.NewSource(time.Now().UnixNano()))
var randSource = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
var hexRandSource = []rune("0123456789abcdef")

func RandString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = randSource[rander.Intn(len(randSource))]
	}
	return string(b)
}

func RandHexString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = hexRandSource[rander.Intn(len(hexRandSource))]
	}
	return string(b)
}
