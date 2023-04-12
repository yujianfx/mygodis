package id

import (
	"testing"
)

func TestSnowflake_NextID(t *testing.T) {
	snowflake, err := NewSnowflake(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40960000; i++ {
		snowflake.NextID()
	}
}
