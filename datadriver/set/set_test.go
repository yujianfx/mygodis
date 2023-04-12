package set

import (
	"testing"
)

func TestSet_RandomMembers(t *testing.T) {
	set := MakeSet("test", "test1", "test2")
	members := set.RandomMembers(2)
	for _, member := range members {
		t.Log(member)
	}

}
