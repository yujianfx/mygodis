package dict

import (
	"reflect"
	"testing"
)

func TestSimpleDict_RandomDistinctKeys(t *testing.T) {
	d := NewSimpleDict(8)
	d.PutIfAbsent("foo", 1)
	d.PutIfAbsent("bar", 2)
	d.PutIfAbsent("baz", 3)
	keys := d.RandomDistinctKeys(2)
	if len(keys) != 2 {
		t.Errorf("Expected length of random distinct keys to be 2, but got %d", len(keys))
	}
	if keys[0] == "foo" && keys[1] == "foo" {
		t.Errorf("Expected 'foo' to be in the random distinct keys, but it was not")
	}
	if keys[0] == "bar" && keys[1] == "bar" {
		t.Errorf("Expected 'bar' to be in the random distinct keys, but it was not")
	}
	if keys[0] == "baz" && keys[1] == "baz" {
		t.Errorf("Expected 'baz' to be in the random distinct keys, but it was not")
	}
}

func TestSimpleDict_RandomKeys(t *testing.T) {
	type fields struct {
		dict map[string]any
	}
	type args struct {
		limit int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &SimpleDict{
				dict: tt.fields.dict,
			}
			if got := d.RandomKeys(tt.args.limit); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RandomKeys() = %v, want %v", got, tt.want)
			}
		})
	}
}
