package list

import (
	"fmt"
	"reflect"
	"testing"
)

func listWithData(list *LinkedList, datas ...any) *LinkedList {
	for _, data := range datas {
		list.Add(data)
	}
	return list
}
func TestLinkedList_Add(t *testing.T) {
	list := NewLikedList()
	list.Add(1)
	list.Add(2)
	list.Add(3)
	list.Add("a")
	list.Add("b")
	list.Add("c")
	list.ForEach(func(i int, val any) bool {
		t.Log(val)
		return true
	})
}

func TestLinkedList_Contains(t *testing.T) {
	type args struct {
		expected Expected
	}
	tests := []struct {
		list *LinkedList
		name string
		args args
		want bool
	}{
		{
			list: listWithData(NewLikedList(), 1, 2, 3, "a", "b", "c"),
			name: "contains",
			args: args{
				expected: func(a any) bool {
					return a == "a"
				},
			},
			want: true,
		},
		{
			list: listWithData(NewLikedList(), 1, 2, 3, "a", "b", "c"),
			name: "not contains",
			args: args{
				expected: func(a any) bool {
					return a == "d"
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.list.Contains(tt.args.expected); got != tt.want {
				t.Errorf("Contains() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLinkedList_ForEach(t *testing.T) {
	type args struct {
		action Consumer
	}
	tests := []struct {
		list *LinkedList
		name string
		args args
	}{
		{
			list: listWithData(NewLikedList(), 1, 2, 3, "a", "b", "c"),
			name: "for each",
			args: args{
				action: func(i int, val any) bool {
					t.Log(val)
					return true
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.list.ForEach(tt.args.action)
		})
	}
}

func TestLinkedList_Get(t *testing.T) {

	type args struct {
		index int
	}
	tests := []struct {
		name   string
		fields *LinkedList
		args   args
		want   any
	}{
		{
			name:   "get",
			fields: listWithData(NewLikedList(), 1, 2, 3, "a", "b", "c"),
			args: args{
				index: 3,
			},
			want: "a",
		},
		{
			name:   "get without data",
			fields: NewLikedList(),
			args: args{
				index: 3,
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.fields.Get(tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLinkedList_Insert(t *testing.T) {

	type args struct {
		index int
		val   any
	}
	tests := []struct {
		name       string
		linkedList *LinkedList
		args       args
	}{
		{
			name:       "insert middle",
			linkedList: listWithData(NewLikedList(), 1, 2, 3, "a", "b", "c"),
			args: args{
				index: 3,
				val:   "d",
			},
		},
		{
			name:       "insert first",
			linkedList: listWithData(NewLikedList(), 1, 2, 3, "a", "b", "c"),
			args: args{
				index: 0,
				val:   "d",
			},
		},
		{
			name:       "insert last",
			linkedList: listWithData(NewLikedList(), 1, 2, 3, "a", "b", "c"),
			args: args{
				index: 5,
				val:   "d",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.linkedList.Insert(tt.args.index, tt.args.val)
			tt.linkedList.ForEach(func(i int, val any) bool {
				t.Log(val)
				return true
			})
		})
	}
}

func TestLinkedList_Len(t *testing.T) {
	tests := []struct {
		name   string
		fields *LinkedList
		want   int
	}{
		{
			name:   "len",
			fields: listWithData(NewLikedList(), 1, 2, 3, "a", "b", "c"),
			want:   6,
		},
		{
			name:   "len without data",
			fields: NewLikedList(),
			want:   0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.fields.Len(); got != tt.want {
				t.Errorf("Len() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLinkedList_Range(t *testing.T) {
	type args struct {
		start int
		stop  int
	}
	tests := []struct {
		name   string
		fields *LinkedList
		args   args
		want   []any
	}{
		{
			name:   "range",
			fields: listWithData(NewLikedList(), 1, 2, 3, "a", "b", "c"),
			args: args{
				start: 2,
				stop:  4,
			},
			want: []any{3, "a"},
		},
		{
			name:   "range without data",
			fields: NewLikedList(),
			args: args{
				start: 2,
				stop:  4,
			},
			want: []any{},
		},
		{
			name:   "range with start > stop",
			fields: listWithData(NewLikedList(), 1, 2, 3, "a", "b", "c"),
			args: args{
				start: 4,
				stop:  2,
			},
			want: []any{},
		},
		{
			name:   "range with start < 0",
			fields: listWithData(NewLikedList(), 1, 2, 3, "a", "b", "c"),
			args: args{
				start: -1,
				stop:  2,
			},
			want: []any{1, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.fields.Range(tt.args.start, tt.args.stop); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Range() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLinkedList_Remove(t *testing.T) {
	type args struct {
		index int
	}
	tests := []struct {
		name       string
		linkedList *LinkedList
		args       args
	}{
		{
			name:       "remove",
			linkedList: listWithData(NewLikedList(), 1, 2, 3, "a", "b", "c"),
			args: args{
				index: 3,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.linkedList.Remove(tt.args.index)
			tt.linkedList.ForEach(func(i int, val any) bool {
				t.Log(val)
				return true
			})
		})
	}
}

func TestLinkedList_Set(t *testing.T) {
	type args struct {
		index int
		val   any
	}
	tests := []struct {
		name       string
		linkedList *LinkedList
		args       args
	}{
		{
			name:       "set",
			linkedList: listWithData(NewLikedList(), 1, 2, 3, "a", "b", "c"),
			args: args{
				index: 3,
				val:   "d",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.linkedList.Set(tt.args.index, tt.args.val)
			tt.linkedList.ForEach(func(i int, val any) bool {
				t.Log(val)
				return true
			})
		})
	}
}

func TestLinkedList_RemoveBatch(t *testing.T) {
	t.Run("remove", func(t *testing.T) {
		list := NewLikedList()
		list.Add("a")
		list.Add("b")
		list.Add("c")
		list.Add("d")
		list.Add("e")
		batch := list.RemoveBatch(1, 3)
		fmt.Println(batch)
	})
	t.Run("removeAll", func(t *testing.T) {
		list := NewLikedList()
		list.Add("a")
		list.Add("b")
		list.Add("c")
		list.Add("d")
		list.Add("e")
		batch := list.RemoveBatch(0, list.Len()-1)
		fmt.Println(batch)
	})

}
