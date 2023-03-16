package sortedset

import (
	"fmt"
	"reflect"
	"testing"
)

var set = MakeZSet()

func loadDataSet() {
	for i := 0; i < 100; i++ {
		set.Add(fmt.Sprintf("elem%d", i), float64(i))

	}
}
func TestMakeZSet(t *testing.T) {
	tests := []struct {
		name string
		want *ZSet
	}{
		{
			name: "test make zset",
			want: &ZSet{
				dict: make(map[string]*Element),
				zsl:  makeSkipList(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MakeZSet(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MakeZSet() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestZSet_Add(t *testing.T) {
	type fields struct {
		dict map[string]*Element
		zsl  *zskiplist
	}
	type args struct {
		member string
		score  float64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "test add",
			fields: fields{
				dict: make(map[string]*Element),
				zsl:  makeSkipList(),
			},
			args: args{
				member: "test",
				score:  1,
			},
		},
		{
			name: "test add",
			fields: fields{
				dict: make(map[string]*Element),
				zsl:  makeSkipList(),
			},
			args: args{
				member: "test",
				score:  1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			zSet := &ZSet{
				dict: tt.fields.dict,
				zsl:  tt.fields.zsl,
			}
			zSet.Add(tt.args.member, tt.args.score)
		})
	}
}

func TestZSet_Count(t *testing.T) {
	set := MakeZSet()
	set.Add("a", 1)
	set.Add("b", 2)
	set.Add("c", 3)
	set.Add("d", 4)
	min := &ScoreBorder{
		Value:   -1,
		Exclude: true,
	}
	max := &ScoreBorder{
		Value:   4,
		Exclude: true,
	}
	fmt.Println(set.Count(min, max))
}

func TestZSet_ForEach(t *testing.T) {
	loadDataSet()
	set.ForEach(55, 66, false, func(element *Element) bool {
		fmt.Println(element)
		return true
	})
}

func TestZSet_ForeachByScore(t *testing.T) {
	loadDataSet()
	min := &ScoreBorder{
		Value:   55,
		Exclude: false,
	}
	max := &ScoreBorder{
		Value:   66,
		Exclude: false,
	}
	set.ForeachByScore(min, max, 5, 10, false, func(element *Element) bool {
		fmt.Println(element)
		return true
	})
}

func TestZSet_Get(t *testing.T) {
	loadDataSet()
	fmt.Println(set.Get("elem50"))
}

func TestZSet_Len(t *testing.T) {
	loadDataSet()
	fmt.Println(set.Len())
}

func TestZSet_PopMax(t *testing.T) {
	loadDataSet()
	fmt.Println(set.PopMax(10))
}

func TestZSet_PopMin(t *testing.T) {
	loadDataSet()
	fmt.Println(set.PopMin(10))
}

func TestZSet_Range(t *testing.T) {
	loadDataSet()
	fmt.Println(set.Range(0, 10, false))
}

func TestZSet_RangeByScore(t *testing.T) {
	loadDataSet()
	min := &ScoreBorder{
		Value:   55,
		Exclude: false,
	}
	max := &ScoreBorder{
		Value:   66,
		Exclude: false,
	}
	fmt.Println(set.RangeByScore(min, max, 5, 10, false))
}

func TestZSet_Remove(t *testing.T) {
	loadDataSet()
	fmt.Println(set.Remove("elem50"))
}

func TestZSet_RemoveByIndex(t *testing.T) {
	loadDataSet()
	fmt.Println(set.RemoveByIndex(50, 60))
	set.ForEach(45, 65, false, func(element *Element) bool {
		fmt.Println(element)
		return true
	})
}

func TestZSet_RemoveByScore(t *testing.T) {
	loadDataSet()
	min := &ScoreBorder{
		Value:   55,
		Exclude: false,
	}
	max := &ScoreBorder{
		Value:   66,
		Exclude: false,
	}
	fmt.Println(set.RemoveByScore(min, max))
	set.ForEach(45, 65, false, func(element *Element) bool {
		fmt.Println(element)
		return true
	})
}

func TestZSet_getIndex(t *testing.T) {
	loadDataSet()
	fmt.Println(set.getIndex("elem0", false))
}
