package db

import (
	"mygodis/common"
	"mygodis/resp"
	"reflect"
	"testing"
)

func Test_execZAdd(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name string
		args args
		want resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execZAdd(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execZAdd() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execZCard(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name string
		args args
		want resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execZCard(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execZCard() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execZCount(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name string
		args args
		want resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execZCount(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execZCount() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execZDiff(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name string
		args args
		want resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execZDiff(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execZDiff() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execZDiffStore(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name string
		args args
		want resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execZDiffStore(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execZDiffStore() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execZIncrBy(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name string
		args args
		want resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execZIncrBy(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execZIncrBy() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execZInter(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name       string
		args       args
		wantResult resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotResult := execZInter(tt.args.db, tt.args.args); !reflect.DeepEqual(gotResult, tt.wantResult) {
				t.Errorf("execZInter() = %v, want %v", gotResult, tt.wantResult)
			}
		})
	}
}

func Test_execZInterCard(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name       string
		args       args
		wantResult resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotResult := execZInterCard(tt.args.db, tt.args.args); !reflect.DeepEqual(gotResult, tt.wantResult) {
				t.Errorf("execZInterCard() = %v, want %v", gotResult, tt.wantResult)
			}
		})
	}
}

func Test_execZInterStore(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name string
		args args
		want resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execZInterStore(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execZInterStore() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execZLexCount(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name string
		args args
		want resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execZLexCount(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execZLexCount() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execZRange(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name string
		args args
		want resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execZRange(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execZRange() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execZRangeStore(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name string
		args args
		want resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execZRangeStore(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execZRangeStore() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execZRank(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name string
		args args
		want resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execZRank(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execZRank() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execZRem(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name string
		args args
		want resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execZRem(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execZRem() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execZRevRank(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name string
		args args
		want resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execZRevRank(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execZRevRank() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execZScore(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name string
		args args
		want resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execZScore(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execZScore() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execZUnion(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name       string
		args       args
		wantResult resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotResult := execZUnion(tt.args.db, tt.args.args); !reflect.DeepEqual(gotResult, tt.wantResult) {
				t.Errorf("execZUnion() = %v, want %v", gotResult, tt.wantResult)
			}
		})
	}
}

func Test_execZUnionStore(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name       string
		args       args
		wantResult resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotResult := execZUnionStore(tt.args.db, tt.args.args); !reflect.DeepEqual(gotResult, tt.wantResult) {
				t.Errorf("execZUnionStore() = %v, want %v", gotResult, tt.wantResult)
			}
		})
	}
}

func Test_getZsetMember(t *testing.T) {
	type args struct {
		args common.CmdLine
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getZsetMember(tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getZsetMember() = %v, want %v", got, tt.want)
			}
		})
	}
}
