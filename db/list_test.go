package db

import (
	"mygodis/common"
	"mygodis/datadriver/list"
	"mygodis/resp"
	"reflect"
	"testing"
)

func Test_execLIndex(t *testing.T) {
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
			if got := execLIndex(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execLIndex() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execLLen(t *testing.T) {
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
			if got := execLLen(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execLLen() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execLPop(t *testing.T) {
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
			if got := execLPop(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execLPop() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execLPush(t *testing.T) {
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
			if got := execLPush(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execLPush() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execLPushX(t *testing.T) {
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
			if got := execLPushX(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execLPushX() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execLRange(t *testing.T) {
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
			if got := execLRange(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execLRange() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execLRem(t *testing.T) {
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
			if got := execLRem(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execLRem() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execLSet(t *testing.T) {
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
			if got := execLSet(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execLSet() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execLTrim(t *testing.T) {
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
			if got := execLTrim(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execLTrim() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execRPop(t *testing.T) {
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
			if got := execRPop(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execRPop() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execRPopLPush(t *testing.T) {
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
			if got := execRPopLPush(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execRPopLPush() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execRPush(t *testing.T) {
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
			if got := execRPush(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execRPush() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execRPushX(t *testing.T) {
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
			if got := execRPushX(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execRPushX() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getOrCreateList(t *testing.T) {
	type args struct {
		d   *DataBaseImpl
		key string
	}
	tests := []struct {
		name          string
		args          args
		wantResult    list.List
		wantIsCreated bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotResult, gotIsCreated := getOrCreateList(tt.args.d, tt.args.key)
			if !reflect.DeepEqual(gotResult, tt.wantResult) {
				t.Errorf("getOrCreateList() gotResult = %v, want %v", gotResult, tt.wantResult)
			}
			if gotIsCreated != tt.wantIsCreated {
				t.Errorf("getOrCreateList() gotIsCreated = %v, want %v", gotIsCreated, tt.wantIsCreated)
			}
		})
	}
}
