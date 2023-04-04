package db

import (
	"mygodis/common"
	"mygodis/resp"
	"reflect"
	"testing"
)

func Test_execHDel(t *testing.T) {
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
			if got := execHDel(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execHDel() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execHExists(t *testing.T) {
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
			if got := execHExists(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execHExists() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execHGet(t *testing.T) {
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
			if got := execHGet(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execHGet() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execHGetAll(t *testing.T) {
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
			if got := execHGetAll(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execHGetAll() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execHIncrBy(t *testing.T) {
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
			if got := execHIncrBy(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execHIncrBy() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execHIncrByFloat(t *testing.T) {
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
			if got := execHIncrByFloat(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execHIncrByFloat() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execHKeys(t *testing.T) {
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
			if got := execHKeys(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execHKeys() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execHLen(t *testing.T) {
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
			if got := execHLen(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execHLen() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execHMGet(t *testing.T) {
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
			if got := execHMGet(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execHMGet() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execHMSet(t *testing.T) {
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
			if got := execHMSet(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execHMSet() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execHSet(t *testing.T) {
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
			if got := execHSet(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execHSet() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execHSetNx(t *testing.T) {
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
			if got := execHSetNx(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execHSetNx() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execHVals(t *testing.T) {
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
			if got := execHVals(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execHVals() = %v, want %v", got, tt.want)
			}
		})
	}
}
