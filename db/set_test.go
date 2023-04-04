package db

import (
	"mygodis/common"
	"mygodis/resp"
	"reflect"
	"testing"
)

func Test_execSAdd(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name      string
		args      args
		wantReply resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotReply := execSAdd(tt.args.db, tt.args.args); !reflect.DeepEqual(gotReply, tt.wantReply) {
				t.Errorf("execSAdd() = %v, want %v", gotReply, tt.wantReply)
			}
		})
	}
}

func Test_execSCard(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name      string
		args      args
		wantReply resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotReply := execSCard(tt.args.db, tt.args.args); !reflect.DeepEqual(gotReply, tt.wantReply) {
				t.Errorf("execSCard() = %v, want %v", gotReply, tt.wantReply)
			}
		})
	}
}

func Test_execSDiff(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name      string
		args      args
		wantReply resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotReply := execSDiff(tt.args.db, tt.args.args); !reflect.DeepEqual(gotReply, tt.wantReply) {
				t.Errorf("execSDiff() = %v, want %v", gotReply, tt.wantReply)
			}
		})
	}
}

func Test_execSDiffStore(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name      string
		args      args
		wantReply resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotReply := execSDiffStore(tt.args.db, tt.args.args); !reflect.DeepEqual(gotReply, tt.wantReply) {
				t.Errorf("execSDiffStore() = %v, want %v", gotReply, tt.wantReply)
			}
		})
	}
}

func Test_execSInter(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name      string
		args      args
		wantReply resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotReply := execSInter(tt.args.db, tt.args.args); !reflect.DeepEqual(gotReply, tt.wantReply) {
				t.Errorf("execSInter() = %v, want %v", gotReply, tt.wantReply)
			}
		})
	}
}

func Test_execSInterStore(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name      string
		args      args
		wantReply resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotReply := execSInterStore(tt.args.db, tt.args.args); !reflect.DeepEqual(gotReply, tt.wantReply) {
				t.Errorf("execSInterStore() = %v, want %v", gotReply, tt.wantReply)
			}
		})
	}
}

func Test_execSIsMember(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name      string
		args      args
		wantReply resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotReply := execSIsMember(tt.args.db, tt.args.args); !reflect.DeepEqual(gotReply, tt.wantReply) {
				t.Errorf("execSIsMember() = %v, want %v", gotReply, tt.wantReply)
			}
		})
	}
}

func Test_execSMembers(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name      string
		args      args
		wantReply resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotReply := execSMembers(tt.args.db, tt.args.args); !reflect.DeepEqual(gotReply, tt.wantReply) {
				t.Errorf("execSMembers() = %v, want %v", gotReply, tt.wantReply)
			}
		})
	}
}

func Test_execSMove(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name      string
		args      args
		wantReply resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotReply := execSMove(tt.args.db, tt.args.args); !reflect.DeepEqual(gotReply, tt.wantReply) {
				t.Errorf("execSMove() = %v, want %v", gotReply, tt.wantReply)
			}
		})
	}
}

func Test_execSPop(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name      string
		args      args
		wantReply resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotReply := execSPop(tt.args.db, tt.args.args); !reflect.DeepEqual(gotReply, tt.wantReply) {
				t.Errorf("execSPop() = %v, want %v", gotReply, tt.wantReply)
			}
		})
	}
}

func Test_execSRandMember(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name      string
		args      args
		wantReply resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotReply := execSRandMember(tt.args.db, tt.args.args); !reflect.DeepEqual(gotReply, tt.wantReply) {
				t.Errorf("execSRandMember() = %v, want %v", gotReply, tt.wantReply)
			}
		})
	}
}

func Test_execSRem(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name      string
		args      args
		wantReply resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotReply := execSRem(tt.args.db, tt.args.args); !reflect.DeepEqual(gotReply, tt.wantReply) {
				t.Errorf("execSRem() = %v, want %v", gotReply, tt.wantReply)
			}
		})
	}
}

func Test_execSUnion(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name      string
		args      args
		wantReply resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotReply := execSUnion(tt.args.db, tt.args.args); !reflect.DeepEqual(gotReply, tt.wantReply) {
				t.Errorf("execSUnion() = %v, want %v", gotReply, tt.wantReply)
			}
		})
	}
}

func Test_execSUnionStore(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name      string
		args      args
		wantReply resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotReply := execSUnionStore(tt.args.db, tt.args.args); !reflect.DeepEqual(gotReply, tt.wantReply) {
				t.Errorf("execSUnionStore() = %v, want %v", gotReply, tt.wantReply)
			}
		})
	}
}
