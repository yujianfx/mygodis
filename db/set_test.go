package db

import (
	"fmt"
	"mygodis/common"
	"mygodis/common/commoninterface"
	"mygodis/resp"
	"reflect"
	"testing"
)

func dbWithSetData(impl *DataBaseImpl, key string, members ...string) *DataBaseImpl {
	set, isNew := impl.getOrCreateSet(key)
	for _, member := range members {
		set.Add(member)
	}
	if isNew {
		data := new(commoninterface.DataEntity)
		data.Data = set
		impl.PutEntity(key, data)
	}
	return impl
}
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
		{
			name: "sadd to empty set",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("a"),
					[]byte("1"),
				},
			},
			wantReply: resp.MakeIntReply(2),
		},
		{
			name: "sadd to non-empty set",
			args: args{
				db: dbWithSetData(NewDB(), "key", "a", "b", "c"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("a"),
					[]byte("1"),
				},
			},
			wantReply: resp.MakeIntReply(1),
		},
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
		{
			name: "scard empty set",
			args: args{
				db: dbWithSetData(NewDB(), "key"),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			wantReply: resp.MakeIntReply(0),
		},
		{
			name: "scard non-empty set",
			args: args{
				db: dbWithSetData(NewDB(), "key", "a", "b", "c"),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			wantReply: resp.MakeIntReply(3),
		},
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
		{
			name: "sdiff a part of set",
			args: args{
				db: dbWithSetData(dbWithSetData(NewDB(), "key1", "a", "b", "c"), "key2", "a", "b"),
				args: common.CmdLine{
					[]byte("key1"),
					[]byte("key2"),
				},
			},
			wantReply: resp.MakeMultiBulkReply([][]byte{
				[]byte("c"),
			}),
		},
		{
			name: "sdiff all of set",
			args: args{
				db: dbWithSetData(dbWithSetData(NewDB(), "key1", "a", "b", "c"), "key2", "a", "b", "c"),
				args: common.CmdLine{
					[]byte("key1"),
					[]byte("key2"),
				},
			},
			wantReply: resp.MakeMultiBulkReply([][]byte{}),
		},
		{
			name: "sdiff cover all of set",
			args: args{
				db: dbWithSetData(dbWithSetData(NewDB(), "key1", "a", "b", "c"), "key2", "a", "b", "c", "d", "e"),
				args: common.CmdLine{
					[]byte("key1"),
					[]byte("key2"),
				},
			},
			wantReply: resp.MakeMultiBulkReply([][]byte{}),
		},
		{
			name: "sdiff multi a part of set",
			args: args{
				db: dbWithSetData(dbWithSetData(dbWithSetData(NewDB(), "key1", "a", "b", "c", "d"), "key2", "a", "b"), "key3", "c"),
				args: common.CmdLine{
					[]byte("key1"),
					[]byte("key2"),
					[]byte("key3"),
				},
			},
			wantReply: resp.MakeMultiBulkReply([][]byte{
				[]byte("d"),
			}),
		},
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
		{
			name: "sdiffstore a part of set",
			args: args{
				db: dbWithSetData(dbWithSetData(NewDB(), "key1", "a", "b", "c"), "key2", "a", "b"),
				args: common.CmdLine{
					[]byte("dest"),
					[]byte("key1"),
					[]byte("key2"),
				},
			},
			wantReply: resp.MakeIntReply(1),
		},
		{
			name: "sdifftore all of set",
			args: args{
				db: dbWithSetData(dbWithSetData(NewDB(), "key1", "a", "b", "c"), "key2", "a", "b", "c"),
				args: common.CmdLine{
					[]byte("dest"),
					[]byte("key1"),
					[]byte("key2"),
				},
			},
			wantReply: resp.MakeIntReply(0),
		},
		{
			name: "sdiffstore cover all of set",
			args: args{
				db: dbWithSetData(dbWithSetData(NewDB(), "key1", "a", "b", "c"), "key2", "a", "b", "c", "d", "e"),
				args: common.CmdLine{
					[]byte("dest"),
					[]byte("key1"),
					[]byte("key2"),
				},
			},
			wantReply: resp.MakeIntReply(0),
		},
		{
			name: "sdiffstore multi a part of set",
			args: args{
				db: dbWithSetData(dbWithSetData(dbWithSetData(NewDB(), "key1", "a", "b", "c", "d"), "key2", "a", "b"), "key3", "c"),
				args: common.CmdLine{
					[]byte("dest"),
					[]byte("key1"),
					[]byte("key2"),
					[]byte("key3"),
				},
			},
			wantReply: resp.MakeIntReply(1),
		},
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
		{
			name: "sinter a part of set",
			args: args{
				db: dbWithSetData(dbWithSetData(NewDB(), "key1", "a", "b", "c"), "key2", "a", "b"),
				args: common.CmdLine{
					[]byte("key1"),
					[]byte("key2"),
				},
			},
			wantReply: resp.MakeMultiBulkReply([][]byte{
				[]byte("a"),
				[]byte("b"),
			}),
		},
		{
			name: "sinter all of set",
			args: args{
				db: dbWithSetData(dbWithSetData(NewDB(), "key1", "a", "b", "c"), "key2", "a", "b", "c"),
				args: common.CmdLine{
					[]byte("key1"),
					[]byte("key2"),
				},
			},
			wantReply: resp.MakeMultiBulkReply([][]byte{
				[]byte("a"),
				[]byte("b"),
				[]byte("c"),
			}),
		},
		{
			name: "sinter cover all of set",
			args: args{
				db: dbWithSetData(dbWithSetData(NewDB(), "key1", "a", "b", "c"), "key2", "a", "b", "c", "d", "e"),
				args: common.CmdLine{
					[]byte("key1"),
					[]byte("key2"),
				},
			},
			wantReply: resp.MakeMultiBulkReply([][]byte{
				[]byte("a"),
				[]byte("b"),
				[]byte("c"),
			}),
		},
		{
			name: "sinter multi a part of set",
			args: args{
				db: dbWithSetData(dbWithSetData(dbWithSetData(NewDB(), "key1", "a", "b", "c", "d"), "key2", "a", "b"), "key3", "a", "b", "c", "e", "z"),
				args: common.CmdLine{
					[]byte("key1"),
					[]byte("key2"),
					[]byte("key3"),
				},
			},
			wantReply: resp.MakeMultiBulkReply([][]byte{
				[]byte("a"),
				[]byte("b"),
			}),
		},
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
		{
			name: "sinter a part of set",
			args: args{
				db: dbWithSetData(dbWithSetData(NewDB(), "key1", "a", "b", "c"), "key2", "a", "b"),
				args: common.CmdLine{
					[]byte("dest"),
					[]byte("key1"),
					[]byte("key2"),
				},
			},
			wantReply: resp.MakeIntReply(2),
		},
		{
			name: "sinter all of set",
			args: args{
				db: dbWithSetData(dbWithSetData(NewDB(), "key1", "a", "b", "c"), "key2", "a", "b", "c"),
				args: common.CmdLine{
					[]byte("dest"),
					[]byte("key1"),
					[]byte("key2"),
				},
			},
			wantReply: resp.MakeIntReply(3),
		},
		{
			name: "sinter cover all of set",
			args: args{
				db: dbWithSetData(dbWithSetData(NewDB(), "key1", "a", "b", "c"), "key2", "a", "b", "c", "d", "e"),
				args: common.CmdLine{
					[]byte("dest"),
					[]byte("key1"),
					[]byte("key2"),
				},
			},
			wantReply: resp.MakeIntReply(3),
		},
		{
			name: "sinter multi a part of set",
			args: args{
				db: dbWithSetData(dbWithSetData(dbWithSetData(NewDB(), "key1", "a", "b", "c", "d"), "key2", "a", "b"), "key3", "a", "b", "c", "e", "z"),
				args: common.CmdLine{
					[]byte("dest"),
					[]byte("key1"),
					[]byte("key2"),
					[]byte("key3"),
				},
			},
			wantReply: resp.MakeIntReply(2),
		},
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
		{
			name: "sismember is a member of target set",
			args: args{
				db: dbWithSetData(NewDB(), "key", "a", "b", "c"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("a"),
				},
			},
			wantReply: resp.MakeIntReply(1),
		},
		{
			name: "sismember is not a member of target set",
			args: args{
				db: dbWithSetData(NewDB(), "key", "a", "b", "c"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("d"),
				},
			},
			wantReply: resp.MakeIntReply(0),
		},
		{
			name: "sismember target set not exist",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("a"),
				},
			},
			wantReply: resp.MakeIntReply(0),
		},
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
		{
			name: "set is empty",
			args: args{
				db:   NewDB(),
				args: common.CmdLine{[]byte("key")},
			},
			wantReply: resp.MakeMultiBulkReply([][]byte{}),
		},
		{
			name: "set is not empty",
			args: args{
				db: dbWithSetData(NewDB(), "key", "a", "b", "c"),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			wantReply: resp.MakeMultiBulkReply([][]byte{[]byte("a"), []byte("b"), []byte("c")}),
		},
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
		{
			name: "smove src set not exist",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("dest"),
					[]byte("key"),
					[]byte("a"),
				},
			},
			wantReply: resp.MakeIntReply(0),
		},
		{
			name: "smove dest set not exist",
			args: args{
				db: dbWithSetData(NewDB(), "key", "a", "b", "c"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("dest"),
					[]byte("a"),
				},
			},
			wantReply: resp.MakeIntReply(1),
		},
		{
			name: "smove src and dest set is not empty",
			args: args{
				db: dbWithSetData(NewDB(), "key", "a", "b", "c", "dest", "d", "e", "f"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("dest"),
					[]byte("a"),
				},
			},
			wantReply: resp.MakeIntReply(1),
		},
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
		{
			name: "spop set is empty",
			args: args{
				db:   NewDB(),
				args: common.CmdLine{[]byte("key")},
			},
			wantReply: resp.MakeNullBulkReply(),
		},
		{
			name: "spop set is not empty without count",
			args: args{
				db: dbWithSetData(NewDB(), "key", "a", "b", "c"),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			wantReply: resp.MakeBulkReply([]byte("a")),
		},
		{
			name: "spop set is not empty with count",
			args: args{
				db: dbWithSetData(NewDB(), "key", "a", "b", "c"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("2"),
				},
			},
			wantReply: resp.MakeMultiBulkReply([][]byte{[]byte("a"), []byte("c")}),
		},
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
	data := dbWithSetData(NewDB(), "key", "a", "b", "c")
	reply := execSRandMember(data, common.CmdLine{[]byte("key")})
	fmt.Println(reply)
	reply = execSRandMember(data, common.CmdLine{[]byte("key"), []byte("2")})
	fmt.Println(reply)
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
		{
			name: "srem set is empty",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("a"),
				},
			},
			wantReply: resp.MakeIntReply(0),
		},
		{
			name: "srem set is not empty",
			args: args{
				db: dbWithSetData(NewDB(), "key", "a", "b", "c"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("a"),
					[]byte("b"),
				},
			},
			wantReply: resp.MakeIntReply(2),
		},
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
		{
			name: "sunion set ",
			args: args{
				db: dbWithSetData(dbWithSetData(NewDB(), "key1", "a", "b", "c"), "key2", "a", "b", "d"),
				args: common.CmdLine{
					[]byte("key1"),
					[]byte("key2"),
				},
			},
			wantReply: resp.MakeMultiBulkReply([][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}),
		},
		{
			name: "sunion set multi",
			args: args{
				db: dbWithSetData(dbWithSetData(dbWithSetData(NewDB(), "key1", "a", "b", "c"), "key2", "a", "b", "d"), "key3", "a", "b", "e"),
				args: common.CmdLine{
					[]byte("key1"),
					[]byte("key2"),
					[]byte("key3"),
				},
			},
			wantReply: resp.MakeMultiBulkReply([][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotReply := execSUnion(tt.args.db, tt.args.args)
			fmt.Println(gotReply)

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
		{
			name: "sunionstore set ",
			args: args{
				db: dbWithSetData(dbWithSetData(NewDB(), "key1", "a", "b", "c"), "key2", "a", "b", "d"),
				args: common.CmdLine{
					[]byte("dest"),
					[]byte("key1"),
					[]byte("key2"),
				},
			},
			wantReply: resp.MakeIntReply(4),
		},
		{
			name: "sunionstore set multi",
			args: args{
				db: dbWithSetData(dbWithSetData(dbWithSetData(NewDB(), "key1", "a", "b", "c"), "key2", "a", "b", "d"), "key3", "a", "b", "e"),
				args: common.CmdLine{
					[]byte("dest"),
					[]byte("key1"),
					[]byte("key2"),
					[]byte("key3"),
				},
			},
			wantReply: resp.MakeIntReply(5),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotReply := execSUnionStore(tt.args.db, tt.args.args); !reflect.DeepEqual(gotReply, tt.wantReply) {
				t.Errorf("execSUnionStore() = %v, want %v", gotReply, tt.wantReply)
			}
		})
	}
}
