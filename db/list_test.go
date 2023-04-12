package db

import (
	"fmt"
	"mygodis/common"
	"mygodis/common/commoninterface"
	"mygodis/datadriver/list"
	"mygodis/resp"
	"reflect"
	"testing"
)

func dbWithListData(db *DataBaseImpl, key string, strs ...string) *DataBaseImpl {
	resultList, created := getOrCreateList(db, key)
	for i := range strs {
		val := strs[i]
		resultList.Add([]byte(val))
	}
	if created {
		data := new(commoninterface.DataEntity)
		data.Data = resultList
		db.PutEntity(key, data)
	}
	return db
}
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
		{
			name: "lindex",
			args: args{
				db: dbWithListData(NewDB(), "list", "a", "b", "c"),
				args: common.CmdLine{
					[]byte("list"),
					[]byte("1"),
				},
			},
			want: resp.MakeBulkReply([]byte("b")),
		},
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
		{
			name: "llen",
			args: args{
				db: dbWithListData(NewDB(), "list", "a", "b", "c"),
				args: common.CmdLine{
					[]byte("list"),
				},
			},
			want: resp.MakeIntReply(3),
		},
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
		{
			name: "lpop",
			args: args{
				db: dbWithListData(NewDB(), "list", "a", "b", "c"),
				args: common.CmdLine{
					[]byte("list"),
				},
			},
			want: resp.MakeBulkReply([]byte("a")),
		},
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
		{
			name: "lpush",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("list"),
					[]byte("a"),
					[]byte("b"),
					[]byte("c"),
				},
			},
			want: resp.MakeIntReply(3),
		},
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
		{
			name: "lpushx",
			args: args{
				db: dbWithListData(NewDB(), "list", "a", "b", "c"),
				args: common.CmdLine{
					[]byte("list"),
					[]byte("d"),
				},
			},
			want: resp.MakeIntReply(4),
		},
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
		{
			name: "lrange",
			args: args{
				db: dbWithListData(NewDB(), "list", "a", "b", "c"),
				args: common.CmdLine{
					[]byte("list"),
					[]byte("0"),
					[]byte("1"),
				},
			},
			want: resp.MakeMultiBulkReply([][]byte{
				[]byte("a"),
				[]byte("b"),
			}),
		},
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
		{
			name: "lrem",
			args: args{
				db: dbWithListData(NewDB(), "list", "a", "b", "a", "c", "a"),
				args: common.CmdLine{
					[]byte("list"),
					[]byte("2"),
					[]byte("a"),
				},
			},
			want: resp.MakeIntReply(2),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execLRem(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execLRem() = %v, want %v", got, tt.want)
			}
			asList, _ := tt.args.db.getAsList("list")
			asList.(list.List).ForEach(func(i int, v interface{}) bool {
				anies := v.([]byte)
				t.Log(string(anies))
				return true
			})

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
		{
			name: "lset",
			args: args{
				db: dbWithListData(NewDB(), "list", "a", "b", "c"),
				args: common.CmdLine{
					[]byte("list"),
					[]byte("1"),
					[]byte("d"),
				},
			},
			want: resp.MakeOkReply(),
		},
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
		{
			name: "ltrim all",
			args: args{
				db: dbWithListData(NewDB(), "list", "a", "b", "c"),
				args: common.CmdLine{
					[]byte("list"),
					[]byte("0"),
					[]byte("-1"),
				},
			},
			want: resp.MakeOkReply(),
		},
		{
			name: "ltrim left",
			args: args{
				db: dbWithListData(NewDB(), "list", "a", "b", "c"),
				args: common.CmdLine{
					[]byte("list"),
					[]byte("1"),
					[]byte("-1"),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execLTrim(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execLTrim() = %v, want %v", got, tt.want)
			}
			asList, _ := tt.args.db.getAsList("list")
			asList.ForEach(func(i int, v interface{}) bool {
				anies := v.([]byte)
				t.Log(string(anies))
				return true
			})
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
		{
			name: "rpop",
			args: args{
				db: dbWithListData(NewDB(), "list", "a", "b", "c"),
				args: common.CmdLine{
					[]byte("list"),
				},
			},
			want: resp.MakeBulkReply([]byte("c")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execRPop(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execRPop() = %v, want %v", got, tt.want)
			}
			asList, _ := tt.args.db.getAsList("list")
			asList.ForEach(func(i int, v any) bool {
				fmt.Println("index", i, "value", string(v.([]byte)))
				return true
			})
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
		{
			name: "rpoplpush",
			args: args{
				db: dbWithListData(NewDB(), "list", "a", "b", "c"),
				args: common.CmdLine{
					[]byte("list"),
					[]byte("list2"),
				},
			},
			want: resp.MakeBulkReply([]byte("c")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execRPopLPush(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execRPopLPush() = %v, want %v", got, tt.want)
			}
			asList, _ := tt.args.db.getAsList("list")
			asList.ForEach(func(i int, v any) bool {
				fmt.Println("list1 index", i, "value", string(v.([]byte)))
				return true
			})
			asList2, _ := tt.args.db.getAsList("list2")
			asList2.ForEach(func(i int, v any) bool {
				fmt.Println("list2 index", i, "value", string(v.([]byte)))
				return true
			})
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
		{
			name: "rpush",
			args: args{
				db: dbWithListData(NewDB(), "list", "1", "2", "3"),
				args: common.CmdLine{
					[]byte("list"),
					[]byte("a"),
					[]byte("b"),
					[]byte("c"),
				},
			},
			want: resp.MakeIntReply(6),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execRPush(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execRPush() = %v, want %v", got, tt.want)
			}
			asList, _ := tt.args.db.getAsList("list")
			asList.ForEach(func(i int, v any) bool {
				fmt.Println("index", i, "value", string(v.([]byte)))
				return true
			})
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
		{
			name: "rpushx",
			args: args{
				db: dbWithListData(NewDB(), "list", "1", "2", "3"),
				args: common.CmdLine{
					[]byte("list"),
					[]byte("a"),
				},
			},
			want: resp.MakeIntReply(4),
		},
		{
			name: "rpushx not exist",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("list"),
					[]byte("a"),
				},
			},
			want: resp.MakeIntReply(0),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execRPushX(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execRPushX() = %v, want %v", got, tt.want)
			}
		})
	}
}
