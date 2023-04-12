package db

import (
	"fmt"
	"mygodis/common"
	"mygodis/common/commoninterface"
	"mygodis/resp"
	"reflect"
	"testing"
)

func dbWithHData(impl *DataBaseImpl, key string, field string, value string) *DataBaseImpl {
	hash, isNew := impl.getOrCreateAsHash(key)
	hash.Put(field, value)
	if isNew {
		data := new(commoninterface.DataEntity)
		data.Data = hash
		impl.PutEntity(key, data)
	}
	return impl
}

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
		{
			name: "hdel key not exist",
			args: args{
				db:   NewDB(),
				args: common.CmdLine{[]byte("key"), []byte("field")},
			},
			want: resp.MakeIntReply(0),
		},
		{
			name: "hdel field not exist",
			args: args{
				db:   dbWithHData(NewDB(), "key", "field", "value"),
				args: common.CmdLine{[]byte("key"), []byte("field1")},
			},
			want: resp.MakeIntReply(0),
		},
		{
			name: "hdel key and feild exist",
			args: args{
				db:   dbWithHData(NewDB(), "key", "field", "value"),
				args: common.CmdLine{[]byte("key"), []byte("field")},
			},
			want: resp.MakeIntReply(1),
		},
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
		{
			name: "hexists key not exist",
			args: args{
				db:   NewDB(),
				args: common.CmdLine{[]byte("key"), []byte("field")},
			},
			want: resp.MakeIntReply(0),
		},
		{
			name: "hexists field not exist",
			args: args{
				db:   dbWithHData(NewDB(), "key", "field", "value"),
				args: common.CmdLine{[]byte("key"), []byte("field1")},
			},
			want: resp.MakeIntReply(0),
		},
		{
			name: "hexists key and feild exist",
			args: args{
				db:   dbWithHData(NewDB(), "key", "field", "value"),
				args: common.CmdLine{[]byte("key"), []byte("field")},
			},
			want: resp.MakeIntReply(1),
		},
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
		{
			name: "hget key not exist",
			args: args{
				db:   NewDB(),
				args: common.CmdLine{[]byte("key"), []byte("field")},
			},
			want: resp.MakeNullBulkReply(),
		},
		{
			name: "hget field not exist",
			args: args{
				db:   dbWithHData(NewDB(), "key", "field", "value"),
				args: common.CmdLine{[]byte("key"), []byte("field1")},
			},
			want: resp.MakeNullBulkReply(),
		},
		{
			name: "hget key and feild exist",
			args: args{
				db:   dbWithHData(NewDB(), "key", "field", "value"),
				args: common.CmdLine{[]byte("key"), []byte("field")},
			},
			want: resp.MakeBulkReply([]byte("value")),
		},
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
		{
			name: "hgetall key not exist",
			args: args{
				db:   NewDB(),
				args: common.CmdLine{[]byte("key")},
			},
			want: resp.MakeNullBulkReply(),
		},
		{
			name: "hgetall key exist",
			args: args{
				db:   dbWithHData(dbWithHData(NewDB(), "key", "field", "value"), "key", "field1", "value1"),
				args: common.CmdLine{[]byte("key")},
			},
			want: resp.MakeMultiBulkReply([][]byte{
				[]byte("field1"),
				[]byte("value1"),
				[]byte("field"),
				[]byte("value"),
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execHGetAll(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				fmt.Println(string(got.ToBytes()))
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
		{
			name: "hincrby key not exist",
			args: args{
				db:   NewDB(),
				args: common.CmdLine{[]byte("key"), []byte("field"), []byte("1")},
			},
			want: resp.MakeIntReply(1),
		},
		{
			name: "hincrby field not exist",
			args: args{
				db:   dbWithHData(NewDB(), "key", "field", "1"),
				args: common.CmdLine{[]byte("key"), []byte("field1"), []byte("1")},
			},
			want: resp.MakeIntReply(1),
		},
		{
			name: "hincrby field not int",
			args: args{
				db:   dbWithHData(NewDB(), "key", "field", "value"),
				args: common.CmdLine{[]byte("key"), []byte("field"), []byte("1")},
			},
			want: resp.MakeErrReply("hash value is not an integer"),
		},
		{
			name: "hincrby key and feild exist",
			args: args{
				db:   dbWithHData(NewDB(), "key", "field", "1"),
				args: common.CmdLine{[]byte("key"), []byte("field"), []byte("1")},
			},
			want: resp.MakeIntReply(2),
		},
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
		{
			name: "hincrbyfloat key not exist",
			args: args{
				db:   NewDB(),
				args: common.CmdLine{[]byte("key"), []byte("field"), []byte("1")},
			},
			want: resp.MakeBulkReply([]byte("1")),
		},
		{
			name: "hincrbyfloat field not exist",
			args: args{
				db:   dbWithHData(NewDB(), "key", "field", "1"),
				args: common.CmdLine{[]byte("key"), []byte("field1"), []byte("1")},
			},
			want: resp.MakeBulkReply([]byte("1")),
		},
		{
			name: "hincrbyfloat field not float",
			args: args{
				db:   dbWithHData(NewDB(), "key", "field", "value"),
				args: common.CmdLine{[]byte("key"), []byte("field"), []byte("1")},
			},
			want: resp.MakeErrReply("hash value is not a float"),
		},
		{
			name: "hincrbyfloat key and feild exist",
			args: args{
				db:   dbWithHData(NewDB(), "key", "field", "1"),
				args: common.CmdLine{[]byte("key"), []byte("field"), []byte("1")},
			},
			want: resp.MakeBulkReply([]byte("2")),
		},
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
		{
			name: "hkeys key not exist",
			args: args{
				db:   NewDB(),
				args: common.CmdLine{[]byte("key")},
			},
			want: resp.MakeNullBulkReply(),
		},
		{
			name: "hkeys key exist",
			args: args{
				db:   dbWithHData(dbWithHData(NewDB(), "key", "field1", "value1"), "key", "field2", "value2"),
				args: common.CmdLine{[]byte("key")},
			},
			want: resp.MakeMultiBulkReply([][]byte{
				[]byte("field1"),
				[]byte("field2"),
			}),
		},
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
		{
			name: "hlen key not exist",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			want: resp.MakeIntReply(0),
		},
		{
			name: "hlen key and field exist",
			args: args{
				db:   dbWithHData(dbWithHData(NewDB(), "key", "field1", "value1"), "key", "field2", "value2"),
				args: common.CmdLine{[]byte("key")},
			},
			want: resp.MakeIntReply(2),
		},
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
		{
			name: "hmget key not exist",
			args: args{
				db:   NewDB(),
				args: common.CmdLine{[]byte("key"), []byte("field1")},
			},
			want: resp.MakeNullBulkReply(),
		},
		{
			name: "hmget key and field exist",
			args: args{
				db:   dbWithHData(NewDB(), "key", "field1", "value1"),
				args: common.CmdLine{[]byte("key"), []byte("field1")},
			},
			want: resp.MakeMultiBulkReply([][]byte{[]byte("value1")}),
		},
		{
			name: "hmget key and field exist",
			args: args{
				db:   dbWithHData(dbWithHData(NewDB(), "key", "field1", "value1"), "key", "field2", "value2"),
				args: common.CmdLine{[]byte("key"), []byte("field1"), []byte("field2")},
			},
			want: resp.MakeMultiBulkReply([][]byte{[]byte("value1"), []byte("value2")}),
		},
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
		{
			name: "hmset key not exist",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("field1"),
					[]byte("value1"),
				},
			},
			want: resp.MakeOkReply(),
		},
		{
			name: "hmset key and field exist",
			args: args{
				db: dbWithHData(NewDB(), "key", "field1", "value1"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("field1"),
					[]byte("value11"),
				},
			},
			want: resp.MakeOkReply(),
		},
		{
			name: "hmset key and field exist",
			args: args{
				db: dbWithHData(NewDB(), "key", "field1", "value1"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("field1"),
					[]byte("value11"),
					[]byte("field2"),
					[]byte("value2"),
				},
			},
			want: resp.MakeOkReply(),
		},
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
		{
			name: "hset key not exist",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("field1"),
					[]byte("value1"),
				},
			},
			want: resp.MakeIntReply(1),
		},
		{
			name: "hset key and field exist",
			args: args{
				db: dbWithHData(NewDB(), "key", "field1", "value1"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("field1"),
					[]byte("value11"),
				},
			},
			want: resp.MakeIntReply(0),
		},
		{
			name: "hset key and field not exist",
			args: args{
				db: dbWithHData(NewDB(), "key", "field1", "value1"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("field11"),
					[]byte("value11"),
				},
			},
			want: resp.MakeIntReply(1),
		},
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
		{
			name: "hsetnx key not exist",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("field1"),
					[]byte("value1"),
				},
			},
			want: resp.MakeIntReply(1),
		},
		{
			name: "hsetnx key and field exist",
			args: args{
				db: dbWithHData(NewDB(), "key", "field1", "value1"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("field1"),
					[]byte("value11"),
				},
			},
			want: resp.MakeIntReply(0),
		},
		{
			name: "hsetnx key and field not exist",
			args: args{
				db: dbWithHData(NewDB(), "key", "field1", "value1"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("field11"),
					[]byte("value11"),
				},
			},
			want: resp.MakeIntReply(1),
		},
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
		{
			name: "hvals key not exist",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			want: resp.MakeNullBulkReply(),
		},
		{
			name: "hvals key exist",
			args: args{
				db: dbWithHData(NewDB(), "key", "field1", "value1"),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			want: resp.MakeMultiBulkReply([][]byte{
				[]byte("value1"),
			}),
		},
		{
			name: "hvals key exist",
			args: args{
				db: dbWithHData(dbWithHData(NewDB(), "key", "field1", "value1"), "key", "field2", "value2"),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			want: resp.MakeMultiBulkReply([][]byte{
				[]byte("value1"),
				[]byte("value2"),
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execHVals(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execHVals() = %v, want %v", got, tt.want)
			}
		})
	}
}
