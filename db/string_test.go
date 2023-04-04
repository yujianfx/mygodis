package db

import (
	"mygodis/common"
	"mygodis/common/commoninterface"
	"mygodis/datadriver/bitmap"
	"mygodis/resp"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func Test_execGetEx(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	db := NewDB()
	db.PutEntity("key", &commoninterface.DataEntity{
		Data: []byte("OK"),
	})
	tests := []struct {
		name    string
		args    args
		wantRes resp.Reply
	}{
		{
			name: "getex wrong",
			args: args{
				db: db,
				args: common.CmdLine{
					[]byte("key"),
					[]byte("EX"),
				},
			},
			wantRes: resp.MakeSyntaxErrReply(),
		},
		{
			name: "getex ex",
			args: args{
				db: db,
				args: common.CmdLine{
					[]byte("key"),
					[]byte("EX"),
					[]byte("10"),
				},
			},
			wantRes: resp.MakeBulkReply([]byte("OK")),
		},
		{
			name: "getex px",
			args: args{
				db: db,
				args: common.CmdLine{
					[]byte("key"),
					[]byte("PX"),
					[]byte("10000"),
				},
			},
			wantRes: resp.MakeBulkReply([]byte("OK")),
		},
		{
			name: "getex exat",
			args: args{
				db: db,
				args: common.CmdLine{
					[]byte("key"),
					[]byte("EXAT"),
					[]byte(strconv.FormatInt(time.Now().Add(10*time.Second).Unix(), 10)),
				},
			},
			wantRes: resp.MakeBulkReply([]byte("OK")),
		},
		{
			name: "getex pxat",
			args: args{
				db: db,
				args: common.CmdLine{
					[]byte("key"),
					[]byte("PXAT"),
					[]byte(strconv.FormatInt(time.Now().Add(10*time.Second).UnixMilli(), 10)),
				},
			},
			wantRes: resp.MakeBulkReply([]byte("OK")),
		},
		{
			name: "getex persist",
			args: args{
				db: db,
				args: common.CmdLine{
					[]byte("key"),
					[]byte("PERSIST"),
				},
			},
			wantRes: resp.MakeBulkReply([]byte("OK")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotRes := execGetEx(tt.args.db, tt.args.args); !reflect.DeepEqual(gotRes, tt.wantRes) {
				t.Errorf("execGetEx() = %v, want %v", gotRes, tt.wantRes)
			}
		})
	}
	time.Sleep(11 * time.Second)
}

func Test_parseSet(t *testing.T) {
	type args struct {
		args   common.CmdLine
		policy *setPolicy
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "parse set",
			args: args{
				args:   common.CmdLine{},
				policy: &setPolicy{putPolicy: put, expirePolicy: noEx},
			},
			wantErr: false,
		},
		{
			name: "parse set ex",
			args: args{
				args: common.CmdLine{
					[]byte("EX"),
					[]byte("10"),
				},
				policy: &setPolicy{putPolicy: put, expirePolicy: noEx},
			},
			wantErr: false,
		},
		{
			name: "parse set px",
			args: args{
				args: common.CmdLine{
					[]byte("PX"),
					[]byte("10000"),
				},
				policy: &setPolicy{putPolicy: put, expirePolicy: noEx},
			},
			wantErr: false,
		},
		{
			name: "parse set exat",
			args: args{
				args: common.CmdLine{
					[]byte("EXAT"),
					[]byte(strconv.FormatInt(time.Now().Add(10*time.Second).Unix(), 10)),
				},
				policy: &setPolicy{putPolicy: put, expirePolicy: noEx},
			},
			wantErr: false,
		},
		{
			name: "parse set pxat",
			args: args{
				args: common.CmdLine{
					[]byte("PXAT"),
					[]byte(strconv.FormatInt(time.Now().Add(10*time.Second).UnixMilli(), 10)),
				},
				policy: &setPolicy{putPolicy: put, expirePolicy: noEx},
			},
			wantErr: false,
		},
		{
			name: "parse set nx",
			args: args{
				args: common.CmdLine{
					[]byte("NX"),
				},
				policy: &setPolicy{putPolicy: put, expirePolicy: noEx},
			},
			wantErr: false,
		},
		{
			name: "parse set xx",
			args: args{
				args: common.CmdLine{
					[]byte("XX"),
				},
				policy: &setPolicy{putPolicy: put, expirePolicy: noEx},
			},
			wantErr: false,
		},
		{
			name: "parse set keepttl",
			args: args{
				args: common.CmdLine{
					[]byte("KEEPTTL"),
				},
				policy: &setPolicy{putPolicy: put, expirePolicy: noEx},
			},
			wantErr: false,
		},
		{
			name: "parse set wrong",
			args: args{
				args: common.CmdLine{
					[]byte("wrong"),
				},
				policy: &setPolicy{putPolicy: put, expirePolicy: noEx},
			},
			wantErr: true,
		},
		{
			name: "parse set nx ex",
			args: args{
				args: common.CmdLine{
					[]byte("NX"),
					[]byte("EX"),
					[]byte("10"),
				},
				policy: &setPolicy{putPolicy: put, expirePolicy: noEx},
			},
			wantErr: false,
		},
		{
			name: "parse set nx px",
			args: args{
				args: common.CmdLine{
					[]byte("NX"),
					[]byte("PX"),
					[]byte("10000"),
				},
				policy: &setPolicy{putPolicy: put, expirePolicy: noEx},
			},
			wantErr: false,
		},
		{
			name: "parse set nx exat",
			args: args{
				args: common.CmdLine{
					[]byte("NX"),
					[]byte("EXAT"),
					[]byte(strconv.FormatInt(time.Now().Add(10*time.Second).Unix(), 10)),
				},
				policy: &setPolicy{putPolicy: put, expirePolicy: noEx},
			},
			wantErr: false,
		},
		{
			name: "parse set nx pxat",
			args: args{
				args: common.CmdLine{
					[]byte("NX"),
					[]byte("PXAT"),
					[]byte(strconv.FormatInt(time.Now().Add(10*time.Second).UnixMilli(), 10)),
				},
				policy: &setPolicy{putPolicy: put, expirePolicy: noEx},
			},
			wantErr: false,
		},
		{
			name: "parse set xx ex",
			args: args{
				args: common.CmdLine{
					[]byte("XX"),
					[]byte("EX"),
					[]byte("10"),
				},
				policy: &setPolicy{putPolicy: put, expirePolicy: noEx},
			},
			wantErr: false,
		},
		{
			name: "parse set xx px",
			args: args{
				args: common.CmdLine{
					[]byte("XX"),
					[]byte("PX"),
					[]byte("10000"),
				},
				policy: &setPolicy{putPolicy: put, expirePolicy: noEx},
			},
			wantErr: false,
		},
		{
			name: "parse set xx exat",
			args: args{
				args: common.CmdLine{
					[]byte("XX"),
					[]byte("EXAT"),

					[]byte(strconv.FormatInt(time.Now().Add(10*time.Second).Unix(), 10)),
				},
				policy: &setPolicy{putPolicy: put, expirePolicy: noEx},
			},
			wantErr: false,
		},
		{
			name: "parse set xx pxat",
			args: args{
				args: common.CmdLine{
					[]byte("XX"),
					[]byte("PXAT"),
					[]byte(strconv.FormatInt(time.Now().Add(10*time.Second).UnixMilli(), 10)),
				},
				policy: &setPolicy{putPolicy: put, expirePolicy: noEx},
			},
			wantErr: false,
		},
		{
			name: "parse set nx xx",
			args: args{
				args: common.CmdLine{
					[]byte("NX"),
					[]byte("XX"),
				},
				policy: &setPolicy{putPolicy: put, expirePolicy: noEx},
			},
			wantErr: true,
		},
		{
			name: "parse set nx exat pxat",
			args: args{
				args: common.CmdLine{

					[]byte("NX"),
					[]byte("EXAT"),
					[]byte(strconv.FormatInt(time.Now().Add(10*time.Second).Unix(), 10)),
					[]byte("PXAT"),
					[]byte(strconv.FormatInt(time.Now().Add(10*time.Second).UnixMilli(), 10)),
				},
				policy: &setPolicy{putPolicy: put, expirePolicy: noEx},
			},
			wantErr: true,
		},
		{
			name: "parse set nx exat ex",
			args: args{
				args: common.CmdLine{
					[]byte("NX"),
					[]byte("EXAT"),
					[]byte(strconv.FormatInt(time.Now().Add(10*time.Second).Unix(), 10)),
					[]byte("EX"),
					[]byte("10"),
				},
				policy: &setPolicy{putPolicy: put, expirePolicy: noEx},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := parseSet(tt.args.args, tt.args.policy); (err != nil) != tt.wantErr {
				t.Errorf("parseSet() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_execSet(t *testing.T) {
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
			name: "set",
			args: args{
				db:   NewDB(),
				args: common.CmdLine{[]byte("key"), []byte("value")},
			},
			want: resp.MakeBulkReply([]byte("OK")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execSet(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execSet() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_undoBitOpCommands(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name string
		args args
		want []common.CmdLine
	}{
		{
			name: "bitop and",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("bitop"),
					[]byte("and"),
					[]byte("dest"),
					[]byte("key1"),
					[]byte("key2"),
				},
			},
			want: []common.CmdLine{
				{
					[]byte("DEL"),
					[]byte("dest"),
				},
			},
		},
		{
			name: "bitop or",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("bitop"),
					[]byte("or"),
					[]byte("dest"),
					[]byte("key1"),
					[]byte("key2"),
				},
			},
			want: []common.CmdLine{
				{
					[]byte("DEL"),
					[]byte("dest"),
				},
			},
		},
		{
			name: "bitop not",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("bitop"),
					[]byte("not"),
					[]byte("dest"),
					[]byte("key1"),
				},
			},
			want: []common.CmdLine{
				{
					[]byte("DEL"),
					[]byte("dest"),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := undoBitOpCommands(tt.args.db, tt.args.args[1:]); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("undoBitOpCommands() = %s, want %s", got, tt.want)
			}
		})
	}
}

func Test_execAppend(t *testing.T) {
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
			name: "append to empty key",
			args: args{
				db:   NewDB(),
				args: common.CmdLine{[]byte("key"), []byte("value")},
			},
			want: resp.MakeIntReply(5),
		},
		{
			name: "append to non-empty key",
			args: args{
				db: dbWithData(NewDB(), "key", "value"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("value"),
				},
			},
			want: resp.MakeIntReply(10),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execAppend(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execAppend() = %v, want %v", got, tt.want)
			}
		})
	}
}
func dbWithData(db *DataBaseImpl, key string, value any) *DataBaseImpl {
	c := new(commoninterface.DataEntity)
	c.Data = value
	db.data.Put(key, c)
	return db
}
func Test_execBitCount(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	count := 0
	bitMap := bitmap.FromBytes([]byte("value"))
	bitMap.ForEachBit(0, bitMap.BitSize(), func(offset int64, val byte) bool {
		if val == 1 {
			count++
		}
		return true
	})
	bitCountF := func(bytes []byte, start, end int64) int64 {
		count := 0
		bitMap := bitmap.FromBytes(bytes)
		bitMap.ForEachBit(start, end, func(offset int64, val byte) bool {
			if val == 1 {
				count++
			}
			return true
		})
		return int64(count)
	}

	tests := []struct {
		name string
		args args
		want resp.Reply
	}{
		{
			name: "bitcount with nil",
			args: args{
				db: dbWithData(NewDB(), "key", nil),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			want: resp.MakeErrReply("value is not a string"),
		},
		{
			name: "bitcount with empty string",
			args: args{
				db: dbWithData(NewDB(), "key", ""),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			want: resp.MakeIntReply(0),
		},
		{
			name: "bitcount with string",
			args: args{
				db: dbWithData(NewDB(), "key", "value"),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			want: resp.MakeIntReply(int64(count)),
		},
		{
			name: "bitcount with string start and end",
			args: args{
				db: dbWithData(NewDB(), "key", "value"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("1"),
					[]byte("6"),
				},
			},
			want: resp.MakeIntReply(bitCountF([]byte("value"), 1, 6)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execBitCount(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execBitCount() = %v, want %v", got, tt.want)
			}
		})
	}
}

var bitopFunc = func(op string, dest string, keys []string) []byte {
	var result []byte
	for _, key := range keys {
		if result == nil {
			result = []byte(key)
		} else {
			switch op {
			case "AND":
				bitmap.And(bitmap.FromBytes(result), bitmap.FromBytes([]byte(key)))
			case "OR":
				bitmap.Or(bitmap.FromBytes(result), bitmap.FromBytes([]byte(key)))
			case "XOR":
				bitmap.Xor(bitmap.FromBytes(result), bitmap.FromBytes([]byte(key)))
			case "NOT":
				bitmap.Not(bitmap.FromBytes(result), bitmap.FromBytes([]byte(key)))
			}
		}
	}
	return result
}

func Test_execBitOp(t *testing.T) {
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
			name: "bitop and with nil",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("AND"),
					[]byte("key"),
					[]byte("key1"),
				},
			},
			want: resp.MakeErrReply("ERR key not exists"),
		},
		{
			name: "bitop and with empty string",
			args: args{
				db: dbWithData(NewDB(), "key", ""),
				args: common.CmdLine{
					[]byte("AND"),
					[]byte("key"),
					[]byte("key1"),
				},
			},
			want: resp.MakeErrReply("ERR key not exists"),
		},
		{
			name: "bitop and with string",
			args: args{
				db: dbWithData(dbWithData(NewDB(), "key", "value"), "key1", "value1"),
				args: common.CmdLine{
					[]byte("AND"),
					[]byte("key"),
					[]byte("key1"),
				},
			},
			want: resp.MakeIntReply(6),
		},
		{
			name: "bitop and with string 3 keys",
			args: args{
				db: dbWithData(dbWithData(dbWithData(NewDB(), "key", "value"), "key1", "value1"), "key2", "value12"),
				args: common.CmdLine{
					[]byte("AND"),
					[]byte("key"),
					[]byte("key1"),
					[]byte("key2"),
				},
			},
			want: resp.MakeIntReply(7),
		},
		{
			name: "bitop or with string 3 keys",
			args: args{
				db: dbWithData(dbWithData(dbWithData(NewDB(), "key", "value"), "key1", "value1"), "key2", "value12"),
				args: common.CmdLine{
					[]byte("OR"),
					[]byte("key"),
					[]byte("key1"),
					[]byte("key2"),
				},
			},
			want: resp.MakeIntReply(7),
		},
		{
			name: "bitop xor with string 3 keys",
			args: args{
				db: dbWithData(dbWithData(dbWithData(NewDB(), "key", "value"), "key1", "value1"), "key2", "value12"),
				args: common.CmdLine{
					[]byte("XOR"),
					[]byte("key"),
					[]byte("key1"),
					[]byte("key2"),
				},
			},
			want: resp.MakeIntReply(7),
		},
		{
			name: "bitop not with string 2 keys",
			args: args{
				db: dbWithData(dbWithData(NewDB(), "key", "value"), "key1", "value1"),
				args: common.CmdLine{
					[]byte("NOT"),
					[]byte("key"),
					[]byte("key1"),
				},
			},
			want: resp.MakeIntReply(6),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execBitOp(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execBitOp() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execDecr(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name string
		args args
		want resp.Reply
	}{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execDecr(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execDecr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execDecrBy(t *testing.T) {
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
			if got := execDecrBy(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execDecrBy() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execGet(t *testing.T) {
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
			if got := execGet(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execGet() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execGetBit(t *testing.T) {
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
			if got := execGetBit(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execGetBit() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execGetDel(t *testing.T) {
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
			if got := execGetDel(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execGetDel() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execGetEx1(t *testing.T) {
	type args struct {
		db   *DataBaseImpl
		args common.CmdLine
	}
	tests := []struct {
		name    string
		args    args
		wantRes resp.Reply
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotRes := execGetEx(tt.args.db, tt.args.args); !reflect.DeepEqual(gotRes, tt.wantRes) {
				t.Errorf("execGetEx() = %v, want %v", gotRes, tt.wantRes)
			}
		})
	}
}

func Test_execGetRange(t *testing.T) {
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
			if got := execGetRange(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execGetRange() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execGetSet(t *testing.T) {
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
			if got := execGetSet(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execGetSet() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execIncr(t *testing.T) {
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
			if got := execIncr(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execIncr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execIncrBy(t *testing.T) {
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
			if got := execIncrBy(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execIncrBy() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execIncrByFloat(t *testing.T) {
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
			if got := execIncrByFloat(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execIncrByFloat() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execMGet(t *testing.T) {
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
			if got := execMGet(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execMGet() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execMSet(t *testing.T) {
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
			if got := execMSet(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execMSet() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execMSetNx(t *testing.T) {
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
			if got := execMSetNx(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execMSetNx() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execPSetEx(t *testing.T) {
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
			if got := execPSetEx(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execPSetEx() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execSet1(t *testing.T) {
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
			if got := execSet(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execSet() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execSetBit(t *testing.T) {
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
			if got := execSetBit(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execSetBit() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execSetEx(t *testing.T) {
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
			if got := execSetEx(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execSetEx() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execSetNx(t *testing.T) {
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
			if got := execSetNx(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execSetNx() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execSetRange(t *testing.T) {
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
			if got := execSetRange(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execSetRange() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_execStrLen(t *testing.T) {
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
			if got := execStrLen(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execStrLen() = %v, want %v", got, tt.want)
			}
		})
	}
}
