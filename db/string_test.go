package db

import (
	"fmt"
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
	}{
		{
			name: "decr with nil",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			want: resp.MakeIntReply(-1),
		},
		{
			name: "decr with empty string",
			args: args{
				db: dbWithData(NewDB(), "key", ""),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			want: resp.MakeIntReply(-1),
		},
		{
			name: "decr with string",
			args: args{
				db: dbWithData(NewDB(), "key", "2"),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			want: resp.MakeIntReply(1),
		},
	}
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
		{
			name: "decrby with nil",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("1"),
				},
			},
			want: resp.MakeIntReply(-1),
		},
		{
			name: "decrby with empty string",
			args: args{
				db: dbWithData(NewDB(), "key", ""),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("1"),
				},
			},
			want: resp.MakeIntReply(-1),
		},
		{
			name: "decrby with string",
			args: args{
				db: dbWithData(dbWithData(NewDB(), "key", "2"), "key1", "value"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("1"),
				},
			},
			want: resp.MakeIntReply(1),
		},
		{
			name: "decrby with string and negative number",
			args: args{
				db: dbWithData(dbWithData(NewDB(), "key", "2"), "key1", "value"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("-1"),
				},
			},
			want: resp.MakeIntReply(3),
		},
		{
			name: "decrby with string and invalid number",
			args: args{
				db: dbWithData(dbWithData(NewDB(), "key", "2"), "key1", "value"),
				args: common.CmdLine{
					[]byte("key"),

					[]byte("invalid"),
				},
			},
			want: resp.MakeErrReply("strconv.Atoi: parsing \"invalid\": invalid syntax"),
		},
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
		{
			name: "get with nil",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			want: resp.MakeNullBulkReply(),
		},
		{
			name: "get with empty string",
			args: args{
				db: dbWithData(NewDB(), "key", ""),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			want: resp.MakeBulkReply([]byte("")),
		},
		{
			name: "get with string",
			args: args{
				db: dbWithData(NewDB(), "key", "value"),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			want: resp.MakeBulkReply([]byte("value")),
		},
		{
			name: "get with string and nil key",
			args: args{
				db: dbWithData(NewDB(), "key", "value"),
				args: common.CmdLine{
					[]byte(nil),
				},
			},
			want: resp.MakeNullBulkReply(),
		},
		{
			name: "get with string and empty key",
			args: args{
				db: dbWithData(NewDB(), "key", "value"),
				args: common.CmdLine{
					[]byte(""),
				},
			},
			want: resp.MakeNullBulkReply(),
		},
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
		{
			name: "getbit with nil",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("1"),
				},
			},
			want: resp.MakeIntReply(0),
		},
		{
			name: "getbit with empty string",
			args: args{
				db: dbWithData(NewDB(), "key", ""),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("1"),
				},
			},
			want: resp.MakeIntReply(0),
		},
		{
			name: "getbit with string offset 0",
			args: args{
				db: dbWithData(NewDB(), "key", "value"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("0"),
				},
			},
			want: resp.MakeIntReply(0),
		},
		{
			name: "getbit with string offset 1",
			args: args{
				db: dbWithData(NewDB(), "key", "value"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("1"),
				},
			},
			want: resp.MakeIntReply(1),
		},
		{
			name: "getbit with string offset out of range more",
			args: args{
				db: dbWithData(NewDB(), "key", "value"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte(fmt.Sprintf("%d", bitmap.FromBytes([]byte("value")).BitSize()+666)),
				},
			},
			want: resp.MakeIntReply(0),
		},
		{
			name: "getbit with string offset out of range less",
			args: args{
				db: dbWithData(NewDB(), "key", "value"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte(fmt.Sprintf("%d", -666)),
				},
			},
			want: resp.MakeErrReply("bit offset is not an integer or out of range"),
		},
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
		{
			name: "getdel with nil",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			want: resp.MakeNullBulkReply(),
		},
		{
			name: "getdel with empty string",
			args: args{
				db: dbWithData(NewDB(), "key", ""),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			want: resp.MakeBulkReply([]byte("")),
		},
		{
			name: "getdel with string",
			args: args{
				db: dbWithData(NewDB(), "key", "value"),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			want: resp.MakeBulkReply([]byte("value")),
		},
		{
			name: "getdel with string and empty key",
			args: args{
				db: dbWithData(NewDB(), "key", "value"),
				args: common.CmdLine{
					[]byte(""),
				},
			},
			want: resp.MakeNullBulkReply(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execGetDel(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execGetDel() = %v, want %v", got, tt.want)
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
		{
			name: "getrange with nil",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("0"),
					[]byte("1"),
				},
			},
			want: resp.MakeNullBulkReply(),
		},
		{
			name: "getrange with empty string",
			args: args{
				db: dbWithData(NewDB(), "key", ""),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("0"),
					[]byte("1"),
				},
			},
			want: resp.MakeBulkReply([]byte("")),
		},
		{
			name: "getrange with string range is safe",
			args: args{
				db: dbWithData(NewDB(), "key", "value"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("0"),
					[]byte("1"),
				},
			},
			want: resp.MakeBulkReply([]byte("v")),
		},
		{
			name: "getrange with string range is unsafe",
			args: args{
				db: dbWithData(NewDB(), "key", "value"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("0"),
					[]byte("666"),
				},
			},
			want: resp.MakeBulkReply([]byte("value")),
		},
		{
			name: "getrange with string range is unsafe",
			args: args{
				db: dbWithData(NewDB(), "key", "value"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("666"),
					[]byte("666"),
				},
			},
			want: resp.MakeNullBulkReply(),
		},
		{
			name: "getrange with string range is unsafe",
			args: args{
				db: dbWithData(NewDB(), "key", "value"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("-666"),
					[]byte("-1"),
				},
			},
			want: resp.MakeBulkReply([]byte("valu")),
		},
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
		{
			name: "getset with nil",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("value"),
				},
			},
			want: resp.MakeNullBulkReply(),
		},
		{
			name: "getset with empty string",
			args: args{
				db: dbWithData(NewDB(), "key", ""),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("value"),
				},
			},
			want: resp.MakeBulkReply([]byte("")),
		},
		{
			name: "getset with string",
			args: args{
				db: dbWithData(NewDB(), "key", "value"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("newValue"),
				},
			},
			want: resp.MakeBulkReply([]byte("value")),
		},
		{
			name: "getset with string and empty key",
			args: args{
				db: dbWithData(NewDB(), "key", "value"),
				args: common.CmdLine{
					[]byte(""),
					[]byte("newValue"),
				},
			},
			want: resp.MakeNullBulkReply(),
		},
		{
			name: "getset with string and empty value",
			args: args{
				db: dbWithData(NewDB(), "key", "value"),

				args: common.CmdLine{
					[]byte("key"),
					[]byte(""),
				},
			},
			want: resp.MakeBulkReply([]byte("value")),
		},
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
		{
			name: "incr with nil",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			want: resp.MakeIntReply(1),
		},
		{
			name: "incr with empty string",
			args: args{
				db: dbWithData(NewDB(), "key", ""),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			want: resp.MakeIntReply(1),
		},
		{
			name: "incr with string",
			args: args{
				db: dbWithData(NewDB(), "key", "1"),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			want: resp.MakeIntReply(2),
		},
		{
			name: "incr with string and empty key",
			args: args{
				db: dbWithData(NewDB(), "key", "1"),
				args: common.CmdLine{
					[]byte(""),
				},
			},
			want: resp.MakeErrReply("key is empty"),
		},
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
		{
			name: "incrby with nil",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("1"),
				},
			},
			want: resp.MakeIntReply(1),
		},
		{
			name: "incrby with empty string",
			args: args{
				db: dbWithData(NewDB(), "key", ""),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("1"),
				},
			},
			want: resp.MakeIntReply(1),
		},
		{
			name: "incrby with string",
			args: args{
				db: dbWithData(NewDB(), "key", "1"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("1"),
				},
			},
			want: resp.MakeIntReply(2),
		},
		{
			name: "incrby with string and empty key",
			args: args{
				db: dbWithData(NewDB(), "key", "1"),
				args: common.CmdLine{
					[]byte(""),
					[]byte("1"),
				},
			},
			want: resp.MakeErrReply("key is empty"),
		},
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
		{
			name: "incrbyfloat with nil",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("1.1"),
				},
			},
			want: resp.MakeBulkReply([]byte("1.1")),
		},
		{
			name: "incrbyfloat with empty string",
			args: args{
				db: dbWithData(NewDB(), "key", ""),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("1.1"),
				},
			},
			want: resp.MakeBulkReply([]byte("1.1")),
		},
		{
			name: "incrbyfloat with string",
			args: args{
				db: dbWithData(NewDB(), "key", "1.1"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("1.1"),
				},
			},
			want: resp.MakeBulkReply([]byte("2.2")),
		},
		{
			name: "incrbyfloat with string and empty key",
			args: args{
				db: dbWithData(NewDB(), "key", "1.1"),
				args: common.CmdLine{
					[]byte(""),
				},
			},
			want: resp.MakeErrReply("wrong number of arguments for 'incrbyfloat' command"),
		},
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
		{
			name: "mget with nil",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			want: resp.MakeMultiBulkReply([][]byte{
				[]byte(nil),
			}),
		},
		{
			name: "mget with empty string",
			args: args{
				db: dbWithData(NewDB(), "key", ""),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			want: resp.MakeMultiBulkReply([][]byte{
				[]byte(""),
			},
			),
		},
		{
			name: "mget with string",
			args: args{

				db: dbWithData(NewDB(), "key", "1"),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			want: resp.MakeMultiBulkReply([][]byte{
				[]byte("1"),
			}),
		},
		{
			name: "mget with multi string",
			args: args{
				db: dbWithData(dbWithData(NewDB(), "key1", "1"), "key2", "2"),
				args: common.CmdLine{
					[]byte("key1"),
					[]byte("key2"),
				},
			},
			want: resp.MakeMultiBulkReply([][]byte{
				[]byte("1"),
				[]byte("2"),
			}),
		},
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
		{
			name: "mset one",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("1"),
				},
			},
			want: resp.MakeOkReply(),
		},
		{
			name: "mset multi",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("key1"),
					[]byte("1"),
					[]byte("key2"),
					[]byte("2"),
					[]byte("key3"),
					[]byte("3"),
				},
			},
			want: resp.MakeOkReply(),
		},
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
		{
			name: "msetnx one key not exist",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("1"),
				},
			},
			want: resp.MakeIntReply(1),
		},
		{
			name: "msetnx one key exist",
			args: args{
				db: dbWithData(NewDB(), "key", "1"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("1"),
				},
			},
			want: resp.MakeIntReply(0),
		},
		{
			name: "msetnx multi key not exist",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("key1"),
					[]byte("1"),
					[]byte("key2"),
					[]byte("2"),
					[]byte("key3"),
					[]byte("3"),
				},
			},
			want: resp.MakeIntReply(1),
		},
		{
			name: "msetnx multi key exist",
			args: args{
				db: dbWithData(dbWithData(NewDB(), "key1", "1"), "key2", "2"),
				args: common.CmdLine{
					[]byte("key1"),
					[]byte("1"),
					[]byte("key2"),
					[]byte("2"),
					[]byte("key3"),
					[]byte("3"),
				},
			},
			want: resp.MakeIntReply(0),
		},
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
	db := dbWithData(NewDB(), "key", "1")
	if asString, _ := db.getAsString("key"); string(asString) != "1" {
		t.Errorf("db.getAsString(\"key\") = %v, want %v", asString, "1")
	}
	t.Run("psetex", func(t *testing.T) {
		if got := execPSetEx(db, common.CmdLine{[]byte("key"), []byte("3000"), []byte("2")}); !reflect.DeepEqual(got, resp.MakeOkReply()) {
			t.Errorf("execPSetEx() = %v, want %v", got, resp.MakeOkReply())
		}
	})
	if asString, _ := db.getAsString("key"); string(asString) != "2" {
		t.Errorf("db.getAsString(\"key\") = %v, want %v", string(asString), "2")
	}
	time.Sleep(5 * time.Second)
	if asString, _ := db.getAsString("key"); asString != nil {
		t.Errorf("db.getAsString(\"key\") = %v, want %v", asString, nil)
	}

}

func Test_execSetBit(t *testing.T) {
	data := dbWithData(NewDB(), "key", "1")
	t.Run("setbit", func(t *testing.T) {
		if got := execSetBit(data, common.CmdLine{[]byte("key"), []byte("1"), []byte("1")}); !reflect.DeepEqual(got, resp.MakeIntReply(0)) {
			t.Errorf("execSetBit() = %v, want %v", got, resp.MakeIntReply(0))
		}
	})
	t.Run("setbit", func(t *testing.T) {
		if got := execSetBit(data, common.CmdLine{[]byte("key"), []byte("0"), []byte("0")}); !reflect.DeepEqual(got, resp.MakeIntReply(1)) {
			t.Errorf("execSetBit() = %v, want %v", got, resp.MakeIntReply(1))
		}
	})

}

func Test_execSetEx(t *testing.T) {
	db := dbWithData(NewDB(), "key", "1")
	if asString, _ := db.getAsString("key"); string(asString) != "1" {
		t.Errorf("db.getAsString(\"key\") = %v, want %v", asString, "1")
	}
	t.Run("setex", func(t *testing.T) {
		if got := execSetEx(db, common.CmdLine{[]byte("key"), []byte("3"), []byte("2")}); !reflect.DeepEqual(got, resp.MakeOkReply()) {
			t.Errorf("execPSetEx() = %v, want %v", got, resp.MakeOkReply())
		}
	})
	if asString, _ := db.getAsString("key"); string(asString) != "2" {
		t.Errorf("db.getAsString(\"key\") = %v, want %v", string(asString), "2")
	}
	time.Sleep(5 * time.Second)
	if asString, _ := db.getAsString("key"); asString != nil {
		t.Errorf("db.getAsString(\"key\") = %v, want %v", asString, nil)
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
		{
			name: "setnx key not exist",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("1"),
				},
			},
			want: resp.MakeIntReply(1),
		},
		{
			name: "setnx key exist",
			args: args{
				db: dbWithData(NewDB(), "key", "1"),
				args: common.CmdLine{
					[]byte("key"),
					[]byte("1"),
				},
			},
			want: resp.MakeIntReply(0),
		},
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
	dbWithData(NewDB(), "key", "hello world")
	t.Run("setrange", func(t *testing.T) {
		if got := execSetRange(NewDB(), common.CmdLine{[]byte("key"), []byte("6"), []byte("redis")}); !reflect.DeepEqual(got, resp.MakeIntReply(11)) {
			t.Errorf("execSetRange() = %v, want %v", got, resp.MakeIntReply(11))
		}
	})
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
		{
			name: "strlen key not exist",
			args: args{
				db: NewDB(),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			want: resp.MakeIntReply(0),
		},
		{
			name: "strlen key exist",
			args: args{
				db: dbWithData(NewDB(), "key", "1"),
				args: common.CmdLine{
					[]byte("key"),
				},
			},
			want: resp.MakeIntReply(1),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := execStrLen(tt.args.db, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execStrLen() = %v, want %v", got, tt.want)
			}
		})
	}
}
