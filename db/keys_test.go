package db

import (
	"container/list"
	"fmt"
	"math/rand"
	"mygodis/common"
	"mygodis/common/commoninterface"
	"mygodis/datadriver/dict"
	"mygodis/datadriver/set"
	"mygodis/datadriver/sortedset"
	"mygodis/resp"
	"mygodis/util/cmdutil"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// 随机数据
func randData(limit int) (result []struct {
	key  string
	data commoninterface.DataEntity
}) {
	result = make([]struct {
		key  string
		data commoninterface.DataEntity
	}, 0, limit)
	for i := 0; i < limit; i++ {
		var data commoninterface.DataEntity
		switch rand.Intn(4) {
		case 0:
			data = commoninterface.DataEntity{Data: strconv.Itoa(rand.Intn(limit))}
		case 1:
			data = commoninterface.DataEntity{Data: list.New()}
		case 2:
			data = commoninterface.DataEntity{Data: set.Make()}
		case 3:
			data = commoninterface.DataEntity{Data: sortedset.MakeZSet()}
		case 4:
			data = commoninterface.DataEntity{Data: dict.NewConcurrentDict()}
		}
		result = append(result, struct {
			key  string
			data commoninterface.DataEntity
		}{
			key:  strconv.Itoa(rand.Intn(limit)),
			data: data,
		})
	}
	return
}
func injectData(db *DataBaseImpl, data []struct {
	key  string
	data commoninterface.DataEntity
}) {
	for _, item := range data {
		db.PutEntity(item.key, &item.data)
	}
}

func Test_execDelete(t *testing.T) {
	type args struct {
		db  *DataBaseImpl
		cmd common.CmdLine
	}
	tests := []struct {
		name string
		args args
		want resp.Reply
	}{
		{
			name: "delete one but not exists",
			args: args{
				db:  NewDB(),
				cmd: cmdutil.ToCmdLine("a"),
			},
			want: resp.MakeIntReply(0),
		},
		{
			name: "delete one exists",
			args: args{
				db:  NewDB(),
				cmd: cmdutil.ToCmdLine("a"),
			},
			want: resp.MakeIntReply(1),
		},
		{
			name: "delete multi exists",
			args: args{
				db:  NewDB(),
				cmd: cmdutil.ToCmdLine("a", "b", "c"),
			},
			want: resp.MakeIntReply(3),
		},
	}
	// 生成随机数据
	tests[1].args.db.PutEntity("a", &commoninterface.DataEntity{Data: 1})
	tests[2].args.db.PutEntity("a", &commoninterface.DataEntity{Data: 1})
	tests[2].args.db.PutEntity("b", &commoninterface.DataEntity{Data: 1})
	tests[2].args.db.PutEntity("c", &commoninterface.DataEntity{Data: 1})

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := execDelete(test.args.db, test.args.cmd); !reflect.DeepEqual(got, test.want) {
				t.Errorf("except %v but got %v", test.want, got)
			}
		})
	}
}

func Test_execExists(t *testing.T) {
	newDB := NewDB()
	datas := make(map[string]commoninterface.DataEntity)
	data := randData(100)
	for _, d := range data {
		datas[d.key] = d.data
		newDB.PutEntity(d.key, &d.data)
	}
	t.Run("exists", func(t *testing.T) {
		for key := range datas {
			if got := execExists(newDB, cmdutil.ToCmdLine(key)); !reflect.DeepEqual(got, resp.MakeIntReply(1)) {
				t.Errorf("except %v but got %v", resp.MakeIntReply(1), got)
			}
		}
	})
	t.Run("not exists", func(t *testing.T) {
		for i := 100; i < 200; i++ {
			if got := execExists(newDB, cmdutil.ToCmdLine(fmt.Sprintf("%d", i))); !reflect.DeepEqual(got, resp.MakeIntReply(0)) {
				t.Errorf("except %v but got %v", resp.MakeIntReply(0), got)
			}
		}
	})
	t.Run("multi exists", func(t *testing.T) {
		var keys []string
		for key := range datas {
			keys = append(keys, key)
		}
		if got := execExists(newDB, cmdutil.ToCmdLine(keys...)); !reflect.DeepEqual(got, resp.MakeIntReply(int64(len(keys)))) {
			t.Errorf("except %v but got %v", resp.MakeIntReply(int64(len(keys))), got)
		}
	})
}

func Test_execExpire(t *testing.T) {
	db := NewDB()
	expires := make(map[string]int64)
	for i := int64(0); i < 10; i++ {
		expires[fmt.Sprintf("%d", i)] = i * 2
	}
	for i := int64(0); i < 10; i++ {
		db.PutEntity(fmt.Sprintf("%d", i), &commoninterface.DataEntity{Data: i})
	}
	for key, expire := range expires {
		if got := execExpire(db, cmdutil.ToCmdLine(key, strconv.FormatInt(expire, 10))); !reflect.DeepEqual(got, resp.MakeIntReply(1)) {
			t.Errorf("except %v but got %v", resp.MakeIntReply(1), got)
		}
		if rand.Int31n(10)%3 == 0 {
			dump(db)
		}
	}

	select {
	case <-time.After(time.Second * 30):
		dump(db)
	}

}

func Test_execExpireAt(t *testing.T) {
	db := NewDB()
	expires := make(map[string]int64)
	for i := int64(0); i < 10; i++ {
		expires[fmt.Sprintf("%d", i)] = time.Now().Add(time.Second * 2).Unix()
	}
	for i := int64(0); i < 10; i++ {
		db.PutEntity(fmt.Sprintf("%d", i), &commoninterface.DataEntity{Data: i})
	}
	dump(db)
	for key, expire := range expires {
		if got := execExpireAt(db, cmdutil.ToCmdLine(key, strconv.FormatInt(expire, 10))); !reflect.DeepEqual(got, resp.MakeIntReply(1)) {
			t.Errorf("except %v but got %v", resp.MakeIntReply(1), got)
		}
	}

	select {
	case <-time.After(time.Second * 25):
		dump(db)
	}

}

func Test_execFlushDB(t *testing.T) {
	db := NewDB()
	injectData(db, randData(100))
	if got := execFlushDB(db, cmdutil.ToCmdLine()); !reflect.DeepEqual(got, resp.MakeOkReply()) {
		t.Errorf("except %v but got %v", resp.MakeOkReply(), got)
	}
	if db.data.Len() != 0 {
		t.Errorf("db should be empty but got %d", db.data.Len())
	}
}

func Test_execPExpire(t *testing.T) {
	db := NewDB()
	expires := make(map[string]int64)
	for i := int64(0); i < 10; i++ {
		expires[fmt.Sprintf("%d", i)] = i * 2000
	}
	for i := int64(0); i < 10; i++ {
		db.PutEntity(fmt.Sprintf("%d", i), &commoninterface.DataEntity{Data: i})
	}
	for key, expire := range expires {
		if got := execPExpire(db, cmdutil.ToCmdLine(key, strconv.FormatInt(expire, 10))); !reflect.DeepEqual(got, resp.MakeIntReply(1)) {
			t.Errorf("except %v but got %v", resp.MakeIntReply(1), got)
		}
	}
	select {
	case <-time.After(time.Second * 30):
		dump(db)

	}
}

func Test_execPExpireAt(t *testing.T) {
	db := NewDB()
	expires := make(map[string]int64)
	for i := int64(0); i < 10; i++ {
		expires[fmt.Sprintf("%d", i)] = time.Now().Add(time.Second * time.Duration(i+1)).UnixMilli()
	}
	for i := int64(0); i < 10; i++ {
		db.PutEntity(fmt.Sprintf("%d", i), &commoninterface.DataEntity{Data: i})
	}
	for key, expire := range expires {
		if got := execPExpireAt(db, cmdutil.ToCmdLine(key, strconv.FormatInt(expire, 10))); !reflect.DeepEqual(got, resp.MakeIntReply(1)) {
			t.Errorf("except %v but got %v", resp.MakeIntReply(1), got)
		}
	}
	time.Sleep(time.Second * 30)
	dump(db)
}

func Test_execPTTL(t *testing.T) {
	db := NewDB()
	expires := make(map[string]int64)
	for i := int64(0); i < 10; i++ {
		expires[fmt.Sprintf("%d", i)] = time.Now().Add(time.Second * time.Duration(i+1)).UnixMilli()
	}
	for i := int64(0); i < 10; i++ {
		db.PutEntity(fmt.Sprintf("%d", i), &commoninterface.DataEntity{Data: i})
	}
	for key, _ := range expires {
		pttl := execPTTL(db, cmdutil.ToCmdLine(key))
		fmt.Println(string(pttl.ToBytes()))
	}
	time.Sleep(time.Second * 20)
	dump(db)
}

func Test_execPersist(t *testing.T) {
	db := NewDB()
	expires := make(map[string]int64)
	for i := int64(0); i < 10; i++ {
		expires[fmt.Sprintf("%d", i)] = time.Now().Add(time.Second * time.Duration(i+1)).UnixMilli()
	}
	for i := int64(0); i < 10; i++ {
		db.PutEntity(fmt.Sprintf("%d", i), &commoninterface.DataEntity{Data: i})
	}
	for key, ex := range expires {
		if got := execPExpireAt(db, cmdutil.ToCmdLine(key, strconv.FormatInt(ex, 10))); !reflect.DeepEqual(got, resp.MakeIntReply(1)) {
			t.Errorf("except %v but got %v", resp.MakeIntReply(1), got)
		}
	}
	for key, _ := range expires {
		if got := execPersist(db, cmdutil.ToCmdLine(key)); !reflect.DeepEqual(got, resp.MakeIntReply(1)) {
			t.Errorf("except %v but got %v", resp.MakeIntReply(1), got)
		}
	}
	time.Sleep(time.Second * 20)
	dump(db)
}

func Test_execRename(t *testing.T) {
	db := NewDB()
	injectData(db, randData(100))

	entity := &commoninterface.DataEntity{
		Data: "testData",
	}
	db.PutEntity("test", entity)
	if got := execRename(db, cmdutil.ToCmdLine("test", "test2")); !reflect.DeepEqual(got, resp.MakeOkReply()) {
		t.Errorf("except %v but got %v", resp.MakeOkReply(), got)
	}
	if got := execGet(db, cmdutil.ToCmdLine("test")); !reflect.DeepEqual(got, resp.MakeNullBulkReply()) {
		t.Errorf("except %v but got %v", resp.MakeNullBulkReply(), got)
	}
	if got := execGet(db, cmdutil.ToCmdLine("test2")); !reflect.DeepEqual(got, resp.MakeBulkReply([]byte("testData"))) {
		t.Logf("got %v", got)
		t.Errorf("except %v but got %v", resp.MakeBulkReply([]byte("testData")), got)
	}
}

func Test_execRenameNx(t *testing.T) {
	db := NewDB()
	injectData(db, randData(100))

	entity := &commoninterface.DataEntity{
		Data: "testData",
	}
	db.PutEntity("test", entity)
	if got := execRenameNx(db, cmdutil.ToCmdLine("test", "test2")); !reflect.DeepEqual(got, resp.MakeOkReply()) {
		t.Errorf("except %v but got %v", resp.MakeIntReply(1), got)
	}
	if got := execGet(db, cmdutil.ToCmdLine("test")); !reflect.DeepEqual(got, resp.MakeNullBulkReply()) {
		t.Errorf("except %v but got %v", resp.MakeNullBulkReply(), got)
	}
	if got := execGet(db, cmdutil.ToCmdLine("test2")); !reflect.DeepEqual(got, resp.MakeBulkReply([]byte("testData"))) {
		t.Logf("got %v", got)
		t.Errorf("except %v but got %v", resp.MakeBulkReply([]byte("testData")), got)
	}
}

func Test_execTTL(t *testing.T) {
	db := NewDB()
	expires := make(map[string]int64)
	for i := int64(0); i < 10; i++ {
		expires[fmt.Sprintf("%d", i)] = time.Now().Add(time.Second * time.Duration(i+1)).Unix()
	}
	for i := int64(0); i < 10; i++ {
		db.PutEntity(fmt.Sprintf("%d", i), &commoninterface.DataEntity{Data: i})
	}
	for key, _ := range expires {
		execExpireAt(db, cmdutil.ToCmdLine(key, strconv.FormatInt(expires[key], 10)))
	}
	for key, _ := range expires {
		ttl := execTTL(db, cmdutil.ToCmdLine(key))
		fmt.Println(string(ttl.ToBytes()))
	}
	time.Sleep(time.Second * 20)
	dump(db)
}

func Test_execType(t *testing.T) {
	db := NewDB()
	data := randData(10)
	injectData(db, data)
	for key, _ := range data {
		reply := execType(db, cmdutil.ToCmdLine(fmt.Sprintf("%d", key)))
		fmt.Println(string(reply.ToBytes()))
	}
}
