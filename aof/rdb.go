package aof

import (
	rdb "github.com/hdt3213/rdb/encoder"
	"github.com/hdt3213/rdb/model"
	"mygodis/common/commoninterface"
	"mygodis/config"
	"mygodis/datadriver/dict"
	"mygodis/datadriver/list"
	"mygodis/datadriver/set"
	"mygodis/datadriver/sortedset"
	logger "mygodis/log"
	"os"
	"strconv"
	"time"
)

func (persister *Persister) RewriteAofToRdb(rdbFilename string) error {
	ctx, err := persister.startRewriteRdb(nil, nil)
	if err != nil {
		return err
	}
	err = persister.rewriteRdb(ctx)
	if err != nil {
		return err
	}
	err = ctx.tmpFile.Close()
	if err != nil {
		return err
	}
	err = os.Rename(ctx.tmpFile.Name(), rdbFilename)
	if err != nil {
		return err
	}
	return nil
}
func (persister *Persister) startRewriteRdb(listener Listener, callBack func()) (*RewriteContext, error) {
	persister.lockForPausingAof.Lock()
	defer persister.lockForPausingAof.Unlock()
	err := persister.aofFile.Sync()
	if err != nil {
		logger.Warn("fsync failed")
		return nil, err
	}
	fileInfo, _ := os.Stat(persister.aofFilename)
	fileSize := fileInfo.Size()
	temp, err := os.CreateTemp("", "*.rdb")
	if err != nil {
		logger.Warn("create temp file failed")
		return nil, err
	}
	if listener != nil {
		persister.listeners[listener] = struct{}{}
	}
	if callBack != nil {
		callBack()
	}
	return &RewriteContext{
		tmpFile:  temp,
		fileSize: fileSize,
	}, nil
}
func (persister *Persister) rewriteRdb(ctx *RewriteContext) error {
	rewritePersister := persister.NewRewritePersister()
	rewritePersister.LoadAof(ctx.fileSize)
	encoder := rdb.NewEncoder(ctx.tmpFile).EnableCompress()
	err := encoder.WriteHeader()
	if err != nil {
		return err
	}
	auxMap := map[string]string{
		"redis-ver":    "6.0.0",
		"redis-bits":   "64",
		"aof-preamble": "0",
		"ctime":        strconv.FormatInt(time.Now().Unix(), 10),
	}
	for k, v := range auxMap {
		err = encoder.WriteAux(k, v)
		if err != nil {
			return err
		}
	}
	for i := 0; i < config.Properties.Databases; i++ {
		keyc, ttlc := rewritePersister.db.GetDBSize(i)
		if keyc == 0 {
			continue
		}
		err = encoder.WriteDBHeader(uint(i), uint64(keyc), uint64(ttlc))
		if err != nil {
			return err
		}
		rewritePersister.db.ForEach(i, func(key string, entity *commoninterface.DataEntity, expiration time.Time) bool {
			var opts []any
			if !expiration.IsZero() {
				opts = append(opts, rdb.WithTTL(uint64(expiration.UnixNano()/1e6)))
			}
			switch obj := entity.Data.(type) {
			case []byte:
				err = encoder.WriteStringObject(key, obj, opts...)

			case list.List:
				val := make([][]byte, 0, obj.Len())
				obj.ForEach(func(i int, v any) bool {
					bytes, _ := v.([]byte)
					val = append(val, bytes)
					return true
				})
				err = encoder.WriteListObject(key, val, opts...)
			case *set.Set:
				val := make([][]byte, 0, obj.Len())
				obj.ForEach(func(member string) bool {
					val = append(val, []byte(member))
					return true
				})
				err = encoder.WriteListObject(key, val, opts...)
			case dict.ConcurrentDict:
				val := make(map[string][]byte, obj.Len())
				obj.ForEach(func(field string, v any) bool {
					bytes, _ := v.([]byte)
					val[field] = bytes
					return true
				})
				err = encoder.WriteHashMapObject(key, val, opts...)
			case *sortedset.ZSet:
				var entries []*model.ZSetEntry
				obj.ForEach(0, obj.Len(), true, func(element *sortedset.Element) bool {
					entries = append(entries, &model.ZSetEntry{
						Score:  element.Score,
						Member: element.Member,
					})
					return true
				})
				err = encoder.WriteZSetObject(key, entries, opts...)
			}
			if err != nil {
				return false
			}
			return true
		})
		if err != nil {
			return err
		}
	}
	err = encoder.WriteEnd()
	if err != nil {
		return err
	}
	return nil
}
func (persister *Persister) RewriteRdbForReplication(rdbFilename string, listener Listener, callBack func()) error {
	ctx, err := persister.startRewriteRdb(listener, callBack)
	if err != nil {
		return err
	}
	err = persister.rewriteRdb(ctx)
	if err != nil {
		return err
	}
	err = ctx.tmpFile.Close()
	if err != nil {
		return err
	}
	err = os.Rename(ctx.tmpFile.Name(), rdbFilename)
	if err != nil {
		return err
	}
	return nil
}
