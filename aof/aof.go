package aof

import (
	"context"
	"io"
	"mygodis/clientc"
	cm "mygodis/common"
	"mygodis/common/commoninterface"
	"mygodis/config"
	logger "mygodis/log"
	"mygodis/parse"
	"mygodis/resp"
	"mygodis/util/cmdutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	AofQueueSize = uint32(1) << 16
)
const (
	Always = int8(iota) << 1
	EverySec
	No
)

type Payload struct {
	CmdLine cm.CmdLine
	DbIndex int
	Wg      *sync.WaitGroup
}
type Listener interface {
	Callback([]cm.CmdLine)
}

type Persister struct {
	ctx               context.Context
	clFunc            context.CancelFunc
	db                commoninterface.StandaloneDBEngine
	tmpDBMaker        func() commoninterface.StandaloneDBEngine
	aofChan           chan *Payload
	aofFile           *os.File
	aofFilename       string
	aofFsyncAction    int8
	aofFinished       chan struct{}
	lockForPausingAof sync.Mutex
	currenDbIndex     int
	listeners         map[Listener]struct{}
	cmdBuffer         []cm.CmdLine
}
type RewriteContext struct {
	tmpFile  *os.File
	fileSize int64
	dbIndex  int
}

func NewPersister(db commoninterface.StandaloneDBEngine, filename string, load bool, fsync int8, tmpDBMaker func() commoninterface.StandaloneDBEngine) (*Persister, error) {
	persister := &Persister{}
	persister.ctx, persister.clFunc = context.WithCancel(context.Background())
	persister.tmpDBMaker = tmpDBMaker
	persister.db = db
	persister.aofChan = make(chan *Payload, AofQueueSize)
	persister.aofFilename = filename
	persister.aofFsyncAction = fsync
	persister.aofFinished = make(chan struct{})
	persister.listeners = make(map[Listener]struct{})
	persister.aofFinished = make(chan struct{})
	persister.currenDbIndex = 0
	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		logger.Errorf("open aof file error: %v", err)
		return nil, err
	}
	persister.aofFile = aofFile
	if load {
		persister.LoadAof(0)
	}
	go func() {
		persister.ListenCmd()
	}()

	return persister, nil
}
func (persister *Persister) ListenCmd() {
	for payload := range persister.aofChan {
		persister.writeAof(payload)
	}
	persister.aofFinished <- struct{}{}
}
func (persister *Persister) FsyncEverySec() {
	ticker := time.NewTicker(time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				persister.lockForPausingAof.Lock()
				err := persister.aofFile.Sync()
				if err != nil {
					logger.Errorf("aof fsync error: %v", err)
				}
				persister.lockForPausingAof.Unlock()
			case <-persister.ctx.Done():
				return
			}
		}
	}()
}
func (persister *Persister) Close() {
	if persister.aofFile != nil {
		close(persister.aofChan)
		<-persister.aofFinished
		err := persister.aofFile.Close()
		if err != nil {
			logger.Errorf("aof close error: %v", err)
		}
	}
	persister.clFunc()

}
func (persister *Persister) LoadAof(maxBytes int64) {
	aofChan := persister.aofChan
	persister.aofChan = nil
	defer func(aofChan chan *Payload) {
		persister.aofChan = aofChan
	}(aofChan)
	aofFile, err := os.Open(persister.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			logger.Error("aof file not exist")
			return
		}
		logger.Errorf("aof file open error: %v", err)
		return
	}
	defer func(aofFile *os.File) {
		err := aofFile.Close()
		if err != nil {
			logger.Errorf("aof file close error: %v", err)
		}
	}(aofFile)
	var reader io.Reader
	if maxBytes > 0 {
		reader = io.LimitReader(aofFile, maxBytes)
	} else {
		reader = aofFile
	}
	fakeConnection := clientc.NewFakeConnection()
	payLoadCh := parse.Parse(reader)
	for payLoad := range payLoadCh {
		if payLoad.Err != nil {
			if payLoad.Err == io.EOF {
				break
			}
			logger.Errorf("aof file parse error: %v", payLoad.Err)
		}
		if payLoad.Data == nil {
			logger.Warn("aof file parse empty data")
		}
		reply, ok := payLoad.Data.(*resp.MultiBulkReply)
		if !ok {
			logger.Warn("aof file parse error")
			continue
		}
		result := persister.db.Exec(fakeConnection, reply.Args)
		if resp.IsErrorReply(result) {
			logger.Errorf("aof file exec error: %v", result)
		}
		if strings.ToLower(string(reply.Args[0])) == "select" {
			dbIndex, err := strconv.Atoi(string(reply.Args[1]))
			if err == nil {
				persister.currenDbIndex = dbIndex
			}
		}
	}
}
func (persister *Persister) writeAof(payload *Payload) {
	persister.cmdBuffer = persister.cmdBuffer[:0]
	persister.lockForPausingAof.Lock()
	defer persister.lockForPausingAof.Unlock()
	if payload.DbIndex != persister.currenDbIndex {
		selectDBcmd := cmdutil.ToCmdLine("select", strconv.Itoa(payload.DbIndex))
		persister.cmdBuffer = append(persister.cmdBuffer, selectDBcmd)
		persister.currenDbIndex = payload.DbIndex
		reply := resp.MakeMultiBulkReply(selectDBcmd)
		_, err := persister.aofFile.Write(reply.ToBytes())
		if err != nil {
			logger.Errorf("aof file write error: %v", err)
			return
		}
	}
	data := resp.MakeMultiBulkReply(payload.CmdLine)
	persister.cmdBuffer = append(persister.cmdBuffer, payload.CmdLine)
	_, err := persister.aofFile.Write(data.ToBytes())
	if err != nil {
		logger.Warn("aof file write error: %v", err)
	}
	if persister.aofFsyncAction == Always {
		err := persister.aofFile.Sync()
		if err != nil {
			logger.Errorf("aof fsync error: %v", err)
		}
	}
}
func (persister *Persister) RemoveListener(listener Listener) {
	persister.lockForPausingAof.Lock()
	defer persister.lockForPausingAof.Unlock()
	delete(persister.listeners, listener)
}
func (persister *Persister) AddListener(listener Listener) {
	persister.lockForPausingAof.Lock()
	defer persister.lockForPausingAof.Unlock()
	persister.listeners[listener] = struct{}{}
}
func (persister *Persister) SaveCmd(dbIndex int, cmdLine [][]byte) {
	if persister.aofChan == nil {
		return
	}
	payload := &Payload{
		CmdLine: cmdLine,
		DbIndex: dbIndex,
	}
	if persister.aofFsyncAction == Always {
		persister.writeAof(payload)
		return
	}
	persister.aofChan <- payload
}
func (persister *Persister) NewRewritePersister() *Persister {
	return &Persister{
		aofFilename: persister.aofFilename,
		db:          persister.tmpDBMaker(),
	}
}
func (persister *Persister) RewriteAof() error {
	rewriteContext, err := persister.StartRewriteAof()
	if err != nil {
		return err
	}
	err = persister.DoRewriteAof(rewriteContext)
	if err != nil {
		return err
	}
	persister.FinishedRewriteAof(rewriteContext)
	return nil
}
func (persister *Persister) StartRewriteAof() (rewriteContext *RewriteContext, err error) {
	persister.lockForPausingAof.Lock()
	defer persister.lockForPausingAof.Unlock()
	err = persister.aofFile.Sync()
	if err != nil {
		logger.Errorf("aof fsync error: %v", err)
		return nil, err
	}
	aofFileInfo, _ := os.Stat(persister.aofFilename)
	fileSize := aofFileInfo.Size()
	temp, err := os.CreateTemp("", "*.aof")
	if err != nil {
		logger.Errorf("aof create temp file error: %v", err)
		return nil, err
	}
	rewriteContext = &RewriteContext{
		tmpFile:  temp,
		fileSize: fileSize,
		dbIndex:  persister.currenDbIndex,
	}
	return rewriteContext, nil
}
func (persister *Persister) FinishedRewriteAof(rewriteContext *RewriteContext) {
	persister.lockForPausingAof.Lock()
	defer persister.lockForPausingAof.Unlock()
	tmpFile := rewriteContext.tmpFile
	srcAof, err := os.Open(persister.aofFilename)
	if err != nil {
		logger.Errorf("aof srcAof file error: %v", err)
		return
	}
	defer func(srcAof *os.File) {
		err := srcAof.Close()
		if err != nil {
			logger.Errorf("aof file close error: %v", err)
		}
	}(srcAof)
	_, err = srcAof.Seek(rewriteContext.fileSize, io.SeekStart)
	if err != nil {
		logger.Errorf("aof file seek error: %v", err)
		return
	}
	data := resp.MakeMultiBulkReply(cmdutil.ToCmdLine("SELECT", strconv.Itoa(rewriteContext.dbIndex))).ToBytes()
	_, err = tmpFile.Write(data)
	if err != nil {
		logger.Error("tmp file rewrite failed: " + err.Error())
		return
	}
	_, err = io.Copy(tmpFile, srcAof)
	if err != nil {
		logger.Errorf("aof file copy error: %v", err)
		return
	}
	_ = persister.aofFile.Close()
	_ = os.Rename(tmpFile.Name(), persister.aofFilename)
	persister.aofFile, err = os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		logger.Errorf("aof file open error: %v", err)
		return
	}
	data = resp.MakeMultiBulkReply(cmdutil.ToCmdLine("SELECT", strconv.Itoa(persister.currenDbIndex))).ToBytes()
	_, err = persister.aofFile.Write(data)
	if err != nil {
		panic(err)
	}
}
func (persister *Persister) DoRewriteAof(rewriteContext *RewriteContext) error {
	tmpFile := rewriteContext.tmpFile
	rewritePersister := persister.NewRewritePersister()
	rewritePersister.LoadAof(rewriteContext.fileSize)
	for i := 0; i < config.Properties.Databases; i++ {
		data := resp.MakeMultiBulkReply(cmdutil.ToCmdLine("SELECT", strconv.Itoa(i))).ToBytes()
		_, err := tmpFile.Write(data)
		if err != nil {
			logger.Error("tmp file rewrite failed: " + err.Error())
			return err
		}
		rewritePersister.db.ForEach(i, func(key string, data *commoninterface.DataEntity, expiration *time.Time) bool {
			cmd := EntityToCmd(key, data)
			if cmd != nil {
				_, _ = tmpFile.Write(cmd.ToBytes())
			}
			if expiration != nil {
				cmd = ExpireToCmd(key, expiration)
				if cmd != nil {
					_, _ = tmpFile.Write(cmd.ToBytes())
				}
			}
			return true
		})
	}
	return nil
}
