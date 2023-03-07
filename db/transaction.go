package db

import (
	cm "mygodis/common"
	"mygodis/common/commoninterface"
	"mygodis/resp"
	"strings"
)

func StartMulti(c commoninterface.Connection) (reply resp.Reply) {
	if c.InMultiState() {
		return resp.MakeMultiErrReply()
	}
	c.SetMultiState(true)
	return resp.MakeOkReply()
}

func DiscardMulti(c commoninterface.Connection) resp.Reply {
	if !c.InMultiState() {
		return resp.MakeMultiErrReply()
	}
	c.SetMultiState(false)
	c.ClearQueuedCmds()
	return resp.MakeOkReply()
}
func Watch(db *DataBaseImpl, c commoninterface.Connection, cmd [][]byte) (reply resp.Reply) {
	if len(cmd) < 2 {
		return resp.MakeArgNumErrReply("watch")
	}
	watching := c.GetWatching()
	for _, key := range cmd[1:] {
		if _, ok := db.GetEntity(string(key)); ok {
			watching[string(key)], _ = db.GetVersion(string(key))
		}
	}
	return resp.MakeOkReply()
}
func watchChanged(db *DataBaseImpl, c commoninterface.Connection) bool {
	watching := c.GetWatching()
	for key, version := range watching {
		if v, ok := db.GetVersion(key); ok && v != version {
			return true
		}
	}
	return false
}
func EnQueue(c commoninterface.Connection, cmdLine [][]byte) (reply resp.Reply) {
	if !c.InMultiState() {
		return resp.MakeMultiErrReply()
	}
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdContainer[cmdName]
	if !ok {
		e := resp.MakeErrReply("ERR unknown command '" + cmdName + "'")
		c.AddTxError(e)
		return e
	}
	if cmd.prepare == nil {
		e := resp.MakeErrReply("ERR command '" + cmdName + "' cannot be used in MULTI")
		c.AddTxError(e)
		return e
	}
	c.EnqueueCmd(cmdLine)
	return resp.MakeQueuedReply()
}
func execMulti(dbi *DataBaseImpl, c commoninterface.Connection) resp.Reply {
	if !c.InMultiState() {
		return resp.MakeMultiErrReply()
	}
	defer c.SetMultiState(false)
	if len(c.GetTxErrors()) > 0 {
		return resp.MakeErrReply("ERR EXECABORT Transaction discarded because of previous errors.")
	}
	cmdLines := c.GetQueuedCmdLine()
	return ExecMulti(dbi, c, c.GetWatching(), cmdLines)

}

func ExecMulti(dbi *DataBaseImpl, c commoninterface.Connection, watching map[string]uint32, cmdLines []cm.CmdLine) resp.Reply {
	if watchChanged(dbi, c) {
		return resp.MakeEmptyMultiBulkReply()
	}
	// prepare
	wkeys := make([]string, 0)
	rkeys := make([]string, 0)
	for _, cmdLine := range cmdLines {
		cmdName := strings.ToLower(string(cmdLine[0]))
		cmd, _ := cmdContainer[cmdName]
		if cmd.prepare != nil {
			wkey, rkey := cmd.prepare(cmdLine)
			wkeys = append(wkeys, wkey...)
			rkeys = append(rkeys, rkey...)
		}
	}
	watchIngKeys := make([]string, 0, len(watching))
	for key := range watching {
		watchIngKeys = append(watchIngKeys, key)
	}
	rkeys = append(rkeys, watchIngKeys...)
	// lock
	defer dbi.RWUnLocks(wkeys, rkeys)
	dbi.RWLocks(wkeys, rkeys)
	// exec
	if watchChanged(dbi, c) {
		return resp.MakeEmptyMultiBulkReply()
	}
	replies := make([]resp.Reply, 0, len(cmdLines))
	aborted := false
	undoCmdLines := make([][]cm.CmdLine, 0, len(cmdLines))
	for _, cmdLine := range cmdLines {
		undoCmdLines = append(undoCmdLines, GetUndoLogs(dbi, cmdLine))
		reply := dbi.ExecWithLock(cmdLine) //commit
		isErrorReply := resp.IsErrorReply(reply)
		if isErrorReply {
			aborted = true
			undoCmdLines = undoCmdLines[:len(undoCmdLines)-1]
			break
		}
		replies = append(replies, reply)
	}
	if aborted {
		//rollback
		for i := len(undoCmdLines) - 1; i >= 0; i-- {
			for _, undoCmdLine := range undoCmdLines[i] {
				dbi.ExecWithLock(undoCmdLine)
			}
		}
		return resp.MakeErrReply("ERR EXECABORT Transaction discarded because of previous errors.")
	}
	dbi.SetVersion(wkeys...)
	return resp.MakeMultiRawReply(replies...)
}
func GetUndoLogs(dbi *DataBaseImpl, line cm.CmdLine) []cm.CmdLine {
	cmdName := strings.ToLower(string(line[0]))
	cmd, _ := cmdContainer[cmdName]
	if cmd.undo == nil {
		return nil
	}
	return cmd.undo(dbi, line)
}
