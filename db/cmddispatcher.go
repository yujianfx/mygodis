package db

import cm "mygodis/common"

var cmdContainer = make(map[string]*Command)

const (
	Write = iota
	ReadOnly
)

type Command struct {
	executor ExecFunc
	prepare  PreFunc // return related keys Command
	undo     UndoFunc
	arity    int // allow number of args, arity < 0 means len(args) >= -arity
	flags    int
}

func GetCommand(line cm.CmdLine) (*Command, bool) {
	c, ok := cmdContainer[string(line[0])]
	return c, ok

}
func RegisterCommand(name string, executor ExecFunc, prepare PreFunc, rollback UndoFunc, arity int, flags int) {
	cmdContainer[name] = &Command{
		executor: executor,
		prepare:  prepare,
		undo:     rollback,
		arity:    arity,
		flags:    flags,
	}
}
func isReadOnly(name string) bool {
	cmd := cmdContainer[name]
	if cmd == nil {
		return false
	}
	return cmd.flags&ReadOnly > 0
}
