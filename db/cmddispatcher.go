package db

var cmds = make(map[string]*command)

const (
	Write = iota
	ReadOnly
)

type command struct {
	executor ExecFunc
	prepare  PreFunc // return related keys command
	undo     UndoFunc
	arity    int // allow number of args, arity < 0 means len(args) >= -arity
	flags    int
}

func RegisterCommand(name string, executor ExecFunc, prepare PreFunc, rollback UndoFunc, arity int, flags int) {
	cmds[name] = &command{
		executor: executor,
		prepare:  prepare,
		undo:     rollback,
		arity:    arity,
		flags:    flags,
	}
}
func isReadOnly(name string) bool {
	cmd := cmds[name]
	if cmd == nil {
		return false
	}
	return cmd.flags&ReadOnly > 0
}
