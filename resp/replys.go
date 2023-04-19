package resp

import (
	"bytes"
	"strconv"
)

const CRLF = "\r\n"

type BulkReply struct {
	Arg []byte
}
type StandardErrReply struct {
	Status string
}
type MultiBulkReply struct {
	Args [][]byte
}
type MultiRawReply struct {
	replies []Reply
}
type SimpleStringReply struct {
	SimpleString string
}

func MakeSimpleStringReply(status string) *SimpleStringReply {
	return &SimpleStringReply{
		SimpleString: status,
	}
}
func (r *SimpleStringReply) ToBytes() []byte {
	return []byte("+" + r.SimpleString + CRLF)
}

func (m MultiRawReply) ToBytes() []byte {
	argLen := len(m.replies)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range m.replies {
		buf.Write(arg.ToBytes())
	}
	return buf.Bytes()
}

func MakeMultiRawReply(replies ...Reply) Reply {
	return &MultiRawReply{
		replies: replies,
	}
}
func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{
		Arg: arg,
	}
}
func MakeErrReply(status string) *StandardErrReply {
	return &StandardErrReply{
		Status: status,
	}
}

type IntReply struct {
	Code int64
}

func MakeIntReply(code int64) *IntReply {
	return &IntReply{
		Code: code,
	}
}

func (r *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}
func MakeMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{
		Args: args,
	}
}
func (sde *StandardErrReply) ToBytes() []byte {
	return []byte("-" + sde.Status + CRLF)
}
func (sde *StandardErrReply) Error() string {
	return sde.Status
}
func (b *BulkReply) ToBytes() []byte {
	if b.Arg == nil {
		return MakeNullBulkReply().ToBytes()
	}
	return []byte("$" + strconv.Itoa(len(b.Arg)) + CRLF + string(b.Arg) + CRLF)
}
func (mb *MultiBulkReply) ToBytes() []byte {
	argLen := len(mb.Args)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range mb.Args {
		if arg == nil {
			buf.WriteString("$-1" + CRLF)
		} else {
			buf.WriteString("$" + strconv.Itoa(len(arg)) + CRLF + string(arg) + CRLF)
		}
	}
	return buf.Bytes()
}
func IsErrorReply(reply Reply) bool {
	return reply.ToBytes()[0] == '-'
}
