package resp

const (
	ok = iota
	pong
	nullBulk
	emptyMultiBulk
	queued
	noReply
)

var constMap = map[int]Reply{
	ok: &OkReply{
		ok: []byte("+OK\r\n"),
	},
	pong: &PongReply{
		pong: []byte("+PONG\r\n"),
	},
	nullBulk: &NullBulkReply{
		nbr: []byte("$-1\r\n"),
	},
	emptyMultiBulk: &EmptyMultiBulkReply{
		emb: []byte("*0\r\n"),
	},
	queued: &QueuedReply{
		qr: []byte("+QUEUED\r\n"),
	},
	noReply: &NoReply{
		nr: []byte(""), // no reply
	},
}

type PongReply struct {
	pong []byte
}

func (r *PongReply) ToBytes() []byte {
	return r.pong
}

type OkReply struct{ ok []byte }

func (r *OkReply) ToBytes() []byte {
	return r.ok
}

func MakeOkReply() *OkReply {
	return constMap[ok].(*OkReply)
}

type NullBulkReply struct {
	nbr []byte
}

func (r *NullBulkReply) ToBytes() []byte {
	return r.nbr
}
func MakeNullBulkReply() *NullBulkReply {
	return constMap[nullBulk].(*NullBulkReply)
}

type EmptyMultiBulkReply struct {
	emb []byte
}

func (r *EmptyMultiBulkReply) ToBytes() []byte {
	return r.emb
}
func MakeEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return constMap[emptyMultiBulk].(*EmptyMultiBulkReply)
}

type NoReply struct {
	nr []byte
}

func (r *NoReply) ToBytes() []byte {
	return r.nr
}

type QueuedReply struct {
	qr []byte
}

func (r *QueuedReply) ToBytes() []byte {
	return r.qr
}
func MakeQueuedReply() *QueuedReply {
	return constMap[queued].(*QueuedReply)
}
