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
		ok: []byte("+OK" + CRLF),
	},
	pong: &PongReply{
		pong: []byte("+PONG" + CRLF),
	},
	nullBulk: &NullBulkReply{
		nbr: []byte("$-1" + CRLF),
	},
	emptyMultiBulk: &EmptyMultiBulkReply{
		emb: []byte("*0" + CRLF),
	},
	queued: &QueuedReply{
		qr: []byte("+QUEUED" + CRLF),
	},
	noReply: &NoReply{
		nr: []byte(""), // no reply
	},
}

type PongReply struct {
	pong []byte
}

type OkReply struct{ ok []byte }
type NullBulkReply struct {
	nbr []byte
}
type EmptyMultiBulkReply struct {
	emb []byte
}
type NoReply struct {
	nr []byte
}
type QueuedReply struct {
	qr []byte
}

func MakePongReply() *PongReply {
	return constMap[pong].(*PongReply)
}
func MakeOkReply() *OkReply {
	return constMap[ok].(*OkReply)
}
func MakeNullBulkReply() *NullBulkReply {
	return constMap[nullBulk].(*NullBulkReply)
}
func MakeEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return constMap[emptyMultiBulk].(*EmptyMultiBulkReply)
}
func MakeQueuedReply() *QueuedReply {
	return constMap[queued].(*QueuedReply)
}
func (r *NoReply) ToBytes() []byte {
	return r.nr
}

func (r *QueuedReply) ToBytes() []byte {
	return r.qr
}
func (r *EmptyMultiBulkReply) ToBytes() []byte {
	return r.emb
}
func (r *NullBulkReply) ToBytes() []byte {
	return r.nbr
}
func (r *PongReply) ToBytes() []byte {
	return r.pong
}
func (r *OkReply) ToBytes() []byte {
	return r.ok
}
