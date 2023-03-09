package bitmap

type BitMap []byte
type Callback func(offset int64, val byte) bool

func NewBitMap() *BitMap {
	b := BitMap(make([]byte, 0))
	return &b
}

func toByteSize(bitSize int64) int64 {
	if bitSize%8 == 0 {
		return bitSize / 8
	}
	return bitSize/8 + 1
}
func (b *BitMap) grow(bitSize int64) {
	byteSize := toByteSize(bitSize)
	gap := byteSize - int64(len(*b))
	if gap <= 0 { // do nothing
		return
	}
	*b = append(*b, make([]byte, gap)...)
}
func (b *BitMap) BitSize() int {
	return len(*b) * 8
}
func FromBytes(bytes []byte) *BitMap {
	bm := BitMap(bytes)
	return &bm
}
func (b *BitMap) ToBytes() []byte {
	return *b
}
func (b *BitMap) SetBit(offset int64, val byte) {
	byteIndex := offset / 8
	bitOffset := offset % 8
	mask := byte(1 << bitOffset)
	b.grow(offset + 1)
	if val > 0 {
		(*b)[byteIndex] |= mask
	} else {
		(*b)[byteIndex] &^= mask
	}
}
func (b *BitMap) GetBit(offset int64) byte {
	byteIndex := offset / 8
	bitOffset := offset % 8
	if byteIndex >= int64(len(*b)) {
		return 0
	}
	return ((*b)[byteIndex] >> bitOffset) & 0x01
}
func (b *BitMap) ForEachBit(begin int64, end int64, cb Callback) {
	offset := begin
	for offset < end {
		if !cb(offset, b.GetBit(offset)) {
			break
		}
		offset++
	}
}
func (b *BitMap) ForEachByte(begin int64, end int64, cb Callback) {
	if end == 0 || end > int64(len(*b)) {
		end = int64(len(*b))
	}
	offset := begin
	for offset < end {
		if !cb(offset, (*b)[offset]) {
			break
		}
		offset++
	}
}
