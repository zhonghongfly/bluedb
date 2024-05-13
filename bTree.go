package bluedb

import "encoding/binary"

// BNode
// | type | nkeys |  pointers  |   offsets  | key-values
// |  2B  |   2B  | nkeys * 8B | nkeys * 2B | ...
// This is the format of the KV pair. Lengths followed by data.
// | klen | vlen | key | val |
// |  2B  |  2B  | ... | ... |
type BNode struct {
	data []byte
}

type BTree struct {
	// 根节点指针
	root uint64

	// 获取BNode
	get func(uint64) BNode
	// 分配一个新页
	new func(BNode) uint64
	// 删除页
	del func(BNode)
}

// data struct config
const (
	dataTypeLen = 2
	keyCountLen = 2
	pointerLen  = 8
	offsetLen   = 2
	kLen        = 2
	vLen        = 2
	kvHeadLen   = kLen + vLen
)

// 页配置
const (
	HEADER           = 4
	BtreePageSize    = 4096
	BtreeMaxKeyLen   = 1000
	BtreeMaxValueLen = 3000
)

func init() {
	nodeMax := HEADER + pointerLen + offsetLen + kvHeadLen + BtreeMaxKeyLen + BtreeMaxValueLen
	assert(nodeMax <= BtreePageSize)
}

func assert(expression bool) {
	if !expression {
		panic("校验不通过")
	}
}

// header
func (node *BNode) dataType() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}

func (node *BNode) keyCount() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node *BNode) setHeader(dataType uint16, keyCount uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], dataType)
	binary.LittleEndian.PutUint16(node.data[2:4], keyCount)
}

// pointer
func (node *BNode) getPointer(idNum uint16) uint64 {
	assert(idNum < node.keyCount())
	pos := HEADER + 8*idNum
	return binary.LittleEndian.Uint64(node.data[pos:])
}

func (node *BNode) setPointer(idNum uint16, val uint64) {
	assert(idNum < node.keyCount())
	pos := HEADER + 8*idNum
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

// offset
func (node *BNode) getOffset(idNum uint16) uint16 {
	if idNum == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.data[offsetPos(node, idNum):])
}

func offsetPos(node *BNode, idNum uint16) uint16 {
	keyCount := node.keyCount()
	assert(idNum >= 1 && idNum <= keyCount)
	return HEADER + pointerLen*keyCount + offsetLen*(idNum-1)
}

// key-value
func (node *BNode) kvPos(idNum uint16) uint16 {
	keyCount := node.keyCount()
	assert(idNum <= keyCount)
	return HEADER + pointerLen*keyCount + offsetLen*keyCount + node.getOffset(idNum)
}

func (node *BNode) getKeyLen(idNum uint16) uint16 {
	pos := node.kvPos(idNum)
	return binary.LittleEndian.Uint16(node.data[pos:])
}

func (node *BNode) getValueLen(idNum uint16) uint16 {
	pos := node.kvPos(idNum)
	return binary.LittleEndian.Uint16(node.data[pos+kLen:])
}

func (node *BNode) getKey(idNum uint16) []byte {
	assert(idNum < node.keyCount())
	pos := node.kvPos(idNum)
	keyLen := node.getKeyLen(idNum)
	start := pos + kvHeadLen
	end := start + keyLen
	return node.data[start:end]
}

func (node *BNode) getValue(idNum uint16) []byte {
	assert(idNum < node.keyCount())
	pos := node.kvPos(idNum)
	keyLen := node.getKeyLen(idNum)
	valueLen := node.getValueLen(idNum)
	start := pos + kvHeadLen + keyLen
	end := start + valueLen
	return node.data[start:end]
}

// node size of byte
func (node *BNode) nodeBytes() uint16 {
	return node.kvPos(node.keyCount())
}
