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

// 页配置
const (
	HEADER              = 4
	BTREE_PAGE_SIZE     = 4096
	BTREE_MAX_KEY_LEN   = 1000
	BTREE_MAX_VALUE_LEN = 3000
)

func init() {
	nodeMax := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_LEN + BTREE_MAX_VALUE_LEN
	assert(nodeMax <= BTREE_PAGE_SIZE)
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
	return binary.LittleEndian.Uint16()
}

func offsetPos(node BNode, idNum uint16) uint16 {
	keyCount := node.keyCount()
	assert(idNum >= 1 && idNum <= keyCount)
	return HEADER + 8*keyCount + 2*(idNum-1)
}
