package index

import (
	"bytes"
	"github.com/google/btree"
	"github.com/guanghuihuang88/logicKV/data"
)

type Indexer interface {
	// Put 向索引中存储 key 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos

	// Get 根据 key 取出对应的数据位置信息
	Get(key []byte) *data.LogRecordPos

	// Delete 根据 key 删除对应的数据位置信息
	Delete(key []byte) (*data.LogRecordPos, bool)

	// Iterator 返回迭代器
	Iterator(reverse bool) Iterator

	// Size 索引长度，key 个数
	Size() int

	// Close 关闭索引
	Close() error
}

type IndexType = int8

const (
	// BTree 索引
	Btree IndexType = iota + 1

	// ART 自适应基数树索引
	ART

	// BPTree B+树（磁盘索引、将索引存储在磁盘上，需要一个文件路径作为参数）
	BPTree
)

// NewIndexer 根据类型初始化索引
func NewIndexer(typ IndexType, dirPath string, syncWrites bool) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return NewArt()
	case BPTree:
		return NewBPlusTree(dirPath, syncWrites)
	default:
		panic("unsupported idnex type")
	}
}

type Item struct {
	Key []byte
	Pos *data.LogRecordPos
}

/*
	这里Less方法中参数bi的类型是btree.Item，btree.Item是接口，这意味着任何实现了btree.Item接口的结构体，都能存入btree
*/
func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.Key, bi.(*Item).Key) == -1
}

type Iterator interface {
	// Rewind 重新回到迭代器的起点
	Rewind()

	// Seek 根据传入的 key 查找到第一个 >=/<= key 的目标 key，从这个 key 开始遍历
	Seek([]byte)

	// Next 跳转到下一个 key
	Next()

	// Valid 是否有效，即是否遍历完所有的 key，用于退出遍历
	Valid() bool

	// Key 当前遍历位置的 key 数据
	Key() []byte

	// Value 当前遍历位置的 value 数据
	Value() *data.LogRecordPos

	// Close 关闭迭代器，释放资源
	Close()
}
