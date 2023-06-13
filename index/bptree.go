package index

import (
	"github.com/guanghuihuang88/logicKV/data"
	"go.etcd.io/bbolt"
	"path/filepath"
)

const bptreeIndexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

// BPlusTree B+树索引
// 磁盘索引、将索引存储在磁盘上，需要一个文件路径作为参数
// 主要封装了 go.etcd.io/bbolt 库
type BPlusTree struct {
	tree *bbolt.DB
}

func NewBPlusTree(dirPath string, syncWrites bool) *BPlusTree {
	defaultOptions := bbolt.DefaultOptions
	defaultOptions.NoSync = !syncWrites
	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, defaultOptions)
	if err != nil {
		panic("failed to open bptree")
	}

	// 创建对应的bucket
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create bucket in bptree")
	}

	return &BPlusTree{tree: bptree}
}

func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	var oldValue []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		oldValue = bucket.Get(key)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("failed to put value in bptree")
	}
	if len(oldValue) == 0 {
		return nil
	}
	return data.DecodeLogRecordPos(oldValue)
}

func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var logRecordPos *data.LogRecordPos
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			logRecordPos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		panic("failed to get value in bptree")
	}
	return logRecordPos
}

func (bpt *BPlusTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	var ok bool
	var oldValue []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if value := bucket.Get(key); len(value) != 0 {
			ok = true
			oldValue = value
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete value in bptree")
	}
	if len(oldValue) == 0 {
		return nil, ok
	}
	return data.DecodeLogRecordPos(oldValue), ok
}

// Iterator 返回迭代器
func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBptIterator(bpt.tree, reverse)
}

// Size 索引长度
func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get size of bptree")
	}
	return size
}

// Close 关闭索引
func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

// BPlusTree 索引迭代器
type bptIterator struct {
	tx        *bbolt.Tx
	cursor    *bbolt.Cursor
	reverse   bool
	currKey   []byte
	currValue []byte
}

func newBptIterator(tree *bbolt.DB, reverse bool) *bptIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed begin a transaction")
	}

	bpt := &bptIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	bpt.Rewind()
	return bpt
}

// Next 跳转到下一个 key
func (iter *bptIterator) Next() {
	if iter.reverse {
		iter.currKey, iter.currValue = iter.cursor.Prev()
	} else {
		iter.currKey, iter.currValue = iter.cursor.Next()
	}
}

// Rewind 重新回到迭代器的起点
func (iter *bptIterator) Rewind() {
	if iter.reverse {
		iter.currKey, iter.currValue = iter.cursor.Last()
	} else {
		iter.currKey, iter.currValue = iter.cursor.First()
	}
}

// Seek 根据传入的 key 查找到第一个 >=/<= key 的目标 key，从这个 key 开始遍历
func (iter *bptIterator) Seek(key []byte) {
	iter.currKey, iter.currValue = iter.cursor.Seek(key)
}

// Valid 是否有效，即是否遍历完所有的 key，用于退出遍历
func (iter *bptIterator) Valid() bool {
	return len(iter.Key()) != 0
}

// Key 当前遍历位置的 key 数据
func (iter *bptIterator) Key() []byte {
	return iter.currKey
}

// Value 当前遍历位置的 value 数据
func (iter *bptIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(iter.currValue)
}

// Close 关闭迭代器，释放资源
func (iter *bptIterator) Close() {
	_ = iter.tx.Rollback()
}
