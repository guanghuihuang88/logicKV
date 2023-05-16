package index

import (
	"bytes"
	"github.com/google/btree"
	"logicKV/data"
	"sort"
	"sync"
)

type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := Item{Key: key, Pos: pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(&it)
	bt.lock.Unlock()
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := Item{Key: key}
	btreeItem := bt.tree.Get(&it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).Pos
}

func (bt *BTree) Delete(key []byte) bool {
	it := Item{Key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(&it)
	bt.lock.Unlock()
	if oldItem == nil {
		return false
	}
	return true
}

// Iterator 返回迭代器
func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.tree, reverse)
}

// Size 索引长度
func (bt *BTree) Size() int {
	return bt.tree.Len()
}

// BTree 索引迭代器
type btreeIterator struct {
	curIndex int
	reverse  bool
	items    []*Item
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	items := make([]*Item, tree.Len())
	idx := 0

	saveItems := func(it btree.Item) bool {
		items[idx] = it.(*Item)
		idx++
		return true
	}
	if reverse {
		tree.Descend(saveItems)
	} else {
		tree.Ascend(saveItems)
	}

	return &btreeIterator{
		curIndex: 0,
		reverse:  reverse,
		items:    items,
	}
}

// Next 跳转到下一个 key
func (iter *btreeIterator) Next() {
	iter.curIndex += 1
}

// Rewind 重新回到迭代器的起点
func (iter *btreeIterator) Rewind() {
	iter.curIndex = 0
}

// Seek 根据传入的 key 查找到第一个 >=/<= key 的目标 key，从这个 key 开始遍历
func (iter *btreeIterator) Seek(key []byte) {
	if iter.reverse {
		iter.curIndex = sort.Search(len(iter.items), func(i int) bool {
			return bytes.Compare(iter.items[i].Key, key) <= 0
		})
	} else {
		iter.curIndex = sort.Search(len(iter.items), func(i int) bool {
			return bytes.Compare(iter.items[i].Key, key) >= 0
		})
	}
}

// Valid 是否有效，即是否遍历完所有的 key，用于退出遍历
func (iter *btreeIterator) Valid() bool {
	return iter.curIndex < len(iter.items)
}

// Key 当前遍历位置的 key 数据
func (iter *btreeIterator) Key() []byte {
	return iter.items[iter.curIndex].Key
}

// Value 当前遍历位置的 value 数据
func (iter *btreeIterator) Value() *data.LogRecordPos {
	return iter.items[iter.curIndex].Pos
}

// Close 关闭迭代器，释放资源
func (iter *btreeIterator) Close() {
	iter.items = nil
}
