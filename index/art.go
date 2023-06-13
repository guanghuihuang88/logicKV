package index

import (
	"bytes"
	"github.com/guanghuihuang88/logicKV/data"
	goart "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

// AdaptiveRadixTree 自适应基数树
// 主要封装了 https//github.com/plar/go-adaptive-radix-tree 库
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

func NewArt() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	oldValue, _ := art.tree.Insert(key, pos)
	art.lock.Unlock()
	if oldValue == nil {
		return nil
	}
	return oldValue.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.Lock()
	defer art.lock.Unlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.Lock()
	defer art.lock.Unlock()
	oldValue, deleted := art.tree.Delete(key)
	if oldValue == nil {
		return nil, false
	}
	return oldValue.(*data.LogRecordPos), deleted
}

// Iterator 返回迭代器
func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	if art == nil {
		return nil
	}
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newArtIterator(art.tree, reverse)
}

// Size 索引长度
func (art *AdaptiveRadixTree) Size() int {
	art.lock.Lock()
	defer art.lock.Unlock()
	return art.tree.Size()
}

// Close 关闭索引
func (art *AdaptiveRadixTree) Close() error {
	return nil
}

// Art 索引迭代器
type artIterator struct {
	curIndex int
	reverse  bool
	items    []*Item
}

func newArtIterator(tree goart.Tree, reverse bool) *artIterator {
	var index = 0
	if reverse {
		index = tree.Size() - 1
	}

	values := make([]*Item, tree.Size())
	saveValues := func(node goart.Node) bool {
		item := &Item{
			Key: node.Key(),
			Pos: node.Value().(*data.LogRecordPos),
		}
		values[index] = item
		if reverse {
			index--
		} else {
			index++
		}
		return true
	}

	tree.ForEach(saveValues)

	return &artIterator{
		curIndex: 0,
		reverse:  reverse,
		items:    values,
	}
}

// Next 跳转到下一个 key
func (iter *artIterator) Next() {
	iter.curIndex += 1
}

// Rewind 重新回到迭代器的起点
func (iter *artIterator) Rewind() {
	iter.curIndex = 0
}

// Seek 根据传入的 key 查找到第一个 >=/<= key 的目标 key，从这个 key 开始遍历
func (iter *artIterator) Seek(key []byte) {
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
func (iter *artIterator) Valid() bool {
	return iter.curIndex < len(iter.items)
}

// Key 当前遍历位置的 key 数据
func (iter *artIterator) Key() []byte {
	return iter.items[iter.curIndex].Key
}

// Value 当前遍历位置的 value 数据
func (iter *artIterator) Value() *data.LogRecordPos {
	return iter.items[iter.curIndex].Pos
}

// Close 关闭迭代器，释放资源
func (iter *artIterator) Close() {
	iter.items = nil
}
