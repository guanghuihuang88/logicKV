package index

import (
	"github.com/guanghuihuang88/logicKV/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()

	res := bt.Put(nil, &data.LogRecordPos{1, 100})
	assert.True(t, res)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{1, 100})
	assert.True(t, res1)

	res2 := bt.Get(nil)
	assert.Equal(t, uint32(1), res2.FileId)
	assert.Equal(t, int64(100), res2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{1, 100})
	assert.True(t, res1)

	res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Put([]byte("ss"), &data.LogRecordPos{2, 200})
	assert.True(t, res3)

	res4 := bt.Delete([]byte("ss"))
	assert.True(t, res4)
}

func TestBTree_Iterator(t *testing.T) {
	bt := NewBTree()
	iter := bt.Iterator(false)
	assert.Equal(t, false, iter.Valid())

	bt.Put([]byte("111"), &data.LogRecordPos{
		FileId: 1,
		Offset: 0,
	})
	iter = bt.Iterator(false)
	assert.Equal(t, true, iter.Valid())

	iter.Next()
	assert.Equal(t, false, iter.Valid())

	bt.Put([]byte("222"), &data.LogRecordPos{
		FileId: 1,
		Offset: 0,
	})
	bt.Put([]byte("333"), &data.LogRecordPos{
		FileId: 1,
		Offset: 0,
	})
	bt.Put([]byte("444"), &data.LogRecordPos{
		FileId: 1,
		Offset: 0,
	})
	iter = bt.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		t.Log(string(iter.Key()))
	}

	bt2 := NewBTree()
	bt2.Put([]byte("111"), &data.LogRecordPos{
		FileId: 1,
		Offset: 0,
	})
	bt2.Put([]byte("222"), &data.LogRecordPos{
		FileId: 1,
		Offset: 0,
	})
	bt2.Put([]byte("333"), &data.LogRecordPos{
		FileId: 1,
		Offset: 0,
	})
	bt2.Put([]byte("444"), &data.LogRecordPos{
		FileId: 1,
		Offset: 0,
	})
	iter = bt2.Iterator(true)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		t.Log(string(iter.Key()))
	}

	// 测试 seek
	iter = bt2.Iterator(true)
	for iter.Seek([]byte("33")); iter.Valid(); iter.Next() {
		t.Log(string(iter.Key()))
	}

}
