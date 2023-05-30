package logic_kv

import (
	"encoding/binary"
	"github.com/guanghuihuang88/logicKV/data"
	"sync"
	"sync/atomic"
)

const nonTxnSeqNo uint64 = 0

var txnFinKey = []byte("txn-fin")

// WriteBatch 原子批量写数据，保证事务的原子性
type WriteBatch struct {
	options       WriteBatchOptions
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord // 暂存用户写入的数据
}

// NewWriteBatch 新建批量写操作
func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	if db.options.IndexType == BPTree && !db.seqNoFileExist && !db.isInitial {
		panic("cannot use write batch, seqNo not exist")
	}
	return &WriteBatch{
		options:       opts,
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put 添加数据
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
	}

	// 暂存记录
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Delete 删除数据
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 若db中还没有该数据：
	// 1. 暂存记录中有该数据，则从暂存记录中删除
	// 2. 否则，直接返回
	if wb.db.index.Get(key) == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}
	// 暂存记录
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Commit 提交数据
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}

	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatch
	}

	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	positions := make(map[string]*data.LogRecordPos)
	for _, record := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   LogRecordKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		positions[string(record.Key)] = logRecordPos
	}

	// 写一条标识事务完成的数据
	finishedRecord := &data.LogRecord{
		Key:  LogRecordKeyWithSeq(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinished,
	}
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	// 根据配置决定是否持久化
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新内存索引
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		if record.Type == data.LogRecordNormal {
			wb.db.index.Put(record.Key, pos)
		}
		if record.Type == data.LogRecordDeleted {
			wb.db.index.Delete(record.Key)
		}
	}

	// 清空暂存数据
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

// LogRecordKeyWithSeq key+seqNo 编码
func LogRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	enc := make([]byte, len(key)+n)
	copy(enc[:n], seq[:n])
	copy(enc[n:], key)

	return enc
}

// ParseLogRecordKey key+seqNo 解码
func ParseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
