package logic_kv

import (
	"bytes"
	"errors"
	"github.com/guanghuihuang88/logicKV/data"
	"github.com/guanghuihuang88/logicKV/index"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB bitcask 存储引擎实例
type DB struct {
	options    Options
	mu         *sync.RWMutex
	fileIds    []int                     // 文件id的集合，只有在加载索引的时候会用到
	activeFile *data.DataFile            // 当前活跃的数据文件，可以用于写入
	olderFiles map[uint32]*data.DataFile // 旧的数据文件，只能用于读
	index      index.Indexer             // 内存索引
	seqNo      uint64                    // 事务id
	isMerging  bool                      // 是否正在进行Merge
}

// Open 打开 bitcask 存储引擎实例
func Open(options Options) (*DB, error) {
	// 对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// 判断数据目录是否存在，如果不存在，则创建这个目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 初始化 DB 实例结构体
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}

	// 加载 merge 数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// 从 hint 索引文件中加载索引
	if err := db.loadIndexFromHintFile(); err != nil {
		return nil, err
	}

	// 加在数据文件
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}

	// 加载索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

// Put 写入 key/value 数据，key 不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// 判断 key 是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体
	logRecord := &data.LogRecord{Key: LogRecordKeyWithSeq(key, nonTxnSeqNo), Value: value, Type: data.LogRecordNormal}
	// 追加写入到当前活跃文件中
	appendLogRecordPos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引
	if ok := db.index.Put(key, appendLogRecordPos); !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

// Get 根据 key 读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyIsEmpty
	}

	return db.getValueByPos(logRecordPos)
}

// 根据 pos 位置获取数据
func (db *DB) getValueByPos(pos *data.LogRecordPos) ([]byte, error) {
	var dataFile *data.DataFile
	if db.activeFile.FileId == pos.FileId {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[pos.FileId]
	}

	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	logRecord, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyIsEmpty
	}

	return logRecord.Key, nil
}

func (db *DB) Delete(key []byte) error {
	// 判断 key 是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// 判断 key 是否存在
	recordPos := db.index.Get(key)
	if recordPos == nil {
		return nil
	}
	// 删除 key
	deleteLogRecord := &data.LogRecord{Key: LogRecordKeyWithSeq(key, nonTxnSeqNo), Type: data.LogRecordDeleted}
	// 追加写入到当前活跃文件中
	if _, err := db.appendLogRecordWithLock(deleteLogRecord); err != nil {
		return err
	}

	// 更新内存索引
	if ok := db.index.Delete(key); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// 追加写数据到活跃文件中（加锁）
func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.appendLogRecord(logRecord)
}

// 追加写数据到活跃文件中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	// 判断当前活跃数据文件是否存在，因为数据库在没有写入的时候是没有文件生成的
	// 如果为空则初始化数据文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 写入数据编码
	encodeLogRecord, size := data.EncodeLogRecord(logRecord)
	// 如果写入数据已经达到了活跃文件的阈值，则关闭活跃文件，打开新的活跃文件
	if db.activeFile.WriteOffset+size > db.options.DataFileSize {
		// 先持久化数据文件，保证已有的数据持久到磁盘中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 当前活跃文件转换为旧的数据文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// 打开新的活跃文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	writeOffset := db.activeFile.WriteOffset
	// 写入数据
	if err := db.activeFile.Write(encodeLogRecord); err != nil {
		return nil, err
	}
	// 根据用户配置决定是否持久化
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}
	// 构造内存索引信息
	pos := &data.LogRecordPos{FileId: db.activeFile.FileId, Offset: writeOffset}
	return pos, nil
}

func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}
	// 打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// 从磁盘中加载数据文件
func (db *DB) loadDataFile() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			fileName := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(fileName[0])
			// 数据目录可能被损坏了
			if err != nil {
				return ErrDataDirCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	// 对文件id进行排序
	sort.Ints(fileIds)
	db.fileIds = fileIds

	for index, fileId := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fileId))
		if err != nil {
			return err
		}
		if index == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.olderFiles[uint32(fileId)] = dataFile
		}
	}
	return nil
}

// 从数据文件加载索引
// 遍历数据文件中的内容，更新到索引中
func (db *DB) loadIndexFromDataFiles() error {
	// 若数据文件列表为空，则数据库是空的，直接返回
	if len(db.fileIds) == 0 {
		return nil
	}

	// 查看是否发生过 merge
	hasMerged, nonMergeFileId := false, uint32(0)
	mergeFinishedFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinishedFileName); err == nil {
		fileId, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerged = true
		nonMergeFileId = fileId
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool
		if typ == data.LogRecordDeleted {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}
		if !ok {
			panic("failed to update index at startup")
		}
	}

	// 暂存事务数据
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	currentTxnSeqNo := nonTxnSeqNo

	// 遍历所有的文件id，处理文件中的数据
	for index, id := range db.fileIds {
		var dataFile *data.DataFile
		var fileId = uint32(id)

		// 若 id 比最近未参与 merge 的文件 id 更小，则跳过
		if hasMerged && fileId < nonMergeFileId {
			continue
		}

		if db.activeFile.FileId == fileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		offset := int64(0)
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				} else {
					return err
				}
			}

			pos := &data.LogRecordPos{FileId: fileId, Offset: offset}

			// 解析 key，事务id
			realKey, seqNo := ParseLogRecordKey(logRecord.Key)

			// 更新索引
			if seqNo == nonTxnSeqNo {
				// 非事务操作，直接更新内存索引
				updateIndex(realKey, logRecord.Type, pos)
			} else {
				// 事务完成，对应 seqNo 的数据可以更新到内存索引
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    pos,
					})
				}
			}

			// 更新事务id
			if seqNo > currentTxnSeqNo {
				currentTxnSeqNo = seqNo
			}

			// 递增 offset，下一次从新的位置开始读取
			offset += size
		}

		if index == len(db.fileIds)-1 {
			db.activeFile.WriteOffset = offset
		}
	}

	// 更新事务id
	db.seqNo = currentTxnSeqNo
	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must greater than 0")
	}
	return nil
}

func (db *DB) Sync() error {
	db.mu.RLock()
	db.mu.RUnlock()
	return db.activeFile.Sync()
}

func (db *DB) Close() error {
	db.mu.RLock()
	db.mu.RUnlock()
	err := db.activeFile.Close()
	if err != nil {
		return err
	}
	for _, dataFile := range db.olderFiles {
		err := dataFile.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// DB 索引迭代器
type dbIterator struct {
	db        *DB
	indexIter index.Iterator
	options   IteratorOptions
}

// ListKeys 获取数据库中所有的 key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	list := make([][]byte, db.index.Size())
	idx := 0
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		list[idx] = iterator.Key()
		idx += 1
	}
	return list
}

// Fold 获取所有数据，并执行用户指定操作，返回 false 时终止
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	db.mu.RUnlock()
	iterator := db.index.Iterator(false)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPos(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

func (db *DB) Iterator(options IteratorOptions) *dbIterator {
	if db == nil {
		return nil
	}
	return &dbIterator{
		db:        db,
		indexIter: db.index.Iterator(options.Reverse),
		options:   options,
	}

}

// Rewind 重新回到迭代器的起点
func (iter *dbIterator) Rewind() {
	iter.indexIter.Rewind()
	iter.skipToPrix()
}

// Seek 根据传入的 key 查找到第一个 >=/<= key 的目标 key，从这个 key 开始遍历
func (iter *dbIterator) Seek(key []byte) {
	iter.indexIter.Seek(key)
	iter.skipToPrix()
}

// Next 跳转到下一个 key
func (iter *dbIterator) Next() {
	iter.indexIter.Next()
	iter.skipToPrix()
}

// Valid 是否有效，即是否遍历完所有的 key，用于退出遍历
func (iter *dbIterator) Valid() bool {
	return iter.indexIter.Valid()
}

// Key 当前遍历位置的 key 数据
func (iter *dbIterator) Key() []byte {
	return iter.indexIter.Key()
}

// Value 当前遍历位置的 value 数据
func (iter *dbIterator) Value() ([]byte, error) {
	logRecordPos := iter.indexIter.Value()
	iter.db.mu.RLock()
	defer iter.db.mu.RUnlock()

	return iter.db.getValueByPos(logRecordPos)
}

// Close 关闭迭代器，释放资源
func (iter *dbIterator) Close() {
	iter.indexIter.Close()
}

// skipToPrix 调到指定前缀位置
func (iter *dbIterator) skipToPrix() {
	prefix := iter.options.Prefix
	if len(prefix) == 0 {
		return
	}

	for ; iter.indexIter.Valid(); iter.indexIter.Next() {
		if len(prefix) <= len(iter.indexIter.Key()) && bytes.Compare(iter.indexIter.Key()[:len(prefix)], prefix) == 0 {
			break
		}
	}
}
