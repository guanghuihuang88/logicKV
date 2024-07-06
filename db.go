package logic_kv

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	"github.com/guanghuihuang88/logicKV/data"
	"github.com/guanghuihuang88/logicKV/fio"
	"github.com/guanghuihuang88/logicKV/index"
	"github.com/guanghuihuang88/logicKV/utils"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const seqNoKey = "seq.no"
const fileLockName = "flock"

// DB bitcask 存储引擎实例
type DB struct {
	options        Options
	mu             *sync.RWMutex
	fileIds        []int                     // 文件id的集合，只有在加载索引的时候会用到
	activeFile     *data.DataFile            // 当前活跃的数据文件，可以用于写入
	olderFiles     map[uint32]*data.DataFile // 旧的数据文件，只能用于读
	index          index.Indexer             // 内存索引
	seqNo          uint64                    // 事务id
	isMerging      bool                      // 是否正在进行Merge
	seqNoFileExist bool                      // 存储事务序列号的文件是否存在
	isInitial      bool                      // 是否是第一次初始化此数据目录
	fileLock       *flock.Flock              // 文件锁，保证多个进程间互斥
	byteWrite      uint                      // 当前累计写了多少字节
	reclaimSize    int64                     // 表示有多少数据是无效的
}

// Stat 存储引擎统计信息
type Stat struct {
	KeyNum          uint  // Key 的总数量
	DataFileNum     uint  // 数据文件数量
	ReclaimableSize int64 // merge 可以回收的数据量（字节为单位）
	DiskSize        int64 // 数据目录所占磁盘空间大小
}

// Open 打开 bitcask 存储引擎实例
func Open(options Options) (*DB, error) {
	// 对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	var isInitial bool
	// 判断数据目录是否存在，如果不存在，则创建这个目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isInitial = true
	}

	// 初始化 DB 实例结构体
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites),
		isInitial:  isInitial,
		fileLock:   fileLock,
	}

	// 加载 merge 数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// 加在数据文件
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}

	if options.IndexType != BPTree {
		// 从 hint 索引文件中加载索引
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}

		// 从数据文件中加载索引
		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}

		// 重置 IO 类型为标准文件 IO
		if db.options.MMapAtStartup {
			if err := db.resetIoType(); err != nil {
				return nil, err
			}
		}
	} else {
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}
		if db.activeFile != nil {
			size, err := db.activeFile.IoManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOffset = size
		}
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
	if oldPos := db.index.Put(key, appendLogRecordPos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}

	return nil
}

// Get 根据 key 读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
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

	return logRecord.Value, nil
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
	pos, err := db.appendLogRecordWithLock(deleteLogRecord)
	if err != nil {
		return err
	}
	db.reclaimSize += int64(pos.Size)

	// 更新内存索引
	oldPos, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
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

	db.byteWrite += uint(size)
	// 根据用户配置决定是否持久化
	var needSync = db.options.SyncWrites
	if !needSync && db.options.BytesPerSync > 0 && db.byteWrite >= db.options.BytesPerSync {
		needSync = true
	}
	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// 清空累计值
		if db.byteWrite > 0 {
			db.byteWrite = 0
		}
	}

	// 构造内存索引信息
	pos := &data.LogRecordPos{FileId: db.activeFile.FileId, Offset: writeOffset, Size: uint32(size)}
	return pos, nil
}

func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}
	// 打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId, fio.StandardFIO)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// 从磁盘中加载数据文件
func (db *DB) loadDataFile() error {
	// 打开DB实例目录
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	// 将目录中所有.data文件的文件id添加到[]int数组
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

	// 遍历文件id，为每个文件id实例化一个dataFile对象，添加到历史文件列表中，其中最后一个文件id，标记为活跃文件
	for index, fileId := range fileIds {
		ioType := fio.StandardFIO
		if db.options.MMapAtStartup {
			ioType = fio.MemoryMap
		}
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fileId), ioType)
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
		var oldPos *data.LogRecordPos

		// 若待更新记录类型是墓碑记录，则删除索引中对应的数据
		if typ == data.LogRecordDeleted {
			oldPos, _ = db.index.Delete(key)
			db.reclaimSize += int64(pos.Size) // 更新无效记录总大小
		} else {
			oldPos = db.index.Put(key, pos) // 若待更新记录没有旧记录，则oldPos = nil
		}
		if oldPos != nil {
			db.reclaimSize += int64(pos.Size) // 若待更新记录有旧记录，则更新无效记录总大小
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

			pos := &data.LogRecordPos{FileId: fileId, Offset: offset, Size: uint32(size)}

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
	if options.DataFileMaergeRatio < 0 || options.DataFileMaergeRatio > 1 {
		return errors.New("invaild merge ratio")
	}
	return nil
}

func (db *DB) Sync() error {
	db.mu.RLock()
	db.mu.RUnlock()
	return db.activeFile.Sync()
}

// Stat 返回数据库相关统计信息
func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var dataFiles = uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFiles += 1
	}

	dirSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size: %v", err))
	}
	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFileNum:     dataFiles,
		ReclaimableSize: db.reclaimSize,
		DiskSize:        dirSize,
	}
}

// Backup 备份数据库
func (db *DB) Backup(dir string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return utils.CopyDir(db.options.DirPath, dir, []string{fileLockName})
}

func (db *DB) Close() error {
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock the directory, %v", err))
		}
	}()

	if db.activeFile == nil {
		return nil
	}
	db.mu.RLock()
	db.mu.RUnlock()

	db.index.Close()

	// 保存当前事务序列号
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
		Type:  0,
	}
	encodeSeqNoRecord, _ := data.EncodeLogRecord(record)
	if err := seqNoFile.Write(encodeSeqNoRecord); err != nil {
		return err
	}
	if err = seqNoFile.Sync(); err != nil {
		return err
	}

	// 关闭当前活跃文件
	err = db.activeFile.Close()
	if err != nil {
		return err
	}

	// 关闭旧的数据文件
	for _, dataFile := range db.olderFiles {
		err := dataFile.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// ListKeys 获取数据库中所有的 key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	defer iterator.Close()
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
	defer iterator.Close()
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

func (db *DB) Iterator(options IteratorOptions) *DbIterator {
	if db == nil {
		return nil
	}
	return &DbIterator{
		db:        db,
		indexIter: db.index.Iterator(options.Reverse),
		options:   options,
	}

}

// DbIterator 索引迭代器
type DbIterator struct {
	db        *DB
	indexIter index.Iterator
	options   IteratorOptions
}

// Rewind 重新回到迭代器的起点
func (iter *DbIterator) Rewind() {
	iter.indexIter.Rewind()
	iter.skipToPrix()
}

// Seek 根据传入的 key 查找到第一个 >=/<= key 的目标 key，从这个 key 开始遍历
func (iter *DbIterator) Seek(key []byte) {
	iter.indexIter.Seek(key)
	iter.skipToPrix()
}

// Next 跳转到下一个 key
func (iter *DbIterator) Next() {
	iter.indexIter.Next()
	iter.skipToPrix()
}

// Valid 是否有效，即是否遍历完所有的 key，用于退出遍历
func (iter *DbIterator) Valid() bool {
	return iter.indexIter.Valid()
}

// Key 当前遍历位置的 key 数据
func (iter *DbIterator) Key() []byte {
	return iter.indexIter.Key()
}

// Value 当前遍历位置的 value 数据
func (iter *DbIterator) Value() ([]byte, error) {
	logRecordPos := iter.indexIter.Value()
	iter.db.mu.RLock()
	defer iter.db.mu.RUnlock()

	return iter.db.getValueByPos(logRecordPos)
}

// Close 关闭迭代器，释放资源
func (iter *DbIterator) Close() {
	iter.indexIter.Close()
}

// skipToPrix 调到指定前缀位置
func (iter *DbIterator) skipToPrix() {
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
func (db *DB) loadSeqNo() error {
	filename := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return err
	}
	seqNoFile, err := data.OpenSeqNoFile(filename)
	if err != nil {
		return err
	}
	seqNoRecord, _, err := seqNoFile.ReadLogRecord(0)
	if err != nil {
		return err
	}
	seqNo, err := strconv.ParseUint(string(seqNoRecord.Value), 10, 64)
	if err != nil {
		return err
	}
	db.seqNo = seqNo
	db.seqNoFileExist = true
	return os.Remove(filename)
}

// 将数据文件的 IO 类型设置为标准文件 IO
func (db *DB) resetIoType() error {
	if db.activeFile == nil {
		return nil
	}

	if err := db.activeFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
		return err
	}
	for _, dataFile := range db.olderFiles {
		if err := dataFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
			return err
		}
	}
	return nil
}
