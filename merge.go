package logic_kv

import (
	"github.com/guanghuihuang88/logicKV/data"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDirName     = "-merge"
	mergeFinishedKey = "merge.finished"
)

// Merge 清理无效数据 生成 hint 文件
func (db *DB) Merge() error {
	// 若数据库为空
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	// 若正在进行 merge
	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeIsProgress
	}

	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	// 持久化当前活跃文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}

	// 将当前活跃文件转换为旧文件
	db.olderFiles[db.activeFile.FileId] = db.activeFile

	// 创建新的活跃文件
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return err
	}
	// 记录最近没有参与 merge 的文件 id
	nonMergeFileId := db.activeFile.FileId

	// 取出所有需要 merge 的文件
	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	db.mu.Unlock()

	// 待 merge 的文件从小到大排序，依次 merge
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	mergePath := db.getMergePath()
	// 如果目录存在，说明发生过 merge，将其删掉
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	// 新建一个 merge path 目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	// 打开一个新的临时 bitcask 实例
	mergeOptions := db.options
	mergeOptions.SyncWrites = false
	mergeOptions.DirPath = mergePath
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}

	// 打开 Hint 文件存储索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	// 遍历处理每一个数据文件
	var offset int64 = 0
	for _, dataFile := range mergeFiles {
		record, n, err := dataFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// 解析拿到实际的 key
		realKey, _ := ParseLogRecordKey(record.Key)
		recordPos := db.index.Get(realKey)

		// 和内存中索引位置进行比较，有效则重写
		if recordPos != nil &&
			dataFile.FileId == recordPos.FileId &&
			offset == recordPos.Offset {
			// 清除事务标记
			record.Key = LogRecordKeyWithSeq(realKey, nonTxnSeqNo)
			logRecordPos, err := mergeDB.appendLogRecord(record)
			if err != nil {
				return err
			}
			// 将当前位置索引写到 Hint 文件中
			if err = hintFile.WriteHintRecord(realKey, logRecordPos); err != nil {
				return err
			}
		}
		// 增加 offset
		offset += n
	}

	// 保证持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}

	// 写标识 merge 完成的文件
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	mergeFinishedRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}
	encodeLogRecord, _ := data.EncodeLogRecord(mergeFinishedRecord)

	if err := mergeFinishedFile.Write(encodeLogRecord); err != nil {
		return err
	}

	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}

	return nil
}

func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)
	return filepath.Join(dir, base+mergeDirName)
}

func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()

	// merge 目录不存在则直接返回
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return err
	}
	defer func() {
		os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// 判断 merge 是否处理完成
	var isMergeFinished = false
	var mergeFinishedNames []string
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			isMergeFinished = true
		}
		mergeFinishedNames = append(mergeFinishedNames, entry.Name())
	}

	// 没有 merge 完成，则直接返回
	if !isMergeFinished {
		return nil
	}

	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}

	// 删除旧的数据文件
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		dataFileName := data.GetDataFileName(db.options.DirPath, fileId)
		if _, err := os.Stat(dataFileName); err == nil {
			if err := os.Remove(dataFileName); err != nil {
				return err
			}
		}
	}

	// 将新的数据文件移动到数据目录中
	for _, finishedName := range mergeFinishedNames {
		srcFileName := filepath.Join(mergePath, finishedName)
		destFileName := filepath.Join(db.options.DirPath, finishedName)
		if err := os.Rename(srcFileName, destFileName); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileId), nil
}

func (db *DB) loadIndexFromHintFile() error {
	hintFilePath := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFilePath); os.IsNotExist(err) {
		return err
	}

	// 打开 hint 索引文件
	hintFile, err := data.OpenHintFile(hintFilePath)
	if err != nil {
		return err
	}

	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// 解码拿到实际的位置索引
		pos := data.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, pos)
		offset += size
	}
	return nil
}
