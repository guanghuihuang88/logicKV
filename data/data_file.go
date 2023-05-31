package data

import (
	"errors"
	"fmt"
	"github.com/guanghuihuang88/logicKV/fio"
	"hash/crc32"
	"io"
	"path/filepath"
)

// DataFile 数据文件
type DataFile struct {
	FileId      uint32        // 文件id
	WriteOffset int64         // 文件写到了那个位置
	IoManager   fio.IOManager // io 读写管理
}

var ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")

const (
	DataFileNameSuffix    = ".data"
	HintFileName          = "hint-index"
	MergeFinishedFileName = "merge-finished"
	SeqNoFileName         = "seq-no"
)

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	fileName := GetDataFileName(dirPath, fileId)
	return NewDataFile(fileName, fileId, ioType)
}

func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
}

// OpenHintFile 打开 Hint 索引文件
func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return NewDataFile(fileName, 0, fio.StandardFIO)
}

// OpenMergeFinishedFile 打开标识 merge 完成的文件
func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return NewDataFile(fileName, 0, fio.StandardFIO)
}

// OpenSeqNoFile 打开存放事务序列号的文件
func OpenSeqNoFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, SeqNoFileName)
	return NewDataFile(fileName, 0, fio.StandardFIO)
}

// NewDataFile 创建新的数据文件
func NewDataFile(fileName string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	// 初始化 IOManager 管理器接口
	fio, err := fio.NewIOManager(fileName, ioType)
	if err != nil {
		return nil, err
	}
	dataFile := &DataFile{
		FileId:      fileId,
		WriteOffset: 0,
		IoManager:   fio,
	}
	return dataFile, nil
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}
	headerBytes := int64(MaxLogRecordHeaderSize)
	if offset+MaxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}

	headerBuf, err := df.ReadNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	header, headerSize := DecodeLogRecordHeader(headerBuf)

	if header == nil {
		return nil, 0, io.EOF
	}

	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// 取出 key，value 长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	recordSize := headerSize + keySize + valueSize

	// 读取 key，value
	logRecord := &LogRecord{Type: header.recordType}
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.ReadNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	// crc校验
	crc := GetLogRecordCRC(headerBuf[crc32.Size:headerSize], logRecord)
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	return logRecord, recordSize, nil
}

func (df *DataFile) ReadNBytes(n int64, offset int64) ([]byte, error) {
	buf := make([]byte, n)
	if _, err := df.IoManager.Read(buf, offset); err != nil {
		return nil, err
	}
	return buf, nil
}

func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOffset += int64(n)
	return nil
}

// WriteHintRecord 写入索引信息到 Hint 文件还在
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}
	logRecord, _ := EncodeLogRecord(record)
	return df.Write(logRecord)
}

func (df *DataFile) Sync() error {
	err := df.IoManager.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (df *DataFile) Close() error {
	err := df.IoManager.Close()
	if err != nil {
		return err
	}
	return nil
}

func (df *DataFile) SetIOManager(dirPath string, ioType fio.FileIOType) error {
	if err := df.IoManager.Close(); err != nil {
		return err
	}
	ioManager, err := fio.NewIOManager(GetDataFileName(dirPath, df.FileId), ioType)
	if err != nil {
		return err
	}
	df.IoManager = ioManager
	return nil
}
