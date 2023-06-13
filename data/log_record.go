package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
)

// crc type keySize valueSize
// 4	1	5			5
const MaxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecord 写入到数据文件的记录
// 之所以叫日志，是因为数据文件中的数据是追加的，类似日志的格式
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

type LogRecordPos struct {
	FileId uint32
	Offset int64
	Size   uint32
}

// TransactionRecord 暂存的事务相关数据
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

type LogRecordHeader struct {
	crc        uint32        // crc校验
	recordType LogRecordType // 日志记录类型
	keySize    uint32        // key长度
	valueSize  uint32        // value长度
}

// EncodeLogRecord 对 LogRecord 进行编码，返回字节数组及其长度
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	// 初始一个header
	header := make([]byte, MaxLogRecordHeaderSize)
	// type
	header[4] = record.Type
	// 变长存放 ksize 和 vsize，节省空间
	index := 5
	index += binary.PutVarint(header[index:], int64(len(record.Key)))
	index += binary.PutVarint(header[index:], int64(len(record.Value)))
	// 构造编码后的字节数组
	size := index + len(record.Key) + len(record.Value)
	encLogRecord := make([]byte, size)
	copy(encLogRecord[:index], header[:index])
	copy(encLogRecord[index:], record.Key)
	copy(encLogRecord[index+len(record.Key):], record.Value)

	// crc校验码存放在头部4字节
	crc := crc32.ChecksumIEEE(encLogRecord[4:])
	binary.LittleEndian.PutUint32(encLogRecord[:4], crc)

	return encLogRecord, int64(size)
}

// EncodeLogRecordPos 对位置信息进行编码
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32*2+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.FileId))
	index += binary.PutVarint(buf[index:], pos.Offset)
	index += binary.PutVarint(buf[index:], int64(pos.Size))
	return buf
}

// DecodeLogRecordPos 对位置信息进行解码
func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	var index = 0
	fileId, i := binary.Varint(buf[index:])
	index += i
	offset, i := binary.Varint(buf[index:])
	index += i
	size, i := binary.Varint(buf[index:])
	return &LogRecordPos{
		FileId: uint32(fileId),
		Offset: offset,
		Size:   uint32(size),
	}
}

// DecodeLogRecordHeader  对 LogRecordHeader 进行解码，返回LogRecordHeader
func DecodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	// 若长度小于4
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}
	index := 5
	kSize, kBytes := binary.Varint(buf[index:])
	header.keySize = uint32(kSize)
	index += kBytes
	vSize, vBytes := binary.Varint(buf[index:])
	header.valueSize = uint32(vSize)
	index += vBytes

	return header, int64(index)
}

func GetLogRecordCRC(header []byte, record *LogRecord) uint32 {
	if record == nil {
		return 0
	}
	//recordBytes := make([]byte, len(header)+len(record.Key)+len(record.Value))
	//copy(recordBytes[:len(header)], header)
	//copy(recordBytes[len(header):len(header)+len(record.Key)], record.Key)
	//copy(recordBytes[len(header)+len(record.Key):], record.Value)
	//crc := crc32.ChecksumIEEE(recordBytes)
	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, record.Key)
	crc = crc32.Update(crc, crc32.IEEETable, record.Value)

	return crc
}
