package data

import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestDecodeLogRecordHeader(t *testing.T) {
	// 正常情况
	header := []byte{140, 190, 23, 254, 0, 8, 10}
	recordHeader, i := DecodeLogRecordHeader(header)
	assert.NotNil(t, recordHeader)
	assert.Equal(t, LogRecordNormal, recordHeader.recordType)
	assert.Equal(t, uint32(4262968972), recordHeader.crc)
	assert.Equal(t, uint32(4), recordHeader.keySize)
	assert.Equal(t, uint32(5), recordHeader.valueSize)
	assert.Equal(t, int64(7), i)

	// value为null的情况
	header = []byte{9, 252, 88, 14, 0, 8, 0}
	recordHeader, i = DecodeLogRecordHeader(header)
	assert.NotNil(t, recordHeader)
	assert.Equal(t, LogRecordNormal, recordHeader.recordType)
	assert.Equal(t, uint32(240712713), recordHeader.crc)
	assert.Equal(t, uint32(4), recordHeader.keySize)
	assert.Equal(t, uint32(0), recordHeader.valueSize)
	assert.Equal(t, int64(7), i)

	// deleted情况
	header = []byte{189, 247, 47, 168, 1, 8, 0}
	recordHeader, i = DecodeLogRecordHeader(header)
	assert.NotNil(t, recordHeader)
	assert.Equal(t, LogRecordDeleted, recordHeader.recordType)
	assert.Equal(t, uint32(2821715901), recordHeader.crc)
	assert.Equal(t, uint32(4), recordHeader.keySize)
	assert.Equal(t, uint32(0), recordHeader.valueSize)
	assert.Equal(t, int64(7), i)
}

func TestEncodeLogRecord(t *testing.T) {
	// 正常情况
	record := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("logic"),
		Type:  LogRecordNormal,
	}
	logRecord, n := EncodeLogRecord(record)
	t.Log(logRecord)
	assert.NotNil(t, logRecord)
	assert.Greater(t, n, int64(5))

	// value为空的情况
	record = &LogRecord{
		Key:   []byte("name"),
		Value: nil,
		Type:  LogRecordNormal,
	}
	logRecord, n = EncodeLogRecord(record)
	t.Log(logRecord)
	assert.NotNil(t, logRecord)
	assert.Greater(t, n, int64(5))

	// deleted的情况
	record = &LogRecord{
		Key:   []byte("name"),
		Value: nil,
		Type:  LogRecordDeleted,
	}
	logRecord, n = EncodeLogRecord(record)
	t.Log(logRecord)
	assert.NotNil(t, logRecord)
	assert.Greater(t, n, int64(5))
}

func TestGetLogRecordCRC(t *testing.T) {

	record := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("logic"),
		Type:  LogRecordNormal,
	}
	header := []byte{140, 190, 23, 254, 0, 8, 10}
	crc := GetLogRecordCRC(header[crc32.Size:], record)
	assert.Equal(t, uint32(4262968972), crc)
}
