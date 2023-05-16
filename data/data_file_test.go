package data

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDataFile_Close(t *testing.T) {
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	record := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("logic"),
		Type:  LogRecordNormal,
	}
	encodeLogRecord, size1 := EncodeLogRecord(record)
	err = dataFile.Write(encodeLogRecord)
	assert.Nil(t, err)

	logRecord, size2, err := dataFile.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, record, logRecord)
	assert.Equal(t, size1, size2)

}

func TestDataFile_ReadNBytes(t *testing.T) {
}

func TestDataFile_Write(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("aaa"))
	assert.Nil(t, err)
}

func TestOpenDataFile(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)
}
