package logic_kv

import (
	"github.com/stretchr/testify/assert"
	"logicKV/utils"
	"os"
	"testing"
)

func TestDB_WriteBatch(t *testing.T) {
	options := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-1")
	options.DirPath = dir
	db, err := Open(options)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(2))
	assert.Nil(t, err)

	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyIsEmpty, err)

	err = wb.Commit()
	assert.Nil(t, err)

	val, err := db.Get(utils.GetTestKey(1))
	assert.NotNil(t, val)
	assert.Nil(t, err)

}
