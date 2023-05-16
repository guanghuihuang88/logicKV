package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestFileIO_Close(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("/tmp", "a.data"))
	defer destroyFile(filepath.Join("/tmp", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}

func TestFileIO_Read(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("/tmp", "a.data"))
	defer destroyFile(filepath.Join("/tmp", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	b := make([]byte, 3)
	n, err := fio.Read(b, 0)
	assert.Equal(t, 3, n)
	assert.Nil(t, err)

	t.Log(b, n, err)
}

func TestFileIO_Sync(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("/tmp", "a.data"))
	defer destroyFile(filepath.Join("/tmp", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
}

func TestFileIO_Write(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("/tmp", "a.data"))
	defer destroyFile(filepath.Join("/tmp", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte("aaa"))
	assert.Equal(t, 3, n)
	assert.Nil(t, err)

	t.Log(n, err)

}

func TestNewFileIOManager(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("/tmp", "a.data"))
	defer destroyFile(filepath.Join("/tmp", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func destroyFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}
