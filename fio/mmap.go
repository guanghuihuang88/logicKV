package fio

import (
	"golang.org/x/exp/mmap"
)

// MMap IO，内存文件映射
type MMap struct {
	readerAt *mmap.ReaderAt
}

// NewMMapIOManager 初始化 MMap IO
func NewMMapIOManager(fileName string) (*MMap, error) {
	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{readerAt: readerAt}, nil
}

// Read 从文件中给定位置读取对应的数据
func (mmap *MMap) Read(b []byte, offset int64) (int, error) {
	return mmap.readerAt.ReadAt(b, offset)
}

// Write 写入字节数组到文件中
func (mmap *MMap) Write([]byte) (int, error) {
	panic("not implemented")
}

// Sync 持久化数据
func (mmap *MMap) Sync() error {
	panic("not implemented")
}

// Close 关闭文件
func (mmap *MMap) Close() error {
	return mmap.readerAt.Close()
}

// Size 获取文件大小
func (mmap *MMap) Size() (int64, error) {
	return int64(mmap.readerAt.Len()), nil
}
