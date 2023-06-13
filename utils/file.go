package utils

import (
	"io/fs"
	"path/filepath"
	"syscall"
)

// DirSize 获取一个目录的大小
func DirSize(dirPath string) (int64, error) {
	var size int64
	err := filepath.Walk(dirPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// AvaliableDiskSize 获取磁盘剩余可用空间大小
func AvaliableDiskSize() (uint64, error) {
	wd, err := syscall.Getwd()
	if err != nil {
		return 0, err
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(wd, &stat); err != nil {
		return 0, err
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}
