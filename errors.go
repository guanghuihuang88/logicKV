package logic_kv

import "errors"

var (
	ErrKeyIsEmpty        = errors.New("the key is empty")
	ErrIndexUpdateFailed = errors.New("failed to update index")
	ErrKeyNotFound       = errors.New("key not fount in database")
	ErrDataFileNotFound  = errors.New("dataFile not fount in database")
	ErrDataDirCorrupted  = errors.New("this data dir is corrupted")
	ErrExceedMaxBatch    = errors.New("exceed the max batch")
	ErrMergeIsProgress   = errors.New("merge is in progress, try again later")
)
