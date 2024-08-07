package logic_kv

import "os"

type Options struct {
	DirPath string // 数据库数据目录

	DataFileSize int64 // 数据文件大小

	SyncWrites bool // 每次写入数据是否持久化

	BytesPerSync uint // 累计写到多少字节后进行持久化

	IndexType IndexerType

	MMapAtStartup bool // 启动时是否使用 MMap 加载数据

	DataFileMergeRatio float32 // 数据文件 merge 比率
}

// IteratorOptions 索引迭代器配置项
type IteratorOptions struct {
	// 遍历前缀为指定值的 Key，默认为空
	Prefix []byte
	// 是否反向遍历，默认 false 是正向
	Reverse bool
}

// WriteBatchOptions 批量写配置项
type WriteBatchOptions struct {
	// 一个批次当中最大的数据量
	MaxBatchNum uint
	// 提交时是否 sync 持久化
	SyncWrites bool
}

type IndexerType = int8

const (
	// Btree 索引
	Btree IndexerType = iota + 1

	// ART 自适应基数树索引
	ART

	// BPTree B+树（磁盘索引、将索引存储在磁盘上，需要一个文件路径作为参数）
	BPTree
)

var DefaultOptions = Options{
	DirPath:            os.TempDir(),
	DataFileSize:       256 * 1024 * 1024, // 256MB
	SyncWrites:         false,
	BytesPerSync:       0,
	IndexType:          Btree,
	MMapAtStartup:      true,
	DataFileMergeRatio: 0.5,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
