# 从零实现logicKV存储引擎-基础篇

## 1 bitcask 论文

> 论文地址：https://riak.com/assets/bitcask-intro.pdf

bitcask 存储模型是由一个做分布式存储系统的商业化公司 Riak 提出来的，是一个简洁高效的存储引擎

**bitcask 实例**

一个 bitcask 实例就是系统上的一个目录，并且限制同一时刻只能有一个进程打开这个目录。目录中有多个文件，同一时刻只能有一个活跃的文件用于写入新的数据

当活跃文件写满后，就将其关闭，并打开一个新的活跃文件用于写入。所以一个 bitcask 实例的目录中，有一个活跃文件和多个旧的数据文件

<img src="https://guanghuihuang-1315055500.cos.ap-guangzhou.myqcloud.com/%E5%AD%A6%E4%B9%A0%E7%AC%94%E8%AE%B0/%E6%95%B0%E6%8D%AE%E5%BA%93/%E4%BB%8E%E9%9B%B6%E5%AE%9E%E7%8E%B0kv%E5%AD%98%E5%82%A8/01.png" style="zoom:50%;" />

**Put**

只能用追加的（append only）方式将数据写入活跃文件，从而利用顺序 IO，减少磁盘寻址时间

**数据结构**

文件中的数据具有固定的规范，每一条数据由以下字段组成：

1. `crc`：数据校验，防止数据被破坏、篡改等
2. `timestamp`：写入数据的时间戳
3. `ksz`：key 的 size 大小
4. `vsz`：value 的 size 大小
5. `key`：用户实际存储的 key
6. `value`：用户实际存储的 value

<img src="https://guanghuihuang-1315055500.cos.ap-guangzhou.myqcloud.com/%E5%AD%A6%E4%B9%A0%E7%AC%94%E8%AE%B0/%E6%95%B0%E6%8D%AE%E5%BA%93/%E4%BB%8E%E9%9B%B6%E5%AE%9E%E7%8E%B0kv%E5%AD%98%E5%82%A8/02.png" style="zoom:33%;" />

**Delete**

数据删除操作的实现，是通过 append 一条该数据，并将该数据的有效位（墓碑值）置为无效。并不会实际删除数据文件中历史追加的该数据，而历史追加的该数据会通过数据文件的 merge 操作清理掉

**Index**

每一次向活跃文件中 append 一条数据后，都会更新内存索引 keydir，论文中使用 hash 表实现，索引的 key 是数据的 key，索引的 value 是数据在文件中的位置。这里索引数据结构的选择比较灵活，还可以是 B+ 树、跳表等天然支持排序的数据结构

内存索引一定会维护一条数据在磁盘中的最新位置，旧的数据依然存在，等待 merge 清除

<img src="https://guanghuihuang-1315055500.cos.ap-guangzhou.myqcloud.com/%E5%AD%A6%E4%B9%A0%E7%AC%94%E8%AE%B0/%E6%95%B0%E6%8D%AE%E5%BA%93/%E4%BB%8E%E9%9B%B6%E5%AE%9E%E7%8E%B0kv%E5%AD%98%E5%82%A8/03.png" style="zoom:33%;" />

**Get**

根据 key 在内存索引中找到相应记录的位置信息，根据位置信息找到磁盘中的对应文件，根据位置信息中的偏移量读取文件中相应的记录

<img src="https://guanghuihuang-1315055500.cos.ap-guangzhou.myqcloud.com/%E5%AD%A6%E4%B9%A0%E7%AC%94%E8%AE%B0/%E6%95%B0%E6%8D%AE%E5%BA%93/%E4%BB%8E%E9%9B%B6%E5%AE%9E%E7%8E%B0kv%E5%AD%98%E5%82%A8/04.png" style="zoom:33%;" />

**Merge**

随着数据越来越多，需要将旧的无效数据删除。论文提出了一个 merge 的过程来清理无效数据。merge 会遍历所有旧的数据文件，将所有有效的数据重新写入到新的数据文件中，并将旧的数据文件删除

merge 完成后还会为所有数据文件生成一个 hint 文件，可以看做全部数据的索引，它存储位置信息而不是实际的数据。他的作用是在 bitcask 启动时，直接加载 hint 文件生成内存索引，而不用重新扫描所有数据文件

**总结**

bitcask 的设计基本满足了如下优点：

1. 查询、写入速度很快，因为读写都只有一次磁盘 IO
2. 写入数据还是顺序 IO，保证了高吞吐
3. 内存中不会存储实际的 value，因此，在 value 较大的情况下，能够处理超过内存容量的数据
4. 提交日志和数据文件实际上就是同一个文件，数据的崩溃恢复能够得到保障
5. 备份和恢复非常简单，只需要拷贝整个数据目录即可
6. 设计简洁，数据文件格式易懂、易管理



## 2 内存、磁盘设计

![](https://guanghuihuang-1315055500.cos.ap-guangzhou.myqcloud.com/%E5%AD%A6%E4%B9%A0%E7%AC%94%E8%AE%B0/%E6%95%B0%E6%8D%AE%E5%BA%93/%E4%BB%8E%E9%9B%B6%E5%AE%9E%E7%8E%B0kv%E5%AD%98%E5%82%A8/05.png)

**内存设计**

内存索引的设计，需要支持高效插入、读取、删除数据的结构，并且如果需要数据高效遍历的话，则最好选择支持有序的结构。常见的内存索引数据结构可以选择 BTree、跳表、红黑树等

此外，由于内存索引的数据结构选择是多样的，因此可以设计一个通用的抽象接口，接入不同的数据结构，如果想要接入一个新的数据结构，只需要实现我们的抽象接口的方法即可

以 BTree 为例，可以直接封装 google 开源的项目：https://github.com/google/btree，调用 btree 的方法实现 Indexer 接口的方法

```go
type BTree struct {
   tree *btree.BTree		// import "github.com/google/btree"
   lock *sync.RWMutex
}
```

```go
// Indexer 通用索引接口
type Indexer interface {
	// Put 向索引中存储 key 对应的数据位置信息
	Put([]byte, *data.LogRecordPos) bool

	// Get 根据 key 取出对应的数据位置信息
	Get([]byte) *data.LogRecordPos

	// Delete 根据 key 删除对应的数据位置信息
	Delete([]byte) bool

	// Iterator 返回迭代器
	Iterator(bool) *btreeIterator

	// Size 索引长度，key 个数
	Size() int
}
```

**磁盘设计**

可以直接封装 golang 自带的 os.File，实现 IOManager 接口的方法

```go
type FileIO struct {
   fd *os.File		// import "os"
}
```

```go
// IOManager 通用 IO 接口
type IOManager interface {
   // Read 从文件中给定位置读取对应的数据
   Read([]byte, int64) (int, error)

   // Write 写入字节数组到文件中
   Write([]byte) (int, error)

   // Sync 持久化数据
   Sync() error

   // Close 关闭文件
   Close() error

   // Size 获取文件大小
   Size() (int64, error)
}
```

**数据结构**

我们将存放在磁盘文件中的数据记录封装为 LogRecord，其中 Type 字段是用来标识数据是否有效的墓碑值

```go
type LogRecord struct {
   Key   []byte
   Value []byte
   Type  LogRecordType
}
```

另外，内存索引中 value 记录的不是数据本身，而是数据记录在文件中的位置 LogRecordPos

```go
type LogRecordPos struct {
   FileId uint32
   Offset int64
}
```

## 3 bitcask 实例

<img src="https://guanghuihuang-1315055500.cos.ap-guangzhou.myqcloud.com/%E5%AD%A6%E4%B9%A0%E7%AC%94%E8%AE%B0/%E6%95%B0%E6%8D%AE%E5%BA%93/%E4%BB%8E%E9%9B%B6%E5%AE%9E%E7%8E%B0kv%E5%AD%98%E5%82%A8/06.png" style="zoom:67%;" />

设计好了内存索引和磁盘文件，就可以在他们的基础上设计我们的 bitcask 存储引擎，我们先给出 bitcask 存储引擎的基本框架及其启动流程，后续将实现读写等基本功能

```go
// DB bitcask 存储引擎实例
type DB struct {
	 options    Options                   // 存储引擎的配置项	
   mu         *sync.RWMutex							// 锁
   fileIds    []int                     // 文件id的集合，只有在加载索引的时候会用到
   activeFile *data.DataFile            // 当前活跃的数据文件，可以用于写入
   olderFiles map[uint32]*data.DataFile // 旧的数据文件，只能用于读
   index      index.Indexer             // 内存索引
   seqNo      uint64                    // 事务id
   isMerging  bool                      // 是否正在进行Merge
}
```

其中，activeFile 是指向当前活跃文件的指针，olderFiles 是历史数据文件的集合。另外，数据文件类型 DataFile 通过封装文件 id 和 IOManager 来实现对数据文件的基本操作

```go
// DataFile 数据文件
type DataFile struct {
   FileId      uint32        // 文件id
   WriteOffset int64         // 文件写到了那个位置
   IoManager   fio.IOManager // io 读写管理
}
```

## 4 bitcask 启动流程

bitcask 实例的启动流程主要分 5 步：

1. 根据配置初始化 bitcask 实例
2. 加载 merge 数据目录
3. 从 hint 索引文件中加载索引
4. 加载数据目录中的文件
5. 遍历数据文件中的内容，构建内存索引

```go
// Open 启动 bitcask 存储引擎实例
func Open(options Options) (*DB, error) {
   // 对用户传入的配置项进行校验

   // 判断数据目录是否存在，如果不存在，则创建这个目录

   // 初始化 bitcask 实例结构体
   db := &DB{
		...
   }

   // 加载 merge 数据目录
   db.loadMergeFiles()

   // 从 hint 索引文件中加载索引
   db.loadIndexFromHintFile()

   // 加载数据文件
   db.loadDataFile()

   // 加载索引
   db.loadIndexFromDataFiles()

   return db, nil
}
```

**加载 merge 目录**

每一次发生 merge 操作（后续讲解），都会将 merge 后的文件存放独立于数据目录之外的 merge 目录，在启动存储引擎实例时，需要先处理 merge 目录：

```go
func (db *DB) loadMergeFiles() error {
   // merge 目录不存在则直接返回
	...
   // 判断 merge 是否处理完成，没有 merge 完成，则直接返回
   if !isMergeFinished {
      return nil
   }
	...
   // 找到最新的未参与 merge 的文件 id
   nonMergeFileId, err := db.getNonMergeFileId(mergePath)

   for ; fileId < nonMergeFileId; fileId++ {
   		// 删除旧的数据文件（文件 id 小于 nonMergeFileId 的文件）
   }

   for _, finishedName := range mergeFinishedNames {
   		// 将 merge 目录中新的数据文件移动到数据目录中
      if err := os.Rename(srcFileName, destFileName); err != nil {
         return err
      }
   }

   return nil
}
```

**加载 hint 索引**

merge 目录中包含一个 hint 索引文件，它存放了针对 merge 目录中数据文件的索引记录，因此可以直接读取 hint 文件中的记录到内存索引中

**加载数据文件**

打开配置项中的数据目录，加载数据目录下的所有数据文件，根据文件名排序，将最新的数据文件作为活跃文件，其他文件作为历史文件

```go
// 从磁盘中加载数据文件
func (db *DB) loadDataFile() error {
  ...
   // 对文件id进行排序
   sort.Ints(fileIds)

   for index, fileId := range fileIds {
      dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fileId))
      if index == len(fileIds)-1 {
         // id 最大的文件作为活跃文件
         db.activeFile = dataFile
      } else {
         // 其他的文件作为历史文件
         db.olderFiles[uint32(fileId)] = dataFile
      }
   }
   return nil
}
```

**加载索引**

加载数据目录，遍历所有数据文件中的内容，更新到索引中。首先，查看 merge-finished 文件，判断是否发生过 merge 操作；然后，从最新未参与 merge 的文件 id 开始遍历，遍历文件中的每一条 LogRecord：

- 判断该记录是否属于事务操作
  - 非事务操作则直接更新到内存索引中
  - 若为事务操作，则判断记录的墓碑值是否标识事务完成
    - 若为未完成事务操作，则将该记录存放到相应事务 id 的集合中，待事务完成统一更新内存索引
    - 若事务完成，则将当前事务集合中的记录统一更新到内存索引中

```go
// 遍历数据文件中的内容，更新到索引中
func (db *DB) loadIndexFromDataFiles() error {
   // 若数据文件列表为空，则数据库是空的，直接返回
	...
   // 查看是否发生过 merge
   if _, err := os.Stat(mergeFinishedFileName); err == nil {
     ...
      hasMerged = true
      nonMergeFileId = fileId
   }
  ...
   // 暂存事务数据
   transactionRecords := make(map[uint64][]*data.TransactionRecord)
   currentTxnSeqNo := nonTxnSeqNo

   // 遍历所有的文件id，处理文件中的数据
   for index, id := range db.fileIds {
      // 若 id 比最近未参与 merge 的文件 id 更小，则跳过
      if hasMerged && fileId < nonMergeFileId {
         continue
      }
			...
      offset := int64(0)
      for {
         logRecord, size, err := dataFile.ReadLogRecord(offset)
         if io.EOF {
               break
         }
				...
         // 更新索引
         if seqNo == nonTxnSeqNo {
            // 非事务操作，直接更新内存索引
            updateIndex(key, pos)
         } else {
            // 事务完成，对应 seqNo 的数据可以更新到内存索引
            if logRecord.Type == data.LogRecordTxnFinished {
               for _, txnRecord := range transactionRecords[seqNo] {
                  updateIndex(Key, Pos)
               }
               delete(transactionRecords, seqNo)
            } else {
            // 否则存放到事务操作列表中
               transactionRecords[seqNo] = append(transactionRecords[seqNo], ...)
            }
         }
        ...
   }
	...
}
```

## 5 写数据流程

![](https://guanghuihuang-1315055500.cos.ap-guangzhou.myqcloud.com/%E5%AD%A6%E4%B9%A0%E7%AC%94%E8%AE%B0/%E6%95%B0%E6%8D%AE%E5%BA%93/%E4%BB%8E%E9%9B%B6%E5%AE%9E%E7%8E%B0kv%E5%AD%98%E5%82%A8/07.png)

写数据流程总体就两步：

1. 写磁盘文件：追加写入到当前活跃文件中
2. 更新内存索引

```go
// Put 写入 key/value 数据，key 不能为空
func (db *DB) Put(key []byte, value []byte) error {
  ...
  
   // 追加写入到当前活跃文件中
   appendLogRecordPos, err := db.appendLogRecordWithLock(logRecord)
  ...

   // 更新内存索引
   if ok := db.index.Put(key, appendLogRecordPos); !ok {
      return ErrIndexUpdateFailed
   }

   return nil
}
```

**appendLogRecord 方法逻辑**

这个方法的伪代码逻辑大概如下：

```go
// 追加写数据到活跃文件中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
   // 判断当前活跃数据文件是否存在，因为数据库在没有写入的时候是没有文件生成的，如果不存在就初始化一个活跃文件
   if db.activeFile == nil {
      // 初始化活跃文件
   }

   // 如果写入数据已经达到了活跃文件的阈值，则关闭活跃文件，打开新的活跃文件
   if db.activeFile.WriteOffset+size > db.options.DataFileSize {
      // 先持久化数据文件，保证已有的数据持久到磁盘中
      db.activeFile.Sync()

      // 当前活跃文件转换为旧的数据文件
      db.olderFiles[db.activeFile.FileId] = db.activeFile

      // 打开新的活跃文件
   }

   // 向活跃文件中写入记录，注意写入记录之前，需要将 LogRecord 编码成字节数组，再调用文件IO接口
   db.activeFile.Write(encodeLogRecord)
  
   // 根据用户配置，决定是否调用 Sync 方法持久化
  
   // 构造内存索引信息并返回
   return pos, nil
}
```

> 对于标准系统 IO，向文件写数据实际上会先写到内存缓冲区，并且等待操作系统进行调度，将其刷到磁盘中
>
> 如果缓冲区的内容还没来得及持久化，而此时操作系统发生了崩溃，可能会导致缓冲区的数据丢失
>
> 所以为了避免数据丢失，我们可以手动指定 fsync 系统调用，强制将缓冲区的内容刷到磁盘，但是这样会导致效率较低，所以我们可以提供配置选项，让用户决定是否每次都刷盘

## 6 读数据流程

![](https://guanghuihuang-1315055500.cos.ap-guangzhou.myqcloud.com/%E5%AD%A6%E4%B9%A0%E7%AC%94%E8%AE%B0/%E6%95%B0%E6%8D%AE%E5%BA%93/%E4%BB%8E%E9%9B%B6%E5%AE%9E%E7%8E%B0kv%E5%AD%98%E5%82%A8/08.png)

```go
// Get 根据 key 读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	...
   logRecordPos := db.index.Get(key)
	...
   return db.getValueByPos(logRecordPos)
}

// 根据 pos 位置获取数据
func (db *DB) getValueByPos(pos *data.LogRecordPos) ([]byte, error) {
   var dataFile *data.DataFile
   if db.activeFile.FileId == pos.FileId {
      dataFile = db.activeFile
   } else {
      dataFile = db.olderFiles[pos.FileId]
   }
	...
   logRecord, _, err := dataFile.ReadLogRecord(pos.Offset)
	...

   return logRecord.Key, nil
}
```

## 7 删数据流程

删数据和写数据一样，实际上是向活跃文件追加一条记录，只是该记录的墓碑值类型为 LogRecordDeleted。被删除的数据会在 merge 操作后被清除

```go
func (db *DB) Delete(key []byte) error {
   // 判断 key 是否有效
   // 判断 key 是否存在
   if db.index.Get(key) == nil return 
   // 删除 key
   deleteLogRecord := &data.LogRecord{
     Key: LogRecordKeyWithSeq(key, nonTxnSeqNo), 
     Type: data.LogRecordDeleted		// 墓碑值标识为删除
   }
   // 追加写入到当前活跃文件中
   db.appendLogRecordWithLock(deleteLogRecord)
   // 更新内存索引
   db.index.Delete(key)
   return nil
}
```

## 8 事务实现：WriteBatch 原子写

本节我们实现一种最简单的事务，利用一个全局锁保证串行化，实现最基本的满足 ACID 的事务

之前我们实现了 Put 写入操作，如果我们在一个循环中连续 Put 100 条数据，这个批量操作是无法保证原子性的，我们需要实现一个批量写入的操作，保证这一批写入要么全部成功，要么全部失败

我们定义一个 WriteBatch 类型，它将一批写入或删除操作封装到一个 pendingWrites 的 map 容器中：

```go
// WriteBatch 原子批量写数据，保证事务的原子性
type WriteBatch struct {
   options       WriteBatchOptions
   mu            *sync.Mutex
   db            *DB
   pendingWrites map[string]*data.LogRecord // 暂存用户写入的数据
}
```

然后基于 WriteBatch 类型实现 Put，Delete，Commit 方法实现事务

**Put**

Put 方法简单的将待写入的 LogRecord 暂存到 pendingWrites 中

```go
// Put 添加数据
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	...
   // 暂存记录
   wb.pendingWrites[string(key)] = &data.LogRecord{
      Key:   key,
      Value: value,
   }
  ...
}
```

**Delete**

Delete 方法先判断待删除的数据是否存在，然后也简单的将待删除的 LogRecord 暂存到 pendingWrites 中，墓碑值置为 Deleted

```go
// Delete 删除数据
func (wb *WriteBatch) Delete(key []byte) error {
	...
   // 若db中还没有该数据：
   if wb.db.index.Get(key) == nil {
     // 1. pendingWrites中有该数据，则从pendingWrites中删除
     // 2. 否则，直接返回
   }

   // 暂存记录
   wb.pendingWrites[string(key)] = &data.LogRecord{
      Key:   key,
      Type: data.LogRecordDeleted,
   }
}
```

**Commit**

Commit 方法的实现是核心：

1. 对存储引擎实例加锁，简单直接地保证隔离性
2. 利用原子操作，获取一个最新的事务 id
3. 遍历 pendingWrites 中的每一条 LogRecord，写入到文件中，并将记录的位置信息记录到一个 map 容器中
4. 写一条标识事务完成的数据到文件中
5. 根据配置决定是否立即持久化文件数据
6. 根据记录位置信息的 map 容器，更新内存索引
7. 清空 pendingWrites

```go
func (wb *WriteBatch) Commit() error {
  ...
   // 对存储引擎实例加锁
   wb.db.mu.Lock()
   defer wb.db.mu.Unlock()
	 
  // 利用原子操作，获取一个最新的事务 id
   seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

   // 遍历 pendingWrites 中的每一条 LogRecord，写入到文件中，并将记录的位置信息记录到一个 map 容器中
   positions := make(map[string]*data.LogRecordPos)
   for _, record := range wb.pendingWrites {
     ... 
      wb.db.appendLogRecord(record)
     ...
      positions[string(record.Key)] = logRecordPos
   }

   // 写一条标识事务完成的数据
   wb.db.appendLogRecord(finishedRecord)

   // 根据配置决定是否持久化
   wb.db.activeFile.Sync()

   // 更新内存索引
   for _, record := range wb.pendingWrites {
     ...
      wb.db.index.Put(record.Key, pos)
     ...
   }

   // 清空 pendingWrites
	...
}
```

**启动时的修改**

启动数据库实例时，不能直接拿到 LogRecord 就更新内存索引，因为可能是无效的事务数据，需要将事务数据暂存起来，读到一个标识事务完成的数据时，才将暂存的对应事务 id 的数据更新到内存索引

## 9 Merge 数据清理

merge 的主要功能是清理磁盘上的无效数据，避免数据目录空间的膨胀

我们可以使用一个临时文件夹 ~/bitcask/merge，在这个临时文件夹中启动一个新的 bitcask 实例，这个实例与正在运行的 db 实例相互独立，因为他们是不同的进程。将 db 实例数据目录中的文件一一读取到新的 merge 实例中，然后遍历其中的 LogRecord，与 db 实例的内存索引进行比对，若当前 LogRecord 有效则将其写入 merge 实例的活跃文件中，并将位置信息写入 Hint 索引文件中

![](https://guanghuihuang-1315055500.cos.ap-guangzhou.myqcloud.com/%E5%AD%A6%E4%B9%A0%E7%AC%94%E8%AE%B0/%E6%95%B0%E6%8D%AE%E5%BA%93/%E4%BB%8E%E9%9B%B6%E5%AE%9E%E7%8E%B0kv%E5%AD%98%E5%82%A8/09.png)













































