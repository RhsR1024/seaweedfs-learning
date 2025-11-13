package storage

import (
	"io"
	"os"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// NeedleMapKind 定义了 needle 映射的实现类型
// SeaweedFS 支持多种索引实现方式,以适应不同的内存和性能需求
type NeedleMapKind int

const (
	// NeedleMapInMemory 纯内存索引
	// 将所有 needle 的索引信息都保存在内存中,性能最好但内存占用最大
	// 适用于有充足内存资源的场景
	NeedleMapInMemory NeedleMapKind = iota

	// NeedleMapLevelDb 小内存占用的 LevelDB 索引
	// 总内存约 4MB,1 个写缓冲区,3 个块缓冲区
	// 适用于内存受限的场景,会牺牲一定的性能
	NeedleMapLevelDb // small memory footprint, 4MB total, 1 write buffer, 3 block buffer

	// NeedleMapLevelDbMedium 中等内存占用的 LevelDB 索引
	// 总内存约 8MB,3 个写缓冲区,5 个块缓冲区
	// 平衡了内存占用和性能
	NeedleMapLevelDbMedium // medium memory footprint, 8MB total, 3 write buffer, 5 block buffer

	// NeedleMapLevelDbLarge 大内存占用的 LevelDB 索引
	// 总内存约 12MB,4 个写缓冲区,8 个块缓冲区
	// 性能较好,但需要更多内存
	NeedleMapLevelDbLarge // large memory footprint, 12MB total, 4write buffer, 8 block buffer
)

// NeedleMapper 是 needle 索引映射的核心接口
// 它定义了所有索引实现必须提供的操作方法
// 作用是维护 NeedleId 到其在 .dat 文件中位置(offset)和大小(size)的映射关系
// 这个索引使得 SeaweedFS 能够快速定位和读取文件数据
type NeedleMapper interface {
	// Put 添加或更新一个 needle 的索引信息
	// key: needle ID,offset: 在 .dat 文件中的偏移量,size: needle 的大小
	Put(key NeedleId, offset Offset, size Size) error

	// Get 根据 needle ID 查询其索引信息
	// 返回 NeedleValue(包含 offset 和 size)和是否存在的标志
	Get(key NeedleId) (element *needle_map.NeedleValue, ok bool)

	// Delete 标记删除一个 needle
	// 注意:这只是在索引中标记删除,实际数据仍在 .dat 文件中
	Delete(key NeedleId, offset Offset) error

	// Close 关闭索引,释放资源
	Close()

	// Destroy 销毁索引,删除所有索引数据
	Destroy() error

	// ContentSize 返回所有有效 needle 的总大小(字节数)
	ContentSize() uint64

	// DeletedSize 返回所有已删除 needle 的总大小(字节数)
	DeletedSize() uint64

	// FileCount 返回当前有效的文件数量
	FileCount() int

	// DeletedCount 返回已删除的文件数量
	DeletedCount() int

	// MaxFileKey 返回当前最大的 needle ID
	MaxFileKey() NeedleId

	// IndexFileSize 返回索引文件的大小(字节数)
	IndexFileSize() uint64

	// Sync 将索引数据同步到磁盘
	Sync() error

	// ReadIndexEntry 读取索引文件中第 n 个条目
	// 用于遍历或恢复索引数据
	ReadIndexEntry(n int64) (key NeedleId, offset Offset, size Size, err error)
}

// baseNeedleMapper 是所有 NeedleMapper 实现的基础结构
// 提供了索引文件的基本操作能力
type baseNeedleMapper struct {
	// mapMetric 嵌入的指标统计结构
	// 用于记录内容大小、删除大小、文件数量等统计信息
	mapMetric

	// indexFile 索引文件句柄(.idx 文件)
	// 索引文件持久化存储 needle 的映射关系
	indexFile *os.File

	// indexFileAccessLock 索引文件访问锁
	// 保护并发写入索引文件时的线程安全
	indexFileAccessLock sync.Mutex

	// indexFileOffset 当前索引文件的写入偏移量
	// 记录下一个索引条目应该写入的位置
	indexFileOffset int64
}

// TempNeedleMapper 临时 needle 映射接口
// 扩展了 NeedleMapper,增加了索引加载和更新功能
// 主要用于 volume 启动时的索引重建和恢复场景
type TempNeedleMapper interface {
	NeedleMapper

	// DoOffsetLoading 从指定位置开始加载索引数据到内存
	// v: volume 实例,indexFile: 索引文件,startFrom: 开始加载的偏移量
	// 用于增量加载索引,避免全量加载带来的性能问题
	DoOffsetLoading(v *Volume, indexFile *os.File, startFrom uint64) error

	// UpdateNeedleMap 更新 needle 映射表
	// 用于将临时索引数据转换为持久化索引(如从内存索引转换为 LevelDB 索引)
	// opts: LevelDB 配置选项,ldbTimeout: LevelDB 操作超时时间
	UpdateNeedleMap(v *Volume, indexFile *os.File, opts *opt.Options, ldbTimeout int64) error
}

// IndexFileSize 返回索引文件的大小
// 通过文件系统获取 .idx 文件的实际大小
func (nm *baseNeedleMapper) IndexFileSize() uint64 {
	stat, err := nm.indexFile.Stat()
	if err == nil {
		return uint64(stat.Size())
	}
	return 0
}

// appendToIndexFile 追加一条索引记录到索引文件
// 每条索引记录包含 needle ID、offset 和 size
// 使用互斥锁保证并发写入的安全性
func (nm *baseNeedleMapper) appendToIndexFile(key NeedleId, offset Offset, size Size) error {
	// 将索引信息序列化为字节数组
	bytes := needle_map.ToBytes(key, offset, size)

	// 加锁保护文件写入操作
	nm.indexFileAccessLock.Lock()
	defer nm.indexFileAccessLock.Unlock()

	// 在当前偏移量位置写入索引数据
	written, err := nm.indexFile.WriteAt(bytes, nm.indexFileOffset)
	if err == nil {
		// 更新偏移量,指向下一个写入位置
		nm.indexFileOffset += int64(written)
	}
	return err
}

// Sync 将索引文件的缓冲数据刷新到磁盘
// 确保数据持久化,防止系统崩溃导致的数据丢失
func (nm *baseNeedleMapper) Sync() error {
	return nm.indexFile.Sync()
}

// ReadIndexEntry 读取索引文件中的第 n 个条目
// 每个条目的大小是固定的(NeedleMapEntrySize 字节)
// 返回该条目的 needle ID、offset 和 size
func (nm *baseNeedleMapper) ReadIndexEntry(n int64) (key NeedleId, offset Offset, size Size, err error) {
	// 分配缓冲区用于读取一个索引条目
	bytes := make([]byte, NeedleMapEntrySize)
	var readCount int

	// 从文件中读取第 n 个条目
	// 位置计算:n * NeedleMapEntrySize
	if readCount, err = nm.indexFile.ReadAt(bytes, n*NeedleMapEntrySize); err != nil {
		// 处理 EOF 情况:如果已读取完整条目,则忽略 EOF 错误
		if err == io.EOF {
			if readCount == NeedleMapEntrySize {
				err = nil
			}
		}
		if err != nil {
			return
		}
	}

	// 解析字节数组,提取 needle ID、offset 和 size
	key, offset, size = idx.IdxFileEntry(bytes)
	return
}
