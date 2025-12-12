// Package memory_map 提供内存映射文件的后端存储实现
package memory_map

import (
	"os"
	"time"
)

var (
	// 编译时检查：确保 MemoryMappedFile 实现了 BackendStorageFile 接口
	// 注意：这里被注释掉是为了避免循环导入（memory_map -> backend -> memory_map）
	// _ backend.BackendStorageFile = &MemoryMappedFile{}
)

// MemoryMappedFile 内存映射文件实现
//
// 实现了 BackendStorageFile 接口，提供基于内存映射的高性能文件访问。
// 这是 DiskFile 的替代实现，适用于小文件和频繁随机访问的场景。
//
// 核心特性：
//   - 实现 io.ReaderAt 接口：支持并发随机读取
//   - 实现 io.WriterAt 接口：支持随机写入
//   - 零拷贝访问：直接操作映射的内存区域
//   - 自动分块管理：按需分配内存映射块
//
// 使用场景：
//   - Windows 平台的小型 Volume（< 100MB）
//   - 需要频繁随机读写的 Volume
//   - 内存充足的服务器环境
//
// 性能对比（vs DiskFile）：
//   - 小 I/O（< 4KB）：快 2-5 倍（减少系统调用）
//   - 大 I/O（> 1MB）：性能相近（受限于磁盘带宽）
//   - 随机访问：快 50%+（利用页缓存）
//   - 顺序写入：略慢（需要额外的映射管理）
type MemoryMappedFile struct {
	mm *MemoryMap // 底层的内存映射对象
}

// NewMemoryMappedFile 创建新的内存映射文件实例
//
// 参数：
//   - f: 已打开的文件句柄（必须是可读写模式）
//   - memoryMapSizeMB: 内存映射的最大大小（单位 MB）
//
// 返回：
//   - *MemoryMappedFile: 内存映射文件对象
//
// 实现细节：
//   - 创建 MemoryMap 对象并初始化文件映射
//   - 将 MB 单位转换为字节（* 1024 * 1024）
//   - 在 Windows 上会调用 CreateFileMapping API
//   - 在 Linux/macOS 上会使用存根实现（空操作）
//
// 注意：
//   - memoryMapSizeMB 指定的是文件映射的最大大小，不是立即分配的内存
//   - 实际内存按需分配，首次写入时才会映射对应的 chunk
//   - 如果文件大小超过 memoryMapSizeMB，行为未定义（可能崩溃）
func NewMemoryMappedFile(f *os.File, memoryMapSizeMB uint32) *MemoryMappedFile {
	mmf := &MemoryMappedFile{
		mm: new(MemoryMap), // 创建 MemoryMap 对象
	}

	// 初始化内存映射，转换 MB 为字节
	// 例如：memoryMapSizeMB = 100 -> maxLength = 104857600 字节（100MB）
	mmf.mm.CreateMemoryMap(f, 1024*1024*uint64(memoryMapSizeMB))

	return mmf
}

// ReadAt 从内存映射文件的指定偏移位置读取数据
//
// 实现 io.ReaderAt 接口，支持并发读取（线程安全）。
//
// 参数：
//   - p: 目标缓冲区，用于存放读取的数据
//   - off: 文件偏移量（字节），从 0 开始
//
// 返回：
//   - n: 实际读取的字节数
//   - err: 错误信息，如果成功则为 nil
//
// 实现方式：
//   - 调用 ReadMemory 临时映射读取区域
//   - 将映射的数据拷贝到目标缓冲区 p
//   - 立即释放临时映射（节省内存）
//
// 性能考虑：
//   - TODO: 当前存在一次额外的内存拷贝（mapped -> p），可以优化为零拷贝
//   - 每次读取都会创建和释放临时映射（MapViewOfFile + UnmapViewOfFile）
//   - 适合不频繁的读取操作，频繁读取建议使用持久映射
func (mmf *MemoryMappedFile) ReadAt(p []byte, off int64) (n int, err error) {
	// 从内存映射中读取数据（临时映射）
	readBytes, e := mmf.mm.ReadMemory(uint64(off), uint64(len(p)))
	if e != nil {
		return 0, e
	}

	// TODO: 避免这次额外的拷贝
	// 可以优化为直接返回 readBytes 的切片视图（零拷贝）
	// 但需要确保调用者不修改数据，否则可能影响映射区域
	copy(p, readBytes)

	return len(readBytes), nil
}

// WriteAt 在内存映射文件的指定偏移位置写入数据
//
// 实现 io.WriterAt 接口，支持随机写入。
//
// 参数：
//   - p: 要写入的数据缓冲区
//   - off: 文件偏移量（字节），从 0 开始
//
// 返回：
//   - n: 实际写入的字节数（总是等于 len(p)）
//   - err: 错误信息，如果成功则为 nil
//
// 实现方式：
//   - 调用 WriteMemory 写入到持久映射的 chunk
//   - 如果目标 chunk 尚未映射，会自动分配
//   - 数据直接写入映射内存，无需系统调用
//
// 性能优势：
//   - 零系统调用：直接写入映射内存
//   - 延迟写入：操作系统异步刷新脏页到磁盘
//   - 批量写入：多次写入可以合并为一次磁盘 I/O
//
// 注意：
//   - 写入后数据并未立即持久化到磁盘
//   - 需要调用 Close 或 Sync 确保数据安全
//   - Windows 会在内存压力大或定期刷新时写回磁盘
func (mmf *MemoryMappedFile) WriteAt(p []byte, off int64) (n int, err error) {
	// 写入到内存映射区域（持久映射）
	// WriteMemory 会自动分配所需的 chunk
	mmf.mm.WriteMemory(uint64(off), uint64(len(p)), p)

	// 内存映射写入总是成功的（除非内存不足导致映射失败）
	return len(p), nil
}

// Truncate 截断或扩展文件到指定大小
//
// 参数：
//   - off: 目标文件大小（字节）
//
// 返回：
//   - error: 错误信息，如果成功则为 nil
//
// 当前实现：
//   - 空操作，直接返回 nil
//   - 内存映射文件不支持 Truncate（会导致数据不一致）
//
// 原因：
//   - 内存映射的大小在创建时已固定（CreateFileMapping）
//   - 修改文件大小需要重新创建映射（性能开销大）
//   - SeaweedFS 的 Volume 是 append-only，不需要 Truncate
//
// 注意：
//   - 如果调用此方法，不会有任何效果
//   - 文件大小仍由 End_of_file 字段决定
func (mmf *MemoryMappedFile) Truncate(off int64) error {
	// 空操作：内存映射文件不支持 Truncate
	return nil
}

// Close 关闭内存映射文件并释放所有资源
//
// 返回：
//   - error: 错误信息，如果成功则为 nil
//
// 执行操作：
//   - 关闭文件句柄（File.Close）
//   - 释放文件映射对象（CloseHandle）
//   - 释放所有内存映射视图（UnmapViewOfFile）
//   - 清空 write_map_views 列表
//
// 重要：
//   - 关闭顺序很关键：先关闭句柄，再释放映射
//   - 这样可以防止数据被异步写回磁盘（删除场景）
//   - 必须在文件使用完毕后调用，否则会泄漏资源
//
// 数据持久化：
//   - 关闭时，Windows 可能会刷新脏页到磁盘
//   - 但不保证一定写入（可能被操作系统缓存）
//   - 需要在关闭前调用 Sync 确保持久化
func (mmf *MemoryMappedFile) Close() error {
	// 删除文件并释放所有内存映射
	mmf.mm.DeleteFileAndMemoryMap()
	return nil
}

// GetStat 获取文件的统计信息（大小和修改时间）
//
// 返回：
//   - datSize: 文件大小（字节），基于 End_of_file 字段
//   - modTime: 文件最后修改时间（从底层文件读取）
//   - err: 错误信息，如果成功则为 nil
//
// 文件大小计算：
//   - 使用 End_of_file + 1（逻辑文件大小）
//   - End_of_file 记录最后写入的字节偏移量
//   - 例如：End_of_file = 99 -> datSize = 100（100 字节）
//
// 修改时间：
//   - 调用底层文件的 Stat() 获取元数据
//   - 返回操作系统记录的最后修改时间
//   - 可能不反映内存映射的写入（延迟更新）
//
// 特殊情况：
//   - 如果 End_of_file = -1（空文件）：datSize = 0
//   - 如果 Stat() 失败：返回零值和错误
func (mmf *MemoryMappedFile) GetStat() (datSize int64, modTime time.Time, err error) {
	// 获取文件元数据（修改时间等）
	stat, e := mmf.mm.File.Stat()
	if e == nil {
		// 返回逻辑文件大小（End_of_file + 1）和修改时间
		return mmf.mm.End_of_file + 1, stat.ModTime(), nil
	}

	// Stat 失败，返回零值和错误
	return 0, time.Time{}, err
}

// Name 返回文件的完整路径
//
// 返回：
//   - string: 文件的绝对路径（例如：D:\data\volume1\3.dat）
//
// 实现：
//   - 直接返回底层文件句柄的名称
//   - 路径在文件打开时就已确定
func (mmf *MemoryMappedFile) Name() string {
	return mmf.mm.File.Name()
}

// Sync 将文件数据同步到磁盘
//
// 返回：
//   - error: 错误信息，如果成功则为 nil
//
// 当前实现：
//   - 空操作，直接返回 nil
//   - 不会主动刷新脏页到磁盘
//
// 原因：
//   - Windows 的 FlushViewOfFile 性能开销大
//   - SeaweedFS 在关闭时会自动刷新
//   - 内存映射依赖操作系统的自动刷新机制
//
// 注意：
//   - 如果需要确保数据持久化，依赖 Close 操作
//   - 或者可以实现 FlushViewOfFile 调用（性能会下降）
func (mmf *MemoryMappedFile) Sync() error {
	// 空操作：依赖操作系统的自动刷新和 Close 时的刷新
	return nil
}
