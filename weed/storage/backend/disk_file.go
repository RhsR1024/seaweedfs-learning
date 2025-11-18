// Package backend 提供 SeaweedFS 的底层存储后端实现
// 支持本地磁盘、远程存储等多种存储介质
package backend

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"io"
	"os"
	"runtime"
	"time"
)

var (
	// 编译时检查：确保 DiskFile 实现了 BackendStorageFile 接口
	// 如果未实现接口的所有方法，编译将失败
	_ BackendStorageFile = &DiskFile{}
)

// isMac 检测当前运行环境是否为 macOS
// macOS 上的 fsync 行为与 Linux 不同，需要特殊处理
const isMac = runtime.GOOS == "darwin"

// DiskFile 表示本地磁盘上的一个 Volume 数据文件或索引文件
// 是 SeaweedFS 最常用的存储后端实现
//
// 设计要点：
// - 封装了 os.File 并缓存文件大小和修改时间，避免频繁的 syscall
// - fileSize 总是对齐到 NeedlePaddingSize (8字节)，保证 Needle 边界对齐
// - 所有写操作都会更新 fileSize 和 modTime 缓存
// - 线程安全：os.File 本身支持并发 ReadAt/WriteAt
type DiskFile struct {
	File         *os.File // 底层的操作系统文件句柄，支持并发读写
	fullFilePath string   // 文件的绝对路径，用于日志和错误报告
	fileSize     int64    // 文件逻辑大小（对齐后），避免每次都调用 Stat()
	modTime      time.Time // 文件最后修改时间的缓存，提升 GetStat 性能
}

// NewDiskFile 从已打开的文件句柄创建 DiskFile 实例
// 会立即获取文件状态并对齐文件大小到 NeedlePaddingSize 边界
//
// 参数：
//   - f: 已打开的文件句柄（必须是可读写模式）
//
// 返回：
//   - *DiskFile: 封装后的磁盘文件对象
//
// 注意：
//   - 如果 Stat() 失败会直接 Fatal 退出（这通常表示严重的系统问题）
//   - fileSize 会向上对齐到 8 字节边界，确保 Needle 写入时边界对齐
func NewDiskFile(f *os.File) *DiskFile {
	// 获取文件的元数据信息（大小、修改时间等）
	stat, err := f.Stat()
	if err != nil {
		// Stat 失败通常是严重错误（如文件已删除、权限问题），直接退出
		glog.Fatalf("stat file %s: %v", f.Name(), err)
	}

	// 获取当前文件的实际大小
	offset := stat.Size()

	// 将文件大小对齐到 NeedlePaddingSize (8字节) 边界
	// 原因：Needle 存储要求 8 字节对齐，便于快速定位和读取
	// 例如：实际大小 105 字节 -> 对齐到 112 字节
	if offset%NeedlePaddingSize != 0 {
		// 计算需要补齐的字节数并向上对齐
		offset = offset + (NeedlePaddingSize - offset%NeedlePaddingSize)
	}

	return &DiskFile{
		fullFilePath: f.Name(),   // 保存文件完整路径
		File:         f,          // 保存文件句柄
		fileSize:     offset,     // 保存对齐后的文件大小
		modTime:      stat.ModTime(), // 缓存修改时间
	}
}

// ReadAt 从文件的指定偏移位置读取数据到缓冲区
// 实现了 io.ReaderAt 接口，支持并发读取
//
// 参数：
//   - p: 目标缓冲区，用于存放读取的数据
//   - off: 文件偏移量（字节），从 0 开始
//
// 返回：
//   - n: 实际读取的字节数
//   - err: 错误信息，如果成功则为 nil
//
// 特点：
//   - 线程安全：os.File.ReadAt 保证并发安全
//   - 不改变文件指针：适合多协程并发读取
//   - 自动处理 EOF：如果读满缓冲区则不返回 EOF 错误
func (df *DiskFile) ReadAt(p []byte, off int64) (n int, err error) {
	// 检查文件是否已关闭
	if df.File == nil {
		return 0, os.ErrClosed
	}

	// 调用底层 os.File 的 ReadAt 方法
	// 这个方法是线程安全的，不会移动文件指针
	n, err = df.File.ReadAt(p, off)

	// 特殊处理：如果正好读满缓冲区，即使遇到 EOF 也不算错误
	// 这是为了兼容 io.ReaderAt 的语义：读满就算成功
	if err == io.EOF && n == len(p) {
		err = nil // 清除 EOF 错误
	}

	return
}

// WriteAt 在文件的指定偏移位置写入数据
// 实现了 io.WriterAt 接口，支持并发写入（注意：需要外部同步避免重叠写入）
//
// 参数：
//   - p: 要写入的数据缓冲区
//   - off: 文件偏移量（字节），从 0 开始
//
// 返回：
//   - n: 实际写入的字节数
//   - err: 错误信息，如果成功则为 nil
//
// 副作用：
//   - 如果写入成功且超过当前 fileSize，会更新 fileSize 和 modTime 缓存
//   - 这个操作不会自动 Sync，需要调用 Close 或 Sync 才能保证持久化
//
// 注意：
//   - 允许随机写入（可能产生文件空洞）
//   - 并发写入同一位置需要外部加锁
func (df *DiskFile) WriteAt(p []byte, off int64) (n int, err error) {
	// 检查文件是否已关闭
	if df.File == nil {
		return 0, os.ErrClosed
	}

	// 调用底层 os.File 的 WriteAt 方法
	// 这个方法是线程安全的，但并发写入同一位置需要外部同步
	n, err = df.File.WriteAt(p, off)

	// 只有写入成功时才更新缓存
	if err == nil {
		// 计算写入后的水位线（最大偏移量）
		waterMark := off + int64(n)

		// 如果写入位置超过了当前文件大小，更新缓存
		if waterMark > df.fileSize {
			df.fileSize = waterMark // 更新文件大小缓存
			df.modTime = time.Now() // 更新修改时间缓存
		}
	}

	return
}

// Write 在文件末尾追加写入数据（顺序写入）
// 实现了 io.Writer 接口，用于 Needle 的追加写入场景
//
// 参数：
//   - p: 要写入的数据缓冲区
//
// 返回：
//   - n: 实际写入的字节数
//   - err: 错误信息，如果成功则为 nil
//
// 实现：
//   - 直接委托给 WriteAt(p, df.fileSize)
//   - 写入位置由 fileSize 缓存决定，避免 syscall
//   - 适合 Volume 的 Needle 追加写入场景
func (df *DiskFile) Write(p []byte) (n int, err error) {
	// 在当前文件大小位置追加写入
	// WriteAt 会自动更新 fileSize，下次 Write 会继续追加
	return df.WriteAt(p, df.fileSize)
}

// Truncate 截断或扩展文件到指定大小
// 用于 Volume 的压缩（Compact）操作，删除已标记为删除的 Needle
//
// 参数：
//   - off: 目标文件大小（字节）
//
// 返回：
//   - error: 错误信息，如果成功则为 nil
//
// 行为：
//   - 如果 off < fileSize：截断文件，丢弃超出部分的数据
//   - 如果 off > fileSize：扩展文件，填充零字节（可能产生文件空洞）
//   - 如果 off == fileSize：不做任何操作
//
// 副作用：
//   - 成功时会更新 fileSize 和 modTime 缓存
//   - 不会自动 Sync，需要显式调用 Sync 或 Close 保证持久化
func (df *DiskFile) Truncate(off int64) error {
	// 检查文件是否已关闭
	if df.File == nil {
		return os.ErrClosed
	}

	// 调用底层的 Truncate 系统调用
	// 这个操作会立即生效，但可能不会立即写入磁盘
	err := df.File.Truncate(off)

	// 只有成功时才更新缓存
	if err == nil {
		df.fileSize = off       // 更新文件大小缓存为新的大小
		df.modTime = time.Now() // 更新修改时间
	}

	return err
}

// Close 关闭文件并确保数据持久化到磁盘
// 这是一个重要的资源释放操作，必须在文件使用完毕后调用
//
// 返回：
//   - error: 第一个遇到的错误（Sync 或 Close 的错误）
//
// 执行顺序（非常重要）：
//   1. 先调用 Sync() 确保数据持久化到磁盘
//   2. 再调用 File.Close() 释放文件描述符
//   3. 设置 df.File = nil 标记为已关闭
//
// 特点：
//   - 即使 Sync 失败也会尝试 Close（保证资源释放）
//   - 即使 Close 失败也会设置 df.File = nil（防止二次关闭）
//   - 多次调用 Close 是安全的（幂等操作）
//   - 返回第一个遇到的错误
func (df *DiskFile) Close() error {
	// 如果已经关闭，直接返回（幂等操作）
	if df.File == nil {
		return nil
	}

	// 步骤 1: 先刷新数据到磁盘（fsync/fdatasync）
	// 这一步失败不会影响后续的 Close 操作
	err := df.Sync()

	// 步骤 2: 关闭文件描述符，释放系统资源
	var err1 error
	if df.File != nil {
		// 始终尝试关闭文件，即使 Sync 失败
		// Close 会释放文件描述符，防止资源泄漏
		err1 = df.File.Close()
	}

	// 步骤 3: 标记为已关闭状态（防止二次操作）
	// 这个操作必须执行，即使前面的步骤失败
	df.File = nil

	// 返回第一个遇到的错误
	if err != nil {
		return err // 返回 Sync 的错误
	}
	if err1 != nil {
		return err1 // 返回 Close 的错误
	}

	return nil // 全部成功
}

// GetStat 获取文件的统计信息（大小和修改时间）
// 直接从缓存返回，避免 syscall，性能很高
//
// 返回：
//   - datSize: 文件大小（字节），已对齐到 8 字节边界
//   - modTime: 文件最后修改时间
//   - err: 错误信息，只有在文件已关闭时才会返回 os.ErrClosed
//
// 特点：
//   - O(1) 时间复杂度，从内存缓存直接返回
//   - 不会触发 syscall，性能比 os.File.Stat() 高很多
//   - 缓存在每次写操作后自动更新，保证准确性
//
// 注意：
//   - 如果有外部进程修改文件，缓存可能不准确
//   - datSize 是对齐后的逻辑大小，可能大于实际物理大小
func (df *DiskFile) GetStat() (datSize int64, modTime time.Time, err error) {
	// 检查文件是否已关闭
	if df.File == nil {
		err = os.ErrClosed
	}

	// 直接返回缓存值，无需 syscall
	return df.fileSize, df.modTime, err
}

// Name 返回文件的完整路径
// 用于日志记录、错误报告和调试
//
// 返回：
//   - string: 文件的绝对路径（例如：/data/volume1/3.dat）
//
// 特点：
//   - 路径在 NewDiskFile 时就已确定，不会改变
//   - 即使文件已关闭也可以调用
func (df *DiskFile) Name() string {
	return df.fullFilePath
}

// Sync 将文件数据和元数据刷新到磁盘
// 确保所有写入操作持久化，防止系统崩溃导致数据丢失
//
// 返回：
//   - error: 错误信息，如果成功则为 nil
//
// 行为：
//   - Linux: 调用 fsync() 系统调用，同步数据和元数据
//   - macOS: 直接返回 nil（因为 macOS 的 fsync 行为不可靠）
//
// 性能考虑：
//   - Sync 是昂贵的 I/O 操作，会阻塞直到数据写入磁盘
//   - 通常只在关闭文件或重要检查点时调用
//   - 频繁调用会严重影响写入性能
//
// macOS 特殊处理：
//   - macOS 的 fsync 不能保证数据持久化（Apple 文档）
//   - 需要使用 F_FULLFSYNC fcntl，但 SeaweedFS 选择跳过
//   - 这是性能和可靠性的权衡
func (df *DiskFile) Sync() error {
	// 检查文件是否已关闭
	if df.File == nil {
		return os.ErrClosed
	}

	// macOS 特殊处理：跳过 Sync 操作
	// 原因：macOS 的 File.Sync() 调用的 fsync 不能保证持久化
	// 需要使用 F_FULLFSYNC，但会严重影响性能
	if isMac {
		return nil // macOS 直接返回成功
	}

	// Linux/其他平台：调用 fsync 确保数据持久化
	// 这会阻塞直到所有数据写入物理磁盘
	return df.File.Sync()
}
