// Package memory_map 实现基于内存映射的文件 I/O（仅 Windows 平台完整支持）
//
// 内存映射 (Memory-Mapped File) 是一种高性能文件访问技术，将文件内容直接映射到进程的虚拟地址空间，
// 允许像访问内存一样访问文件，避免频繁的系统调用和数据拷贝。
//
// 核心优势：
//   - 减少用户态/内核态切换：直接通过内存访问文件
//   - 利用操作系统页缓存：自动缓存热点数据
//   - 延迟写入：操作系统异步将脏页写回磁盘
//   - 适合随机访问：O(1) 访问任意位置
//
// 适用场景：
//   - 小型 Volume 文件（< 100MB）
//   - 频繁随机读写的场景
//   - 内存充足的服务器
//
// 平台支持：
//   - Windows: 完整实现，使用 CreateFileMapping + MapViewOfFile
//   - Linux/macOS: 存根实现，返回错误（使用 DiskFile 替代）
package memory_map

import (
	"os"
	"strconv"
)

// MemoryBuffer 表示一个内存映射缓冲区
//
// 在 Windows 上，文件被分割成多个固定大小的 chunk，每个 chunk 对应一个 MemoryBuffer。
// 这种分块设计可以：
//   - 避免一次性映射整个大文件（节省虚拟地址空间）
//   - 支持按需加载（只映射正在使用的 chunk）
//   - 更好地控制内存使用
//
// 字段说明：
//   - aligned_length: 对齐后的内存区域长度（按系统分配粒度对齐，通常为 64KB）
//   - length: 实际可用的数据长度（可能小于 aligned_length）
//   - aligned_ptr: 对齐后的内存起始地址（MapViewOfFile 返回的地址）
//   - ptr: 实际数据的起始地址（aligned_ptr + offset）
//   - Buffer: Go 切片，指向映射的内存区域，用于读写数据
//
// 内存对齐原因：
//   - Windows 要求映射地址必须是 dwAllocationGranularity 的倍数
//   - 典型值为 64KB（65536 字节）
//   - 对齐后才能成功调用 MapViewOfFile
type MemoryBuffer struct {
	aligned_length uint64  // 对齐后的内存区域长度（字节）
	length         uint64  // 实际可用的数据长度（字节）
	aligned_ptr    uintptr // 对齐后的内存起始地址
	ptr            uintptr // 实际数据的起始地址（可能偏移）
	Buffer         []byte  // 指向映射内存的 Go 切片（零拷贝访问）
}

// MemoryMap 表示一个内存映射文件
//
// 封装了 Windows 的文件映射 API，提供高性能的文件访问能力。
// 设计采用分块映射策略，将大文件分割成多个小块（chunk），按需映射。
//
// 字段说明：
//   - File: 底层的文件句柄
//   - file_memory_map_handle: Windows 文件映射对象句柄（CreateFileMapping 返回）
//   - write_map_views: 写入视图列表，每个元素对应一个映射的 chunk
//   - max_length: 文件映射的最大长度（字节）
//   - End_of_file: 文件的逻辑结束位置（最后写入的字节偏移量）
//
// 工作流程：
//   1. CreateMemoryMap: 创建文件映射对象（指定最大大小）
//   2. WriteMemory: 按需分配 chunk，写入数据
//   3. ReadMemory: 临时映射读取区域，读取后立即释放
//   4. DeleteFileAndMemoryMap: 关闭句柄，释放所有内存映射
//
// 性能优化：
//   - 写入使用持久映射（write_map_views），避免重复映射
//   - 读取使用临时映射，读完立即释放，节省内存
//   - 使用 VirtualLock 锁定内存页，防止被换出到页面文件
type MemoryMap struct {
	File                   *os.File       // 底层文件句柄
	file_memory_map_handle uintptr        // Windows 文件映射对象句柄（HANDLE）
	write_map_views        []MemoryBuffer // 已映射的写入缓冲区列表（分块存储）
	max_length             uint64         // 文件映射的最大长度（字节）
	End_of_file            int64          // 文件的逻辑结束位置（-1 表示空文件）
}

// ReadMemoryMapMaxSizeMb 解析内存映射最大大小配置字符串
//
// 用于从配置文件或命令行参数中读取内存映射大小设置。
//
// 参数：
//   - memoryMapMaxSizeMbString: 内存映射大小字符串（单位 MB），例如 "100" 表示 100MB
//
// 返回：
//   - uint32: 解析后的 MB 数值
//   - error: 解析错误（如格式不正确、超出范围）
//
// 特殊值：
//   - "" (空字符串): 返回 0，表示禁用内存映射（使用标准文件 I/O）
//   - "0": 返回 0，表示禁用内存映射
//   - "100": 返回 100，表示最大映射 100MB 文件
//
// 使用示例：
//   size, err := ReadMemoryMapMaxSizeMb("100")  // size = 100
//   size, err := ReadMemoryMapMaxSizeMb("")     // size = 0 (禁用)
//   size, err := ReadMemoryMapMaxSizeMb("abc")  // err != nil (格式错误)
func ReadMemoryMapMaxSizeMb(memoryMapMaxSizeMbString string) (uint32, error) {
	// 空字符串表示禁用内存映射
	if memoryMapMaxSizeMbString == "" {
		return 0, nil
	}

	// 解析为 64 位无符号整数
	memoryMapMaxSize64, err := strconv.ParseUint(memoryMapMaxSizeMbString, 10, 32)

	// 转换为 32 位（SeaweedFS 的 Volume 大小不会超过 uint32 范围）
	return uint32(memoryMapMaxSize64), err
}
