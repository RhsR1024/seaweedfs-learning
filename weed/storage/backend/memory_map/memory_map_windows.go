//go:build windows
// +build windows

// Package memory_map Windows 平台的完整内存映射实现
//
// 本文件实现了基于 Windows API 的高性能内存映射文件访问。
// 核心 Windows API:
//   - CreateFileMapping: 创建文件映射对象
//   - MapViewOfFile: 将文件映射到进程地址空间
//   - UnmapViewOfFile: 解除映射
//   - VirtualLock: 锁定内存页，防止换出
//   - VirtualUnlock: 解锁内存页
//
// 性能优化技术：
//   - 分块映射：避免一次性映射大文件，节省虚拟地址空间
//   - 持久写入映射：write_map_views 保持映射，避免重复创建
//   - 临时读取映射：读取后立即释放，节省内存
//   - VirtualLock: 锁定热点内存，防止页面换出
//   - Working Set 调整：提示操作系统优先保留映射内存
//
// 内存管理策略：
//   - chunkSize: 默认为 AllocationGranularity * 128（通常 8MB）
//   - maxMemoryLimit: 限制为物理内存的 80%
//   - 超过限制后不再 VirtualLock（避免内存耗尽）
package memory_map

import (
	"os"
	"reflect"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Windows API 类型别名
type DWORDLONG = uint64 // 64 位无符号整数（Windows DWORDLONG 类型）
type DWORD = uint32     // 32 位无符号整数（Windows DWORD 类型）
type WORD = uint16      // 16 位无符号整数（Windows WORD 类型）

var (
	// 动态加载 kernel32.dll，避免静态链接
	modkernel32 = syscall.NewLazyDLL("kernel32.dll")

	// Windows API 函数句柄（延迟加载）
	procGetSystemInfo            = modkernel32.NewProc("GetSystemInfo")            // 获取系统信息
	procGlobalMemoryStatusEx     = modkernel32.NewProc("GlobalMemoryStatusEx")     // 获取内存状态
	procGetProcessWorkingSetSize = modkernel32.NewProc("GetProcessWorkingSetSize") // 获取进程工作集大小
	procSetProcessWorkingSetSize = modkernel32.NewProc("SetProcessWorkingSetSize") // 设置进程工作集大小
)

var currentProcess, _ = windows.GetCurrentProcess() // 获取当前进程句柄

// currentMinWorkingSet 和 currentMaxWorkingSet 记录当前进程的工作集大小
// Working Set 是进程在物理内存中的页面集合
// 调整 Working Set 可以提示操作系统优先保留这些内存页
var currentMinWorkingSet uint64 = 0 // 最小工作集大小（字节）
var currentMaxWorkingSet uint64 = 0 // 最大工作集大小（字节）

// 初始化：获取当前进程的工作集大小
var _ = getProcessWorkingSetSize(uintptr(currentProcess), &currentMinWorkingSet, &currentMaxWorkingSet)

// systemInfo 缓存系统信息（页面大小、分配粒度等）
var systemInfo, _ = getSystemInfo()

// chunkSize 是每个内存映射块的大小
// 计算：AllocationGranularity * 128
// 典型值：64KB * 128 = 8MB
// 原因：
//   - AllocationGranularity 是 Windows 要求的映射对齐单位（通常 64KB）
//   - 乘以 128 是为了减少映射次数，提升性能
//   - 8MB 是一个平衡值：不会太大占用地址空间，也不会太小导致频繁映射
var chunkSize = uint64(systemInfo.dwAllocationGranularity) * 128

// memoryStatusEx 缓存内存状态信息
var memoryStatusEx, _ = globalMemoryStatusEx()

// maxMemoryLimitBytes 限制内存映射的最大物理内存占用
// 设置为物理内存的 80%，避免耗尽系统内存
// 超过此限制后，不再调用 VirtualLock 锁定内存页
var maxMemoryLimitBytes = uint64(float64(memoryStatusEx.ullTotalPhys) * 0.8)

// CreateMemoryMap 创建文件的内存映射对象
//
// 参数：
//   - file: 已打开的文件句柄
//   - maxLength: 映射的最大长度（字节）
//
// 实现流程：
//   1. 计算对齐后的映射长度（向上对齐到 chunkSize）
//   2. 调用 CreateFileMapping 创建文件映射对象
//   3. 初始化 MemoryMap 结构体
//   4. 预分配 write_map_views 切片（容量 = chunks 数量）
//
// Windows API：
//   CreateFileMapping(hFile, lpAttributes, flProtect, dwMaximumSizeHigh, dwMaximumSizeLow, lpName)
//   - hFile: 文件句柄
//   - lpAttributes: 安全属性（nil 表示默认）
//   - flProtect: 保护标志（PAGE_READWRITE 表示可读写）
//   - dwMaximumSizeHigh/Low: 映射的最大大小（64 位，拆分为高低 32 位）
//   - lpName: 映射对象名称（nil 表示匿名）
//
// 注意：
//   - CreateFileMapping 不会立即分配物理内存
//   - 只是创建映射对象，实际映射在 MapViewOfFile 时发生
//   - 映射大小必须是 AllocationGranularity 的倍数
func (mMap *MemoryMap) CreateMemoryMap(file *os.File, maxLength uint64) {
	// 步骤 1: 计算需要的 chunk 数量
	chunks := (maxLength / chunkSize)
	// 向上取整：如果不能整除，增加一个 chunk
	if chunks*chunkSize < maxLength {
		chunks = chunks + 1
	}

	// 计算对齐后的映射长度（chunkSize 的整数倍）
	alignedMaxLength := chunks * chunkSize

	// 步骤 2: 将 64 位长度拆分为高 32 位和低 32 位
	// Windows API 需要两个 32 位参数表示 64 位长度
	maxLength_high := uint32(alignedMaxLength >> 32)       // 高 32 位
	maxLength_low := uint32(alignedMaxLength & 0xFFFFFFFF) // 低 32 位

	// 步骤 3: 调用 CreateFileMapping 创建文件映射对象
	// PAGE_READWRITE: 允许读写访问
	file_memory_map_handle, err := windows.CreateFileMapping(
		windows.Handle(file.Fd()), // 文件句柄
		nil,                        // 默认安全属性
		windows.PAGE_READWRITE,     // 可读写
		maxLength_high,             // 最大大小高 32 位
		maxLength_low,              // 最大大小低 32 位
		nil,                        // 匿名映射对象
	)

	// 步骤 4: 如果创建成功，初始化 MemoryMap 结构体
	if err == nil {
		mMap.File = file                                      // 保存文件句柄
		mMap.file_memory_map_handle = uintptr(file_memory_map_handle) // 保存映射对象句柄
		// 预分配 write_map_views 切片（容量 = chunks，避免后续扩容）
		mMap.write_map_views = make([]MemoryBuffer, 0, alignedMaxLength/chunkSize)
		mMap.max_length = alignedMaxLength // 保存对齐后的最大长度
		mMap.End_of_file = -1              // 初始文件结束位置为 -1（空文件）
	}
}

// DeleteFileAndMemoryMap 删除文件并释放所有内存映射
//
// 关闭顺序很重要：
//   1. 先关闭文件映射对象句柄（file_memory_map_handle）
//   2. 再关闭文件句柄（File）
//   3. 最后释放所有内存映射视图（write_map_views）
//
// 这个顺序可以防止数据被异步写回磁盘：
//   - 如果先 Unmap 再 Close，Windows 可能会刷新脏页到磁盘
//   - 如果先 Close 再 Unmap，文件已关闭，脏页被丢弃
//   - 这对于删除 Volume 的场景非常重要
//
// 清理步骤：
//   1. CloseHandle(file_memory_map_handle): 释放映射对象
//   2. CloseHandle(File.Fd()): 关闭文件
//   3. releaseMemory(): 释放所有映射视图
//   4. 清空 write_map_views 和 max_length
func (mMap *MemoryMap) DeleteFileAndMemoryMap() {
	// 步骤 1: 关闭文件映射对象句柄
	// 这会阻止新的映射视图创建
	windows.CloseHandle(windows.Handle(mMap.file_memory_map_handle))

	// 步骤 2: 关闭文件句柄
	// 这会告诉操作系统文件即将被删除，不要刷新脏页
	windows.CloseHandle(windows.Handle(mMap.File.Fd()))

	// 步骤 3: 释放所有已映射的视图
	for _, view := range mMap.write_map_views {
		view.releaseMemory() // 调用 UnmapViewOfFile 和 VirtualUnlock
	}

	// 步骤 4: 清空状态
	mMap.write_map_views = nil // 释放切片引用，允许 GC 回收
	mMap.max_length = 0        // 重置最大长度
}

// min 返回两个 uint64 的较小值
// 用于计算写入范围时的边界处理
func min(x, y uint64) uint64 {
	if x < y {
		return x
	}
	return y
}

// WriteMemory 写入数据到内存映射区域
//
// 参数：
//   - offset: 文件偏移量（字节）
//   - length: 写入长度（字节）
//   - data: 要写入的数据
//
// 实现策略：
//   - 写入使用持久映射（write_map_views）
//   - 如果目标 chunk 尚未映射，自动分配
//   - 支持跨 chunk 写入（自动处理分片）
//
// 写入流程：
//   1. 确保所有需要的 chunk 已映射（调用 allocateChunk）
//   2. 计算起始 chunk 索引和偏移量
//   3. 循环写入到各个 chunk（可能跨越多个 chunk）
//   4. 更新 End_of_file（文件逻辑结束位置）
//
// 示例：
//   假设 chunkSize = 8MB，写入 10MB 数据从 offset = 6MB
//   - sliceIndex = 6MB / 8MB = 0（第 0 个 chunk）
//   - sliceOffset = 6MB % 8MB = 6MB（chunk 内偏移）
//   - 第 1 次写入：chunk[0][6MB:8MB] = data[0:2MB]（写入 2MB）
//   - 第 2 次写入：chunk[1][0:8MB] = data[2MB:10MB]（写入 8MB）
func (mMap *MemoryMap) WriteMemory(offset uint64, length uint64, data []byte) {
	// 步骤 1: 确保所有需要的 chunk 已映射
	// 计算最后一个 chunk 的索引：(offset + length) / chunkSize
	for {
		// 检查是否需要分配新的 chunk
		// +1 是因为索引从 0 开始，而 len 是从 1 开始
		if ((offset+length)/chunkSize)+1 > uint64(len(mMap.write_map_views)) {
			allocateChunk(mMap) // 分配一个新的 chunk
		} else {
			break // 所有需要的 chunk 都已分配
		}
	}

	// 步骤 2: 初始化写入状态
	remaining_length := length                      // 剩余要写入的字节数
	sliceIndex := offset / chunkSize                // 起始 chunk 索引
	sliceOffset := offset - (sliceIndex * chunkSize) // 起始 chunk 内偏移
	dataOffset := uint64(0)                         // data 缓冲区的读取偏移

	// 步骤 3: 循环写入数据到各个 chunk
	for {
		// 计算当前 chunk 的写入结束位置
		// min(剩余长度 + chunk 内偏移, chunkSize)
		// 例如：如果剩余 10MB，chunk 内偏移 6MB，chunkSize 8MB
		// 则 writeEnd = min(16MB, 8MB) = 8MB（写满当前 chunk）
		writeEnd := min((remaining_length + sliceOffset), chunkSize)

		// 拷贝数据到 chunk 的缓冲区
		// mMap.write_map_views[sliceIndex].Buffer[sliceOffset:writeEnd] 是目标切片
		// data[dataOffset:] 是源数据
		copy(mMap.write_map_views[sliceIndex].Buffer[sliceOffset:writeEnd], data[dataOffset:])

		// 更新剩余长度（减去已写入的字节数）
		remaining_length -= (writeEnd - sliceOffset)

		// 更新 data 缓冲区的读取偏移
		dataOffset += (writeEnd - sliceOffset)

		// 检查是否还有剩余数据
		if remaining_length > 0 {
			// 移动到下一个 chunk
			sliceIndex += 1
			sliceOffset = 0 // 下一个 chunk 从偏移 0 开始
		} else {
			break // 所有数据都已写入
		}
	}

	// 步骤 4: 更新文件的逻辑结束位置
	// End_of_file 记录最后写入的字节偏移量（从 0 开始）
	// 例如：写入 100 字节到 offset = 0，End_of_file = 99
	if mMap.End_of_file < int64(offset+length-1) {
		mMap.End_of_file = int64(offset + length - 1)
	}
}

// ReadMemory 从内存映射区域读取数据
//
// 参数：
//   - offset: 文件偏移量（字节）
//   - length: 读取长度（字节）
//
// 返回：
//   - dataSlice: 读取的数据（新分配的切片）
//   - err: 错误信息
//
// 实现策略：
//   - 读取使用临时映射（读完立即释放）
//   - 每次读取都创建新的映射视图
//   - 拷贝数据后立即 Unmap，节省内存
//
// 读取流程：
//   1. 分配目标缓冲区（dataSlice）
//   2. 调用 allocate 创建临时映射（read-only）
//   3. 拷贝数据到目标缓冲区
//   4. 释放临时映射（releaseMemory）
//
// 性能考虑：
//   - 每次读取都有 MapViewOfFile + UnmapViewOfFile 开销
//   - 适合不频繁的读取操作
//   - 如果需要频繁读取，考虑使用持久映射
func (mMap *MemoryMap) ReadMemory(offset uint64, length uint64) (dataSlice []byte, err error) {
	// 步骤 1: 分配目标缓冲区
	dataSlice = make([]byte, length)

	// 步骤 2: 创建临时映射（read-only）
	// false 表示只读映射（FILE_MAP_READ）
	mBuffer, err := allocate(windows.Handle(mMap.file_memory_map_handle), offset, length, false)

	// 步骤 3: 拷贝数据到目标缓冲区
	copy(dataSlice, mBuffer.Buffer)

	// 步骤 4: 释放临时映射
	mBuffer.releaseMemory()

	return dataSlice, err
}

// releaseMemory 释放内存映射缓冲区
//
// 执行步骤：
//   1. VirtualUnlock: 解锁内存页（如果之前锁定）
//   2. UnmapViewOfFile: 解除内存映射
//   3. 减少进程工作集大小（提示操作系统）
//   4. 清空 MemoryBuffer 结构体字段
//
// Windows API：
//   - VirtualUnlock: 允许内存页被换出到页面文件
//   - UnmapViewOfFile: 解除映射，释放虚拟地址空间
//   - SetProcessWorkingSetSize: 调整进程工作集大小
//
// 注意：
//   - 必须在使用完映射后调用，否则会泄漏虚拟地址空间
//   - 解锁和 Unmap 的顺序很重要：先 Unlock 再 Unmap
func (mBuffer *MemoryBuffer) releaseMemory() {
	// 步骤 1: 解锁内存页（如果之前锁定）
	// VirtualUnlock 允许页面被换出到页面文件
	windows.VirtualUnlock(mBuffer.aligned_ptr, uintptr(mBuffer.aligned_length))

	// 步骤 2: 解除内存映射
	// 释放虚拟地址空间，允许其他映射使用
	windows.UnmapViewOfFile(mBuffer.aligned_ptr)

	// 步骤 3: 减少进程工作集大小
	// 提示操作系统不再需要这些内存页
	currentMinWorkingSet -= mBuffer.aligned_length
	currentMaxWorkingSet -= mBuffer.aligned_length

	// 只有在限制范围内才调用 SetProcessWorkingSetSize
	// 避免频繁调用系统调用影响性能
	if currentMinWorkingSet < maxMemoryLimitBytes {
		var _ = setProcessWorkingSetSize(uintptr(currentProcess), currentMinWorkingSet, currentMaxWorkingSet)
	}

	// 步骤 4: 清空结构体字段（防止悬空指针）
	mBuffer.ptr = 0
	mBuffer.aligned_ptr = 0
	mBuffer.length = 0
	mBuffer.aligned_length = 0
	mBuffer.Buffer = nil
}

// allocateChunk 为写入操作分配新的内存映射块
//
// 参数：
//   - mMap: MemoryMap 对象
//
// 实现：
//   - 计算新 chunk 的起始偏移量
//   - 调用 allocate 创建映射（write 模式）
//   - 将新 chunk 添加到 write_map_views
//
// 自动扩展：
//   - 每次分配一个 chunkSize 大小的映射
//   - 追加到 write_map_views 切片
//   - 支持动态增长（按需分配）
func allocateChunk(mMap *MemoryMap) {
	// 计算新 chunk 的起始偏移量
	// len(write_map_views) 是已分配的 chunk 数量
	// start = chunk 数量 * chunkSize
	start := uint64(len(mMap.write_map_views)) * chunkSize

	// 分配新的 chunk（write 模式）
	// true 表示可写映射（FILE_MAP_WRITE）
	mBuffer, err := allocate(windows.Handle(mMap.file_memory_map_handle), start, chunkSize, true)

	// 如果分配成功，添加到 write_map_views
	if err == nil {
		mMap.write_map_views = append(mMap.write_map_views, mBuffer)
	}
}

// allocate 分配内存映射缓冲区（核心函数）
//
// 参数：
//   - hMapFile: 文件映射对象句柄
//   - offset: 映射的起始偏移量（字节）
//   - length: 映射的长度（字节）
//   - write: 是否可写（true = FILE_MAP_WRITE, false = FILE_MAP_READ）
//
// 返回：
//   - MemoryBuffer: 映射的缓冲区对象
//   - error: 错误信息
//
// 实现流程：
//   1. 对齐偏移量到 AllocationGranularity 边界
//   2. 计算对齐后的映射长度
//   3. 调用 MapViewOfFile 创建映射
//   4. 增加进程工作集大小（提示操作系统）
//   5. 调用 VirtualLock 锁定内存页（如果在限制范围内）
//   6. 构造 MemoryBuffer 对象（包含 Go 切片）
//
// 内存对齐：
//   - Windows 要求映射偏移量必须是 AllocationGranularity 的倍数
//   - 典型值：64KB（65536 字节）
//   - 如果 offset 不对齐，需要向下对齐并记录偏移差值
//
// 示例：
//   假设 AllocationGranularity = 64KB
//   - offset = 100KB, length = 10KB
//   - start = (100KB / 64KB) * 64KB = 64KB（向下对齐）
//   - diff = 100KB - 64KB = 36KB（偏移差值）
//   - aligned_length = 36KB + 10KB = 46KB
//   - 实际映射：[64KB, 64KB + 46KB)
//   - 可用区域：[100KB, 110KB)（通过 ptr 和 Buffer 暴露）
func allocate(hMapFile windows.Handle, offset uint64, length uint64, write bool) (MemoryBuffer, error) {
	mBuffer := MemoryBuffer{} // 创建空的 MemoryBuffer

	// 步骤 1: 对齐内存分配到系统分配粒度边界
	// dwAllocationGranularity 通常为 64KB（Windows 要求）
	dwSysGran := systemInfo.dwAllocationGranularity

	// 向下对齐到 AllocationGranularity 的倍数
	// 例如：offset = 100KB, dwSysGran = 64KB -> start = 64KB
	start := (offset / uint64(dwSysGran)) * uint64(dwSysGran)

	// 计算偏移差值（需要跳过的字节数）
	diff := offset - start

	// 计算对齐后的映射长度（包含偏移差值）
	aligned_length := diff + length

	// 步骤 2: 将 64 位偏移量拆分为高低 32 位
	offset_high := uint32(start >> 32)       // 高 32 位
	offset_low := uint32(start & 0xFFFFFFFF) // 低 32 位

	// 步骤 3: 确定访问权限
	access := windows.FILE_MAP_READ // 默认只读
	if write {
		access = windows.FILE_MAP_WRITE // 可写
	}

	// 步骤 4: 增加进程工作集大小
	// 提示操作系统优先保留这些内存页在物理内存中
	currentMinWorkingSet += aligned_length
	currentMaxWorkingSet += aligned_length

	if currentMinWorkingSet < maxMemoryLimitBytes {
		// 只有在限制范围内才调整工作集大小
		// 避免耗尽物理内存
		var _ = setProcessWorkingSetSize(uintptr(currentProcess), currentMinWorkingSet, currentMaxWorkingSet)
	}

	// 步骤 5: 调用 MapViewOfFile 创建映射
	// 这是核心 Windows API，将文件映射到进程地址空间
	addr_ptr, errno := windows.MapViewOfFile(
		hMapFile,                  // 文件映射对象句柄
		uint32(access),            // 访问权限（读/写）
		offset_high,               // 偏移量高 32 位
		offset_low,                // 偏移量低 32 位
		uintptr(aligned_length),   // 映射长度
	)

	// 检查映射是否成功
	if addr_ptr == 0 {
		return mBuffer, errno // 映射失败，返回错误
	}

	// 步骤 6: 锁定内存页（如果在限制范围内）
	// VirtualLock 防止页面被换出到页面文件，提升性能
	if currentMinWorkingSet < maxMemoryLimitBytes {
		windows.VirtualLock(mBuffer.aligned_ptr, uintptr(mBuffer.aligned_length))
	}

	// 步骤 7: 填充 MemoryBuffer 结构体
	mBuffer.aligned_ptr = addr_ptr         // 对齐后的起始地址
	mBuffer.aligned_length = aligned_length // 对齐后的长度
	mBuffer.ptr = addr_ptr + uintptr(diff)  // 实际数据的起始地址（跳过偏移差值）
	mBuffer.length = length                 // 实际可用的长度

	// 步骤 8: 构造 Go 切片指向映射的内存
	// 使用 unsafe 将映射的内存包装为 []byte 切片（零拷贝）
	slice_header := (*reflect.SliceHeader)(unsafe.Pointer(&mBuffer.Buffer))
	slice_header.Data = addr_ptr + uintptr(diff) // 切片数据指针（跳过偏移差值）
	slice_header.Len = int(length)               // 切片长度
	slice_header.Cap = int(length)               // 切片容量

	return mBuffer, nil
}

// ========== Windows API 结构体和函数封装 ==========

// _MEMORYSTATUSEX Windows 内存状态结构体
// 对应 Windows API: MEMORYSTATUSEX
// 文档: https://docs.microsoft.com/en-gb/windows/win32/api/sysinfoapi/ns-sysinfoapi-memorystatusex
//
// 字段说明：
//   - dwLength: 结构体大小（字节）
//   - dwMemoryLoad: 内存使用率（0-100）
//   - ullTotalPhys: 物理内存总量（字节）
//   - ullAvailPhys: 可用物理内存（字节）
//   - ullTotalPageFile: 页面文件总量（字节）
//   - ullAvailPageFile: 可用页面文件（字节）
//   - ullTotalVirtual: 虚拟地址空间总量（字节）
//   - ullAvailVirtual: 可用虚拟地址空间（字节）
//   - ullAvailExtendedVirtual: 扩展虚拟地址空间（保留）
type _MEMORYSTATUSEX struct {
	dwLength                DWORD     // 结构体大小
	dwMemoryLoad            DWORD     // 内存使用率（百分比）
	ullTotalPhys            DWORDLONG // 物理内存总量
	ullAvailPhys            DWORDLONG // 可用物理内存
	ullTotalPageFile        DWORDLONG // 页面文件总量
	ullAvailPageFile        DWORDLONG // 可用页面文件
	ullTotalVirtual         DWORDLONG // 虚拟地址空间总量
	ullAvailVirtual         DWORDLONG // 可用虚拟地址空间
	ullAvailExtendedVirtual DWORDLONG // 扩展虚拟地址空间
}

// globalMemoryStatusEx 获取系统内存状态
// 封装 Windows API: GlobalMemoryStatusEx
// 文档: https://docs.microsoft.com/en-gb/windows/win32/api/sysinfoapi/nf-sysinfoapi-globalmemorystatusex
//
// 返回：
//   - _MEMORYSTATUSEX: 内存状态信息
//   - error: 错误信息
//
// 使用场景：
//   - 获取物理内存总量（用于计算 maxMemoryLimitBytes）
//   - 检查可用内存（避免内存耗尽）
func globalMemoryStatusEx() (_MEMORYSTATUSEX, error) {
	var mem_status _MEMORYSTATUSEX

	// 设置结构体大小（Windows API 要求）
	mem_status.dwLength = uint32(unsafe.Sizeof(mem_status))

	// 调用 GlobalMemoryStatusEx
	_, _, err := procGlobalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&mem_status)))

	// 检查错误（Windows API 约定：Errno(0) 表示成功）
	if err != syscall.Errno(0) {
		return mem_status, err
	}

	return mem_status, nil
}

// _SYSTEM_INFO Windows 系统信息结构体
// 对应 Windows API: SYSTEM_INFO
// 文档: https://docs.microsoft.com/en-gb/windows/win32/api/sysinfoapi/ns-sysinfoapi-system_info
//
// 字段说明：
//   - dwOemId: OEM ID（已弃用）
//   - dwPageSize: 页面大小（通常 4KB）
//   - lpMinimumApplicationAddress: 最小应用程序地址
//   - lpMaximumApplicationAddress: 最大应用程序地址
//   - dwActiveProcessorMask: 活动处理器掩码
//   - dwNumberOfProcessors: 处理器数量
//   - dwProcessorType: 处理器类型（已弃用）
//   - dwAllocationGranularity: 内存分配粒度（通常 64KB）
//   - wProcessorLevel: 处理器级别
//   - wProcessorRevision: 处理器版本
type _SYSTEM_INFO struct {
	dwOemId                     DWORD   // OEM ID（已弃用，但保留兼容性）
	dwPageSize                  DWORD   // 页面大小（字节）
	lpMinimumApplicationAddress uintptr // 最小应用程序地址
	lpMaximumApplicationAddress uintptr // 最大应用程序地址
	dwActiveProcessorMask       uintptr // 活动处理器掩码
	dwNumberOfProcessors        DWORD   // 处理器数量
	dwProcessorType             DWORD   // 处理器类型
	dwAllocationGranularity     DWORD   // 内存分配粒度（重要！）
	wProcessorLevel             WORD    // 处理器级别
	wProcessorRevision          WORD    // 处理器版本
}

// getSystemInfo 获取系统信息
// 封装 Windows API: GetSystemInfo
// 文档: https://docs.microsoft.com/en-us/windows/win32/api/sysinfoapi/nf-sysinfoapi-getsysteminfo
//
// 返回：
//   - _SYSTEM_INFO: 系统信息
//   - error: 错误信息
//
// 使用场景：
//   - 获取 AllocationGranularity（内存映射对齐要求）
//   - 获取 PageSize（用于性能优化）
func getSystemInfo() (_SYSTEM_INFO, error) {
	var si _SYSTEM_INFO

	// 调用 GetSystemInfo（无返回值，通过指针填充）
	_, _, err := procGetSystemInfo.Call(uintptr(unsafe.Pointer(&si)))

	// 检查错误
	if err != syscall.Errno(0) {
		return si, err
	}

	return si, nil
}

// getProcessWorkingSetSize 获取进程工作集大小
// 封装 Windows API: GetProcessWorkingSetSize
// 文档: https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-getprocessworkingsetsize
//
// 参数：
//   - process: 进程句柄
//   - dwMinWorkingSet: 最小工作集大小（输出）
//   - dwMaxWorkingSet: 最大工作集大小（输出）
//
// 返回：
//   - error: 错误信息
//
// Working Set 说明：
//   - Working Set 是进程在物理内存中的页面集合
//   - MinWorkingSet: 系统保证至少保留的内存页数
//   - MaxWorkingSet: 系统允许的最大内存页数
func getProcessWorkingSetSize(process uintptr, dwMinWorkingSet *uint64, dwMaxWorkingSet *uint64) error {
	r1, _, err := syscall.Syscall(
		procGetProcessWorkingSetSize.Addr(), // 函数地址
		3,                                    // 参数数量
		process,                              // 进程句柄
		uintptr(unsafe.Pointer(dwMinWorkingSet)), // 最小工作集（输出）
		uintptr(unsafe.Pointer(dwMaxWorkingSet)), // 最大工作集（输出）
	)

	// 检查返回值（0 表示失败）
	if r1 == 0 {
		if err != syscall.Errno(0) {
			return err
		}
	}

	return nil
}

// setProcessWorkingSetSize 设置进程工作集大小
// 封装 Windows API: SetProcessWorkingSetSize
// 文档: https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-setprocessworkingsetsize
//
// 参数：
//   - process: 进程句柄
//   - dwMinWorkingSet: 最小工作集大小（字节）
//   - dwMaxWorkingSet: 最大工作集大小（字节）
//
// 返回：
//   - error: 错误信息
//
// 作用：
//   - 提示操作系统优先保留这些内存页在物理内存中
//   - 减少页面换出到页面文件的概率
//   - 提升内存映射的性能
//
// 注意：
//   - 这只是一个提示，不是强制要求
//   - 操作系统可能因内存压力而忽略此设置
//   - 不要设置过大的值，否则可能耗尽物理内存
func setProcessWorkingSetSize(process uintptr, dwMinWorkingSet uint64, dwMaxWorkingSet uint64) error {
	r1, _, err := syscall.Syscall(
		procSetProcessWorkingSetSize.Addr(), // 函数地址
		3,                                    // 参数数量
		process,                              // 进程句柄
		uintptr(dwMinWorkingSet),             // 最小工作集
		uintptr(dwMaxWorkingSet),             // 最大工作集
	)

	// 检查返回值（0 表示失败）
	if r1 == 0 {
		if err != syscall.Errno(0) {
			return err
		}
	}

	return nil
}
