//go:build !windows
// +build !windows

// Package memory_map 非 Windows 平台的存根实现
//
// 本文件为 Linux、macOS 等非 Windows 平台提供空实现。
// 由于内存映射的 API 在不同平台差异很大，SeaweedFS 选择：
//   - Windows: 完整实现内存映射（memory_map_windows.go）
//   - 其他平台: 存根实现（本文件），返回错误或空操作
//
// 原因：
//   - Linux/macOS 的 mmap 实现复杂度较高
//   - 标准 DiskFile 在 Linux 上性能已经足够好
//   - 避免维护多平台的内存映射代码
package memory_map

import (
	"fmt"
	"os"
)

// CreateMemoryMap 在非 Windows 平台上创建内存映射（存根实现）
//
// 参数：
//   - file: 文件句柄
//   - maxLength: 映射的最大长度（字节）
//
// 实现：
//   - 空操作，不执行任何实际的映射
//   - 不会报错，静默失败
//
// 注意：
//   - 调用此方法后，mMap 对象仍然可用
//   - 但后续的 WriteMemory/ReadMemory 会失败
//   - 应该在非 Windows 平台使用 DiskFile 替代
func (mMap *MemoryMap) CreateMemoryMap(file *os.File, maxLength uint64) {
	// 空操作：非 Windows 平台不实现内存映射
}

// WriteMemory 在非 Windows 平台上写入内存映射（存根实现）
//
// 参数：
//   - offset: 文件偏移量（字节）
//   - length: 写入长度（字节）
//   - data: 要写入的数据
//
// 实现：
//   - 空操作，数据会被丢弃
//   - 不会报错，静默失败
//
// 注意：
//   - 在非 Windows 平台调用此方法不会有任何效果
//   - 应该在创建 Volume 时检测平台并使用 DiskFile
func (mMap *MemoryMap) WriteMemory(offset uint64, length uint64, data []byte) {
	// 空操作：非 Windows 平台不实现内存映射
}

// ReadMemory 在非 Windows 平台上读取内存映射（存根实现）
//
// 参数：
//   - offset: 文件偏移量（字节）
//   - length: 读取长度（字节）
//
// 返回：
//   - []byte: 空切片
//   - error: 错误信息，说明当前平台不支持内存映射
//
// 实现：
//   - 返回空切片和错误
//   - 告知调用者内存映射未实现
//
// 错误处理：
//   - 上层代码应该捕获此错误并回退到 DiskFile
//   - 或者在创建 Volume 时就避免使用内存映射
func (mMap *MemoryMap) ReadMemory(offset uint64, length uint64) ([]byte, error) {
	dataSlice := []byte{} // 空切片
	// 返回错误：提示内存映射未实现
	return dataSlice, fmt.Errorf("Memory Map not implemented for this platform")
}

// DeleteFileAndMemoryMap 在非 Windows 平台上删除内存映射（存根实现）
//
// 实现：
//   - 空操作，不执行任何清理
//   - 因为没有实际创建映射，无需释放资源
//
// 注意：
//   - 在非 Windows 平台调用此方法不会有任何效果
//   - 文件句柄需要由调用者单独关闭
func (mBuffer *MemoryMap) DeleteFileAndMemoryMap() {
	// 空操作：非 Windows 平台不实现内存映射，无需清理
}
