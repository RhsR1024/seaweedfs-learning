//go:build !linux && !windows
// +build !linux,!windows

// Package backend 提供跨平台的 Volume 文件创建功能
// 本文件是通用实现，用于 Linux 和 Windows 以外的操作系统（如 macOS、BSD）
package backend

import (
	"os"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// CreateVolumeFile 在非 Linux/Windows 平台上创建 Volume 数据文件
// 这是一个通用实现，不支持高级特性如磁盘空间预分配和内存映射
//
// 参数：
//   - fileName: Volume 文件的完整路径（例如：/data/volume1/3.dat）
//   - preallocate: 预分配磁盘空间大小（字节），在此平台上被忽略
//   - memoryMapSizeMB: 内存映射大小（MB），在此平台上被忽略
//
// 返回：
//   - BackendStorageFile: 封装后的存储文件对象（DiskFile 实例）
//   - error: 错误信息，如果成功则为 nil
//
// 平台特性：
//   - macOS/BSD: 不支持 fallocate，空间预分配被忽略
//   - 不支持内存映射，始终使用标准文件 I/O
//   - 文件权限固定为 0644（rw-r--r--）
//
// 文件创建标志：
//   - O_RDWR: 读写模式打开
//   - O_CREATE: 文件不存在时创建
//   - O_TRUNC: 如果文件已存在，清空内容（用于覆盖旧文件）
func CreateVolumeFile(fileName string, preallocate int64, memoryMapSizeMB uint32) (BackendStorageFile, error) {
	// 以读写模式创建或打开文件
	// O_TRUNC 确保如果文件已存在会被清空（覆盖模式）
	file, e := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if e != nil {
		return nil, e // 文件创建失败（权限不足、磁盘满等）
	}

	// 检查是否请求了空间预分配
	if preallocate > 0 {
		// macOS/BSD 不支持 fallocate 系统调用，记录日志提示
		// 虽然不预分配，文件仍然可以正常使用，只是性能可能略低
		glog.V(2).Infof("Preallocated disk space for %s is not supported", fileName)
	}

	// 注意：memoryMapSizeMB 参数在此平台被完全忽略
	// 始终返回标准的 DiskFile，使用 pread/pwrite 系统调用

	// 封装为 DiskFile 并返回
	return NewDiskFile(file), nil
}
