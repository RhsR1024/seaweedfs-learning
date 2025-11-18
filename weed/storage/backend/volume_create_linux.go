//go:build linux
// +build linux

// Package backend 提供 Linux 平台特定的 Volume 文件创建功能
// 利用 Linux 的 fallocate 系统调用进行高效的磁盘空间预分配
package backend

import (
	"os"
	"syscall"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// CreateVolumeFile 在 Linux 平台上创建 Volume 数据文件
// 支持使用 fallocate 系统调用进行磁盘空间预分配，提升性能
//
// 参数：
//   - fileName: Volume 文件的完整路径（例如：/data/volume1/3.dat）
//   - preallocate: 预分配磁盘空间大小（字节），0 表示不预分配
//   - memoryMapSizeMB: 内存映射大小（MB），当前 Linux 实现中被忽略
//
// 返回：
//   - BackendStorageFile: 封装后的存储文件对象（DiskFile 实例）
//   - error: 错误信息，如果成功则为 nil
//
// 磁盘空间预分配的优势：
//   - 减少文件碎片：连续分配磁盘块，提升顺序读写性能
//   - 避免写入时的延迟：预先分配空间，写入时不需要再分配磁盘块
//   - 防止 ENOSPC 错误：提前确保有足够的磁盘空间
//   - 提升大文件写入性能：对于 GB 级别的 Volume 文件特别有效
//
// fallocate 模式说明：
//   - 模式 1 (FALLOC_FL_KEEP_SIZE): 预分配但不改变文件大小
//   - 文件逻辑大小仍为 0，但磁盘空间已分配
//   - 适合追加写入的场景（Volume 写入模式）
//
// 文件创建标志：
//   - O_RDWR: 读写模式打开
//   - O_CREATE: 文件不存在时创建
//   - O_TRUNC: 如果文件已存在，清空内容（用于覆盖旧文件）
func CreateVolumeFile(fileName string, preallocate int64, memoryMapSizeMB uint32) (BackendStorageFile, error) {
	// 步骤 1: 创建或打开文件
	// O_TRUNC 确保如果文件已存在会被清空（新建 Volume）
	file, e := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if e != nil {
		return nil, e // 文件创建失败（权限不足、磁盘满等）
	}

	// 步骤 2: 执行磁盘空间预分配（如果需要）
	if preallocate != 0 {
		// 调用 Linux 的 fallocate 系统调用预分配磁盘空间
		// 参数说明：
		//   - int(file.Fd()): 文件描述符
		//   - 1: 模式标志 FALLOC_FL_KEEP_SIZE（不改变文件大小）
		//   - 0: 起始偏移量（从文件开头开始）
		//   - preallocate: 要分配的字节数
		//
		// 注意：即使 fallocate 失败也继续（只是性能可能略低）
		// fallocate 可能失败的原因：
		//   - 文件系统不支持（如 tmpfs、NFS）
		//   - 磁盘空间不足
		//   - 设备不支持（如某些虚拟化环境）
		syscall.Fallocate(int(file.Fd()), 1, 0, preallocate)

		// 记录预分配信息到日志（级别 1，通常会显示）
		glog.V(1).Infof("Preallocated %d bytes disk space for %s", preallocate, fileName)
	}

	// 注意：memoryMapSizeMB 参数在当前 Linux 实现中被忽略
	// 未来可能会支持 mmap 以提升性能（类似 Windows 实现）

	// 步骤 3: 封装为 DiskFile 并返回
	// NewDiskFile 会读取文件状态并对齐文件大小
	return NewDiskFile(file), nil
}
