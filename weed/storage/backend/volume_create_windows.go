//go:build windows
// +build windows

// Package backend 提供 Windows 平台特定的 Volume 文件创建功能
// 支持内存映射（Memory-Mapped File）以提升大文件的读写性能
package backend

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/backend/memory_map"
	"golang.org/x/sys/windows"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend/memory_map/os_overloads"
)

// CreateVolumeFile 在 Windows 平台上创建 Volume 数据文件
// 根据配置可以选择使用内存映射或标准文件 I/O
//
// 参数：
//   - fileName: Volume 文件的完整路径（例如：D:\data\volume1\3.dat）
//   - preallocate: 预分配磁盘空间大小（字节），Windows 不支持，被忽略
//   - memoryMapSizeMB: 内存映射大小（MB），0 表示使用标准 I/O
//
// 返回：
//   - BackendStorageFile: 封装后的存储文件对象
//     * 如果 memoryMapSizeMB > 0: 返回 MemoryMappedFile 实例
//     * 如果 memoryMapSizeMB = 0: 返回 DiskFile 实例
//   - error: 错误信息，如果成功则为 nil
//
// 内存映射模式（memoryMapSizeMB > 0）：
//   - 优点：
//     * 减少用户态/内核态切换，提升小 I/O 性能
//     * 利用操作系统的页缓存机制
//     * 适合频繁随机读写的场景
//   - 缺点：
//     * 占用虚拟地址空间（32位系统可能受限）
//     * 需要更多内存资源
//   - 适用场景：
//     * 小文件（< 100MB）
//     * 热点 Volume（访问频繁）
//     * 内存充足的服务器
//
// 标准文件 I/O 模式（memoryMapSizeMB = 0）：
//   - 优点：
//     * 内存占用小
//     * 适合大文件（> 1GB）
//     * 性能稳定可预测
//   - 适用场景：
//     * 大型 Volume 文件
//     * 内存资源有限
//     * 主要是顺序读写
//
// Windows 平台特性：
//   - 不支持 fallocate，空间预分配被忽略
//   - 使用 os_overloads.OpenFile 替代标准库（处理 Windows 路径和权限）
//   - 文件权限固定为 0644（Windows 会映射到相应的 ACL）
//
// 文件创建标志说明：
//   - windows.O_RDWR: 读写模式
//   - windows.O_CREAT: 文件不存在时创建
//   - windows.O_TRUNC: 清空已存在文件（仅标准 I/O 模式）
func CreateVolumeFile(fileName string, preallocate int64, memoryMapSizeMB uint32) (BackendStorageFile, error) {
	// 检查是否请求了空间预分配
	if preallocate > 0 {
		// Windows 不支持类似 Linux fallocate 的机制
		// 记录警告日志（级别 0，总是显示）
		glog.V(0).Infof("Preallocated disk space for %s is not supported", fileName)
	}

	// 根据 memoryMapSizeMB 参数选择创建模式
	if memoryMapSizeMB > 0 {
		// ========== 内存映射模式 ==========

		// 步骤 1: 打开或创建文件（内存映射模式）
		// 注意：不使用 O_TRUNC 标志
		// 原因：内存映射需要保留文件内容，会在映射时处理大小调整
		// 第 4 个参数 true 表示启用 FILE_FLAG_RANDOM_ACCESS 优化
		file, e := os_overloads.OpenFile(fileName, windows.O_RDWR|windows.O_CREAT, 0644, true)
		if e != nil {
			return nil, e // 文件打开失败
		}

		// 步骤 2: 创建内存映射文件对象
		// memory_map.NewMemoryMappedFile 会：
		//   1. 创建文件映射对象（CreateFileMapping）
		//   2. 将文件映射到进程地址空间（MapViewOfFile）
		//   3. 封装为 BackendStorageFile 接口
		return memory_map.NewMemoryMappedFile(file, memoryMapSizeMB), nil

	} else {
		// ========== 标准文件 I/O 模式 ==========

		// 步骤 1: 打开或创建文件（标准 I/O 模式）
		// O_TRUNC 确保清空已存在的文件（新建 Volume）
		// 第 4 个参数 false 表示使用默认的文件访问优化
		file, e := os_overloads.OpenFile(fileName, windows.O_RDWR|windows.O_CREAT|windows.O_TRUNC, 0644, false)
		if e != nil {
			return nil, e // 文件打开失败
		}

		// 步骤 2: 封装为标准 DiskFile
		// 使用 ReadFile/WriteFile API 进行 I/O
		return NewDiskFile(file), nil
	}

}
