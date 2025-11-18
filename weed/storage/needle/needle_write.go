// Package needle 实现 Needle 的写入操作
// Needle 写入模块负责将 Needle 数据追加到 Volume 文件中
package needle

import (
	"bytes"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/buffer_pool"
)

// Append 将 Needle 追加到 Volume 文件末尾
// 这是 Needle 写入的核心方法，采用追加写模式
//
// 参数:
//   - w: 后端存储文件接口
//   - version: Volume 版本号（影响数据格式）
//
// 返回值:
//   - offset: 写入的起始偏移量（字节）
//   - size: Needle 的逻辑大小（不含 padding）
//   - actualSize: 实际写入的字节数（含 padding）
//   - err: 错误信息
//
// 工作流程:
//  1. 获取文件当前大小作为写入偏移量
//  2. 检查 Volume 容量限制（MaxPossibleVolumeSize）
//  3. 从缓冲池获取临时 buffer
//  4. 根据版本序列化 Needle 到 buffer
//  5. 将 buffer 写入文件
//  6. 如果失败则回滚（truncate 到原大小）
//
// 注意:
//   - 使用 buffer pool 减少内存分配
//   - 写入失败时会尝试截断文件恢复
//   - 这是一个原子操作（要么全部成功，要么回滚）
func (n *Needle) Append(w backend.BackendStorageFile, version Version) (offset uint64, size Size, actualSize int64, err error) {
	// 获取文件当前大小（将作为写入偏移量）
	end, _, e := w.GetStat()
	if e != nil {
		err = fmt.Errorf("Cannot Read Current Volume Position: %w", e)
		return
	}
	offset = uint64(end)

	// 检查 Volume 容量限制（默认 256GB）
	// 如果数据为空（删除标记）则允许超过限制
	if offset >= MaxPossibleVolumeSize && len(n.Data) != 0 {
		err = fmt.Errorf("Volume Size %d Exceeded %d", offset, MaxPossibleVolumeSize)
		return
	}

	// 从对象池获取可重用的 buffer，避免频繁 GC
	bytesBuffer := buffer_pool.SyncPoolGetBuffer()
	defer func() {
		// 如果写入失败，截断文件到原大小
		if err != nil {
			if te := w.Truncate(end); te != nil {
				// handle error or log
			}
		}
		// 归还 buffer 到对象池
		buffer_pool.SyncPoolPutBuffer(bytesBuffer)
	}()

	// 根据 Volume 版本序列化 Needle 到 buffer
	// size: 逻辑大小，actualSize: 实际字节数（含 padding）
	size, actualSize, err = writeNeedleByVersion(version, n, offset, bytesBuffer)
	if err != nil {
		return
	}

	// 将 buffer 写入文件指定偏移量
	_, err = w.WriteAt(bytesBuffer.Bytes(), int64(offset))
	if err != nil {
		err = fmt.Errorf("failed to write %d bytes to %s at offset %d: %w", actualSize, w.Name(), offset, err)
	}

	return offset, size, actualSize, err
}

// WriteNeedleBlob 直接写入预序列化的 Needle 二进制数据
// 这是一个高性能的写入方法，跳过序列化步骤
//
// 参数:
//   - w: 后端存储文件接口
//   - dataSlice: 已序列化的 Needle 二进制数据
//   - size: Needle 的数据大小（用于计算时间戳位置）
//   - appendAtNs: 追加时间戳（纳秒，仅 Version3 使用）
//   - version: Volume 版本号
//
// 返回值:
//   - offset: 写入的起始偏移量
//   - err: 错误信息
//
// 使用场景:
//   - EC (Erasure Coding) 重建：直接写入恢复的数据
//   - 副本同步：直接写入从主节点获取的原始数据
//   - 数据迁移：避免重复序列化/反序列化
//
// 注意:
//   - dataSlice 必须是完整的 Needle 格式（含 header/checksum/padding）
//   - Version3 会在指定位置更新时间戳
//   - 失败时自动回滚到原文件大小
func WriteNeedleBlob(w backend.BackendStorageFile, dataSlice []byte, size Size, appendAtNs uint64, version Version) (offset uint64, err error) {

	// 获取文件当前大小，并设置错误恢复机制
	if end, _, e := w.GetStat(); e == nil {
		defer func(w backend.BackendStorageFile, off int64) {
			// 如果写入失败，截断文件到原大小
			if err != nil {
				if te := w.Truncate(end); te != nil {
					glog.V(0).Infof("Failed to truncate %s back to %d with error: %v", w.Name(), end, te)
				}
			}
		}(w, end)
		offset = uint64(end)
	} else {
		err = fmt.Errorf("Cannot Read Current Volume Position: %v", e)
		return
	}

	// Version3 需要更新时间戳字段
	// 时间戳位置 = Header(16) + Data + Checksum(4)
	if version == Version3 {
		tsOffset := NeedleHeaderSize + size + NeedleChecksumSize
		util.Uint64toBytes(dataSlice[tsOffset:tsOffset+TimestampSize], appendAtNs)
	}

	// 直接写入二进制数据
	if err == nil {
		_, err = w.WriteAt(dataSlice, int64(offset))
	}

	return

}

// prepareNeedleWrite 封装所有版本 writeNeedle 函数的通用前置逻辑
// 用于减少代码重复，统一错误处理和资源管理
//
// 参数:
//   - w: 后端存储文件接口
//   - n: 要写入的 Needle 对象
//
// 返回值:
//   - offset: 写入的起始偏移量
//   - bytesBuffer: 从对象池获取的临时缓冲区
//   - cleanup: 清理函数（负责回滚和归还 buffer）
//   - err: 错误信息
//
// 工作流程:
//  1. 获取文件当前大小作为写入偏移量
//  2. 检查 Volume 容量限制
//  3. 从 buffer pool 获取临时缓冲区
//  4. 返回清理函数（调用者在 defer 中使用）
//
// 使用示例:
//
//	offset, buf, cleanup, err := prepareNeedleWrite(w, n)
//	if err != nil {
//	    return err
//	}
//	defer cleanup(err)
//	// ... 执行实际写入 ...
//
// 注意:
//   - cleanup 函数必须在 defer 中调用
//   - cleanup 会根据 err 参数决定是否回滚
func prepareNeedleWrite(w backend.BackendStorageFile, n *Needle) (offset uint64, bytesBuffer *bytes.Buffer, cleanup func(err error), err error) {
	// 获取文件当前大小（将作为写入偏移量）
	end, _, e := w.GetStat()
	if e != nil {
		err = fmt.Errorf("Cannot Read Current Volume Position: %w", e)
		return
	}
	offset = uint64(end)

	// 检查 Volume 容量限制（默认 256GB）
	// 如果数据为空（删除标记）则允许超过限制
	if offset >= MaxPossibleVolumeSize && len(n.Data) != 0 {
		err = fmt.Errorf("Volume Size %d Exceeded %d", offset, MaxPossibleVolumeSize)
		return
	}

	// 从对象池获取可重用的 buffer
	bytesBuffer = buffer_pool.SyncPoolGetBuffer()

	// 创建清理函数，用于错误回滚和资源释放
	cleanup = func(err error) {
		// 如果写入失败，截断文件到原大小
		if err != nil {
			if te := w.Truncate(end); te != nil {
				// handle error or log
			}
		}
		// 归还 buffer 到对象池
		buffer_pool.SyncPoolPutBuffer(bytesBuffer)
	}
	return
}
