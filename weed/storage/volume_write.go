// Package storage 实现 SeaweedFS 的存储引擎
// 本文件包含 Volume 的写入、删除操作，以及异步写入机制
//
// 核心概念:
//  1. Needle: 最小存储单元，包含文件数据和元数据
//  2. Volume: 由多个 Needle 组成的大文件（.dat 文件）
//  3. Needle Map: Needle ID 到磁盘偏移量的索引（.idx 文件）
//  4. 同步写入 vs 异步写入: 是否立即 fsync 到磁盘
//
// 文件结构:
//   - *.dat: 数据文件，存储所有 Needle
//   - *.idx: 索引文件，存储 Needle 的位置信息
//   - *.cpd/.cpx: Compaction 临时文件
//
// 写入流程:
//  1. 检查文件是否未改变（幂等性）
//  2. 验证 Cookie（防止错误覆盖）
//  3. 追加数据到 .dat 文件
//  4. 更新 .idx 索引
//  5. 可选：fsync 刷盘
package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// 预定义错误类型
var ErrorNotFound = errors.New("not found")         // Needle 不存在
var ErrorDeleted = errors.New("already deleted")    // Needle 已被删除
var ErrorSizeMismatch = errors.New("size mismatch") // 大小不匹配

// checkReadWriteError 检查并记录 IO 错误
// 用于检测磁盘硬件故障（如 EIO 错误）
//
// 参数:
//   - err: IO 操作的错误
//
// 工作原理:
//   - 如果检测到 EIO (Input/Output Error)，记录到 v.lastIoError
//   - 如果操作成功，清除之前的 IO 错误
//   - 用于监控磁盘健康状态
func (v *Volume) checkReadWriteError(err error) {
	if err == nil {
		// 操作成功，清除错误状态
		if v.lastIoError != nil {
			v.lastIoError = nil
		}
		return
	}
	// 检查是否是 IO 错误（通常表示硬件故障）
	if errors.Is(err, syscall.EIO) {
		v.lastIoError = err
	}
}

// isFileUnchanged 检查要写入的 Needle 是否与现有 Needle 完全相同
// 用于实现幂等上传：如果文件内容未改变，无需重复写入
//
// 参数:
//   - n: 要写入的 Needle
//
// 返回值:
//   - bool: true 表示文件未改变，false 表示是新文件或内容已改变
//
// 工作原理:
//  1. 如果 Volume 设置了 TTL，总是写入（因为 TTL 可能改变）
//  2. 从 Needle Map 查找现有 Needle 的位置
//  3. 读取现有 Needle 的数据
//  4. 比较 Cookie、Checksum 和数据内容
//  5. 如果完全相同，返回 true
//
// 注意:
//   - 此方法要求在同一个 Volume 内串行访问
//   - 用于优化重复上传，避免浪费磁盘空间
func (v *Volume) isFileUnchanged(n *needle.Needle) bool {
	// 如果 Volume 设置了 TTL，不检查是否未改变
	// 因为即使内容相同，TTL 也可能不同
	if v.Ttl.String() != "" {
		return false
	}

	// 从 Needle Map 中查找现有 Needle 的元数据
	nv, ok := v.nm.Get(n.Id)
	if ok && !nv.Offset.IsZero() && nv.Size.IsValid() {
		// 找到现有 Needle，读取其完整数据
		oldNeedle := new(needle.Needle)
		err := oldNeedle.ReadData(v.DataBackend, nv.Offset.ToActualOffset(), nv.Size, v.Version())
		if err != nil {
			glog.V(0).Infof("Failed to check updated file at offset %d size %d: %v", nv.Offset.ToActualOffset(), nv.Size, err)
			return false
		}
		// 比较三个关键属性：Cookie、Checksum、Data
		// 只有三者完全相同，才认为文件未改变
		if oldNeedle.Cookie == n.Cookie && oldNeedle.Checksum == n.Checksum && bytes.Equal(oldNeedle.Data, n.Data) {
			n.DataSize = oldNeedle.DataSize // 复用旧的 DataSize
			return true
		}
	}
	return false
}

var ErrVolumeNotEmpty = fmt.Errorf("volume not empty") // Volume 不为空错误

// Destroy 销毁 Volume，删除所有相关文件
// 用于删除整个 Volume 及其数据
//
// 参数:
//   - onlyEmpty: 如果为 true，仅在 Volume 为空时才删除
//
// 返回值:
//   - err: 错误信息
//
// 工作流程:
//  1. 加锁，防止并发访问
//  2. 如果 onlyEmpty=true，检查 Volume 是否为空
//  3. 检查 Volume 是否正在 Compaction
//  4. 关闭异步请求通道
//  5. 删除远程存储的数据（如果有）
//  6. 关闭 Volume
//  7. 删除所有本地文件（.dat, .idx, .vif, .sdx, .cpd, .cpx, .ldb, .note）
//
// 删除的文件类型:
//   - .dat/.idx: 数据和索引文件
//   - .vif: Volume Info 文件
//   - .sdx: Sorted Index 文件
//   - .cpd/.cpx: Compaction 文件
//   - .ldb: LevelDB 索引文件
//   - .note: 标记文件（损坏或不完整的 Volume）
func (v *Volume) Destroy(onlyEmpty bool) (err error) {
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	if onlyEmpty {
		isEmpty, e := v.doIsEmpty()
		if e != nil {
			err = fmt.Errorf("failed to read isEmpty %v", e)
			return
		}
		if !isEmpty {
			err = ErrVolumeNotEmpty
			return
		}
	}
	if v.isCompacting || v.isCommitCompacting {
		err = fmt.Errorf("volume %d is compacting", v.Id)
		return
	}
	close(v.asyncRequestsChan)
	storageName, storageKey := v.RemoteStorageNameKey()
	if v.HasRemoteFile() && storageName != "" && storageKey != "" {
		if backendStorage, found := backend.BackendStorages[storageName]; found {
			backendStorage.DeleteFile(storageKey)
		}
	}
	v.doClose()
	removeVolumeFiles(v.DataFileName())
	removeVolumeFiles(v.IndexFileName())
	return
}

// removeVolumeFiles - 删除 Volume 的所有相关文件（辅助函数）
//
// 功能说明：
// 删除指定基础文件名的所有 Volume 相关文件。
// 这是 Destroy 方法的辅助函数。
//
// 参数：
//   - filename: 基础文件名（不含扩展名）
//
// 删除的文件列表：
//
// 【基础文件】
//   - .dat: 数据文件，存储所有 Needle
//   - .idx: 索引文件，存储 Needle ID 到偏移量的映射
//   - .vif: Volume Info 文件，存储 Volume 元数据
//
// 【索引文件】
//   - .sdx: Sorted Index 文件，用于加速范围查询
//
// 【Compaction 文件】
//   - .cpd: Compaction 数据文件（临时）
//   - .cpx: Compaction 索引文件（临时）
//
// 【数据库文件】
//   - .ldb: LevelDB 目录（索引的持久化存储）
//
// 【标记文件】
//   - .note: 标记文件，表示 Volume 损坏或不完整
//
// 错误处理：
// - 所有删除操作都忽略错误（文件可能不存在）
// - 使用 os.RemoveAll 删除 .ldb 目录（递归删除）
func removeVolumeFiles(filename string) {
	// 【基础文件】
	os.Remove(filename + ".dat") // 数据文件
	os.Remove(filename + ".idx") // 索引文件
	os.Remove(filename + ".vif") // Volume 信息文件

	// 【索引文件】
	os.Remove(filename + ".sdx") // Sorted Index 文件

	// 【Compaction 文件】
	os.Remove(filename + ".cpd") // Compaction 数据文件
	os.Remove(filename + ".cpx") // Compaction 索引文件

	// 【数据库文件】
	os.RemoveAll(filename + ".ldb") // LevelDB 目录（递归删除）

	// 【标记文件】
	os.Remove(filename + ".note") // 损坏或不完整的标记
}

// asyncRequestAppend - 将异步请求添加到队列
//
// 功能说明：
// 将一个异步写入或删除请求添加到 Volume 的异步请求通道。
// 这个方法不会阻塞，除非通道已满。
//
// 参数：
//   - request: 异步请求对象（包含 Needle 和操作类型）
//
// 工作原理：
// - 将请求发送到 asyncRequestsChan 通道
// - 后台 worker goroutine 会从通道读取并批量处理请求
// - 批量处理提高了 fsync 的效率
//
// 并发安全：
// - Go channel 本身是线程安全的
// - 多个 goroutine 可以并发调用此方法
//
// 使用场景：
// - writeNeedle2(fsync=true): 需要持久化的写入
// - deleteNeedle2(fsync=true): 需要持久化的删除
func (v *Volume) asyncRequestAppend(request *needle.AsyncRequest) {
	// 发送请求到异步通道
	// worker goroutine 会批量处理这些请求
	v.asyncRequestsChan <- request
}

// syncWrite - 同步写入 Needle（不刷盘）
//
// 功能说明：
// 同步写入 Needle 到 Volume，但不调用 fsync 刷盘。
// 这是大多数写入操作的默认路径。
//
// 参数：
//   - n: 要写入的 Needle
//   - checkCookie: 是否验证 Cookie
//
// 返回值：
//   - offset: Needle 在 .dat 文件中的偏移量
//   - size: Needle 的大小
//   - isUnchanged: 文件是否未改变
//   - err: 错误信息
//
// 工作原理：
// 1. 获取数据文件访问锁（独占）
// 2. 调用 doWriteRequest 执行实际写入
// 3. 释放锁
//
// 与 fsync 的关系：
// - 不调用 fsync，数据可能在操作系统缓存中
// - 操作系统会定期刷盘（通常几秒）
// - 性能更高，但掉电可能丢失数据
//
// 使用场景：
// - 普通文件上传（不需要立即持久化）
// - 高吞吐量场景
// - 可以容忍少量数据丢失的场景
func (v *Volume) syncWrite(n *needle.Needle, checkCookie bool) (offset uint64, size Size, isUnchanged bool, err error) {
	// glog.V(4).Infof("writing needle %s", needle.NewFileIdFromNeedle(v.Id, n).String())

	// 获取数据文件的独占访问锁
	// 确保同一时间只有一个 goroutine 在写入
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	// 执行实际的写入操作
	return v.doWriteRequest(n, checkCookie)
}

// writeNeedle2 Volume 写入 Needle 的入口方法
// 根据 fsync 参数选择同步写入或异步写入
//
// 参数:
//   - n: 要写入的 Needle
//   - checkCookie: 是否检查 Cookie（防止错误覆盖）
//   - fsync: 是否需要fsync 刷盘（确保数据持久化）
//
// 返回值:
//   - offset: Needle 在 .dat 文件中的偏移量
//   - size: Needle 的大小
//   - isUnchanged: 文件是否未改变（幂等写入（Idempotent Write）是指对同一资源进行多次相同写入操作，其结果与执行一次写入操作相同）
//   - err: 错误信息
//
// 工作原理:
//   - 如果 Needle 没有 TTL，使用 Volume 的 TTL
//   - fsync=false: 调用 syncWrite 同步写入（不刷盘）
//   - fsync=true: 调用异步写入机制（批量刷盘）
//
// 异步写入的优势:
//   - 批量处理多个写入请求（最多 128 个或 4MB）
//   - 减少 fsync 调用次数，提高性能
//   - 保证数据持久化
func (v *Volume) writeNeedle2(n *needle.Needle, checkCookie bool, fsync bool) (offset uint64, size Size, isUnchanged bool, err error) {
	// glog.V(4).Infof("writing needle %s", needle.NewFileIdFromNeedle(v.Id, n).String())

	// 步骤 1: 如果 Needle 没有 TTL，使用 Volume 的默认 TTL
	if n.Ttl == needle.EMPTY_TTL && v.Ttl != needle.EMPTY_TTL {
		n.SetHasTtl()
		n.Ttl = v.Ttl
	}

	// 步骤 2: 根据 fsync 参数选择写入方式
	if !fsync {
		// 同步写入（不刷盘），适合大部分场景
		return v.syncWrite(n, checkCookie)
	} else {
		// 异步写入（批量刷盘），适合高吞吐场景
		asyncRequest := needle.NewAsyncRequest(n, true)
		// using len(n.Data) here instead of n.Size before n.Size is populated in n.Append()
		// 使用 len(n.Data) 而不是 n.Size，因为 n.Size 在 Append() 中才会填充
		asyncRequest.ActualSize = needle.GetActualSize(Size(len(n.Data)), v.Version())

		// 将请求加入异步队列
		v.asyncRequestAppend(asyncRequest)
		// 等待异步写入完成
		offset, _, isUnchanged, err = asyncRequest.WaitComplete()

		return
	}
}

// doWriteRequest 执行实际的写入操作
// 这是写入 Needle 的核心逻辑
//
// 参数:
//   - n: 要写入的 Needle
//   - checkCookie: 是否验证 Cookie
//
// 返回值:
//   - offset: Needle 在 .dat 文件中的偏移量
//   - size: Needle 的大小
//   - isUnchanged: 文件是否未改变
//   - err: 错误信息
//
// 工作流程:
//  1. 检查文件是否未改变（幂等性检查）
//  2. 如果存在相同 ID 的 Needle，验证 Cookie
//  3. 追加 Needle 到 .dat 文件
//  4. 更新 Needle Map 索引
//  5. 更新 Volume 的最后修改时间
//
// Cookie 验证:
//   - Cookie 是 Needle 的随机密钥
//   - 用于防止错误覆盖（例如 ID 冲突）
//   - 只有 Cookie 匹配才允许覆盖
//
// 幂等性:
//   - 如果文件内容完全相同（Cookie、Checksum、Data），返回 isUnchanged=true
//   - 避免重复写入，节省磁盘空间
func (v *Volume) doWriteRequest(n *needle.Needle, checkCookie bool) (offset uint64, size Size, isUnchanged bool, err error) {
	// glog.V(4).Infof("writing needle %s", needle.NewFileIdFromNeedle(v.Id, n).String())

	// 步骤 1: 检查文件是否未改变（幂等性检查）
	if v.isFileUnchanged(n) {
		size = Size(n.DataSize)
		isUnchanged = true
		return
	}

	// 步骤 2: 验证 Cookie（如果需要）
	// check whether existing needle cookie matches
	nv, ok := v.nm.Get(n.Id)
	if ok {
		// 找到现有 Needle，读取其 Header 验证 Cookie
		existingNeedle, _, _, existingNeedleReadErr := needle.ReadNeedleHeader(v.DataBackend, v.Version(), nv.Offset.ToActualOffset())
		if existingNeedleReadErr != nil {
			err = fmt.Errorf("reading existing needle: %w", existingNeedleReadErr)
			return
		}
		if n.Cookie == 0 && !checkCookie {
			// this is from batch deletion, and read back again when tailing a remote volume
			// which only happens when checkCookie == false and fsync == false
			// 这来自批量删除，在跟随远程 Volume 时重新读取
			// 仅在 checkCookie == false 且 fsync == false 时发生
			n.Cookie = existingNeedle.Cookie
		}
		// 验证 Cookie 是否匹配
		if existingNeedle.Cookie != n.Cookie {
			glog.V(0).Infof("write cookie mismatch: existing %s, new %s",
				needle.NewFileIdFromNeedle(v.Id, existingNeedle), needle.NewFileIdFromNeedle(v.Id, n))
			err = fmt.Errorf("mismatching cookie %x", n.Cookie)
			return
		}
	}

	// 步骤 3: 追加 Needle 到 .dat 文件
	// append to dat file
	n.UpdateAppendAtNs(v.lastAppendAtNs) // 更新追加时间戳
	var actualSize int64
	offset, size, actualSize, err = n.Append(v.DataBackend, v.Version())
	v.checkReadWriteError(err) // 检查 IO 错误
	if err != nil {
		err = fmt.Errorf("append to volume %d size %d actualSize %d: %v", v.Id, size, actualSize, err)
		return
	}
	v.lastAppendAtNs = n.AppendAtNs // 记录最后追加时间

	// 步骤 4: 更新 Needle Map 索引
	// add to needle map
	// 仅在 Needle 不存在或新偏移量更大时才更新索引
	if !ok || uint64(nv.Offset.ToActualOffset()) < offset {
		if err = v.nm.Put(n.Id, ToOffset(int64(offset)), n.Size); err != nil {
			glog.V(4).Infof("failed to save in needle map %d: %v", n.Id, err)
		}
	}

	// 步骤 5: 更新 Volume 的最后修改时间
	if v.lastModifiedTsSeconds < n.LastModified {
		v.lastModifiedTsSeconds = n.LastModified
	}
	return
}

// syncDelete - 同步删除 Needle（不刷盘）
//
// 功能说明：
// 同步删除指定的 Needle，但不调用 fsync 刷盘。
// 删除操作实际上是追加一个"删除标记"到 .dat 文件末尾。
//
// 参数：
//   - n: 要删除的 Needle（只需要 ID 和 Cookie）
//
// 返回值：
//   - Size: 被删除的 Needle 的原始大小（用于统计空间回收）
//   - error: 错误信息
//
// 工作原理：
// 1. 获取数据文件访问锁（独占访问）
// 2. 检查 Needle Map 是否已初始化
// 3. 调用 doDeleteRequest 执行实际删除
// 4. 释放锁
//
// 删除机制：
// - SeaweedFS 使用"软删除"机制
// - 不会物理删除数据，而是追加删除标记
// - 被删除的空间在 Compaction 时才会回收
// - 这种设计保证了 append-only 的写入模式
//
// 线程安全：
// - 使用 dataFileAccessLock 保证互斥访问
// - 同一时间只有一个 goroutine 可以删除
//
// 使用场景：
// - 普通文件删除操作
// - 不需要立即持久化的删除
// - deleteNeedle2 的内部实现
func (v *Volume) syncDelete(n *needle.Needle) (Size, error) {
	// glog.V(4).Infof("delete needle %s", needle.NewFileIdFromNeedle(v.Id, n).String())

	// 获取数据文件的独占访问锁
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	// 如果 Needle Map 未初始化，直接返回
	// 这可能发生在 Volume 正在关闭时
	if v.nm == nil {
		return 0, nil
	}

	// 执行实际的删除操作
	return v.doDeleteRequest(n)
}

// deleteNeedle2 - Volume 删除 Needle 的入口方法
// 根据 fsync 参数选择同步删除或异步删除
//
// 参数：
//   - n: 要删除的 Needle
//
// 返回值：
//   - Size: 被删除的 Needle 的原始大小
//   - error: 错误信息
//
// 工作原理：
//   - fsync=false: 调用 syncDelete 同步删除（不刷盘）
//   - fsync=true: 调用异步删除机制（批量刷盘）
//
// 当前实现：
// - 目前删除操作总是使用 fsync=false，因为大多数情况下，文件删除不需要极高的可靠性，如果删除操作丢失，用户可以再次删除，而 fsync=false 提供更好的性能
// - 删除标记只是追加到文件末尾，不立即刷盘
// - 这是合理的，因为删除操作不如写入操作重要
// - 即使掉电丢失删除标记，也不会造成数据丢失
//
// 【TODO】未来改进：
// - 可能需要支持 fsync 的删除操作
// - 某些场景下（如合规要求）需要确保删除立即生效
// - 目前代码已经支持异步删除的框架
//
// 异步删除的工作流程：
// 1. 创建 AsyncRequest（IsWriteRequest=false）
// 2. 设置 ActualSize（删除标记的大小）
// 3. 将请求加入异步队列
// 4. 等待后台 worker 批量处理
// 5. 返回删除结果
//
// 使用场景：
// - 用户删除文件的 API 调用
// - Volume 复制时的删除同步
// - Compaction 时的空间回收
func (v *Volume) deleteNeedle2(n *needle.Needle) (Size, error) {
	// TODO: 删除信息总是追加但不 fsync，未来可能需要 fsync
	// todo: delete info is always appended no fsync, it may need fsync in future
	fsync := false

	if !fsync {
		// 同步删除（不刷盘），当前的默认行为
		return v.syncDelete(n)
	} else {
		// 异步删除（批量刷盘），未来可能启用
		asyncRequest := needle.NewAsyncRequest(n, false) // false 表示删除操作
		// 删除标记的实际大小（不包含原始数据）
		asyncRequest.ActualSize = needle.GetActualSize(0, v.Version())

		// 将请求加入异步队列
		v.asyncRequestAppend(asyncRequest)
		// 等待异步删除完成
		_, size, _, err := asyncRequest.WaitComplete()

		return Size(size), err
	}
}

// doDeleteRequest - 执行实际的删除操作
// 这是删除 Needle 的核心逻辑
//
// 参数：
//   - n: 要删除的 Needle
//
// 返回值：
//   - Size: 被删除的 Needle 的原始大小（用于统计）
//   - error: 错误信息
//
// 工作流程：
//  1. 从 Needle Map 查找 Needle 的元数据
//  2. 检查 Needle 是否存在且未被删除
//  3. 追加删除标记到 .dat 文件（软删除）
//  4. 更新 Needle Map 中的删除标记
//  5. 返回被删除 Needle 的大小
//
// 软删除机制：
// - SeaweedFS 使用 append-only 的存储模型
// - 删除不会物理移除数据，而是追加一个"删除标记"
// - 删除标记是一个 Data 为空的 Needle（n.Data = nil）
// - Needle Map 中的偏移量会更新为删除标记的位置
// - 原始数据仍然存在，但索引指向删除标记
//
// 远程存储特殊处理：
// - 如果 Volume 使用远程存储（如 S3），不追加删除标记
// - 远程存储通常不支持追加操作
// - 直接在 Needle Map 中标记删除即可
//
// 幂等性：
// - 如果 Needle 已经被删除（Size.IsDeleted()），直接返回 0
// - 避免重复删除操作
//
// 使用场景：
// - syncDelete 的内部实现
// - 异步删除 worker 的内部实现
// - Volume 复制时的删除同步
func (v *Volume) doDeleteRequest(n *needle.Needle) (Size, error) {
	glog.V(4).Infof("delete needle %s", needle.NewFileIdFromNeedle(v.Id, n).String())

	// 步骤 1: 从 Needle Map 查找 Needle 的元数据
	nv, ok := v.nm.Get(n.Id)
	// fmt.Println("key", n.Id, "volume offset", nv.Offset, "data_size", n.Size, "cached size", nv.Size)

	// 步骤 2: 检查 Needle 是否存在且未被删除
	if ok && !nv.Size.IsDeleted() {
		var offset uint64
		var err error
		size := nv.Size // 保存原始大小，用于返回

		// 步骤 3: 追加删除标记（软删除）
		if !v.hasRemoteFile {
			// 本地存储：追加删除标记到 .dat 文件
			n.Data = nil                         // 清空数据，表示这是删除标记
			n.UpdateAppendAtNs(v.lastAppendAtNs) // 更新追加时间戳
			offset, _, _, err = n.Append(v.DataBackend, v.Version())
			v.checkReadWriteError(err) // 检查 IO 错误
			if err != nil {
				return size, err
			}
		}
		// 远程存储：不追加删除标记（远程存储通常不支持追加）
		// 直接在 Needle Map 中标记删除

		// 更新最后追加时间戳
		v.lastAppendAtNs = n.AppendAtNs

		// 步骤 4: 更新 Needle Map 中的删除标记
		// Delete 方法会将 Size 设置为负值或特殊标记
		if err = v.nm.Delete(n.Id, ToOffset(int64(offset))); err != nil {
			return size, err
		}

		// 步骤 5: 返回被删除 Needle 的原始大小
		return size, err
	}

	// Needle 不存在或已被删除，返回 0
	return 0, nil
}

// startWorker - 启动异步写入和删除的后台 worker goroutine
// 这是 SeaweedFS 批量写入优化的核心实现
//
// 功能说明：
// 启动一个后台 goroutine 来批量处理异步写入和删除请求。
// 通过批量处理多个请求并调用一次 fsync，大幅提高了 I/O 性能。
//
// 工作原理：
// 1. 从 asyncRequestsChan 通道接收异步请求
// 2. 批量收集请求（最多 128 个或 4MB）
// 3. 获取数据文件锁，执行所有请求
// 4. 调用一次 fsync 刷盘
// 5. 通知所有请求完成
//
// 【批量优化策略】
//
// 批量大小限制：
// - 最多 128 个请求（避免延迟过高）
// - 最多 4MB 数据（平衡吞吐量和延迟）
// - 如果通道为空，立即提交（避免 I/O 空闲）
//
// 为什么批量处理？
// - fsync 是昂贵的系统调用（通常 5-50ms）
// - 批量处理可以将多个请求的 fsync 合并为一次
// - 例如：100 个请求只需 1 次 fsync，性能提升 100 倍
//
// 【错误处理和回滚】
//
// fsync 失败回滚：
// - 如果 fsync 失败，说明数据未持久化
// - 使用 Truncate 回滚到批处理前的文件位置
// - 所有成功的请求都标记为失败
// - 保证数据一致性（要么全部成功，要么全部失败）
//
// 为什么需要回滚？
// - 写入成功但 fsync 失败，数据在内核缓存中
// - 如果告诉客户端"成功"，但掉电会丢失数据
// - 回滚后客户端会重试，保证数据最终持久化
//
// 【生命周期管理】
//
// Goroutine 退出条件：
// - Volume 关闭时会 close(asyncRequestsChan)
// - Worker 检测到通道关闭（ok == false）
// - 设置 chanClosed = true，下一次循环退出
//
// 优雅关闭：
// - 处理完当前批次后才退出
// - 不会丢失已提交的请求
//
// 【性能特性】
//
// 延迟：
// - 单个请求延迟增加（等待批量）
// - 但吞吐量大幅提升（批量 fsync）
//
// 吞吐量：
// - 4MB 批次 + 1 次 fsync ≈ 50ms
// - 吞吐量 ≈ 80MB/s（单线程）
// - 多线程可以进一步提升
//
// 使用场景：
// - writeNeedle2(fsync=true) 的后台实现
// - deleteNeedle2(fsync=true) 的后台实现
// - 高吞吐量写入场景（如批量导入）
func (v *Volume) startWorker() {
	go func() {
		chanClosed := false // 通道是否已关闭
		for {
			// 【主循环退出条件】
			// chan closed. go thread will exit
			if chanClosed {
				break
			}

			// 【批次收集阶段】
			// 批量收集请求，最多 128 个或 4MB
			currentRequests := make([]*needle.AsyncRequest, 0, 128)
			currentBytesToWrite := int64(0) // 当前批次的总字节数

			for {
				// 从通道接收异步请求
				request, ok := <-v.asyncRequestsChan
				// volume may be closed
				if !ok {
					// 通道已关闭，Volume 正在关闭
					chanClosed = true
					break
				}

				// 【容量检查】检查写入后是否超过 Volume 最大容量
				if MaxPossibleVolumeSize < v.ContentSize()+uint64(currentBytesToWrite+request.ActualSize) {
					// 超过容量限制，拒绝此请求
					request.Complete(0, 0, false,
						fmt.Errorf("volume size limit %d exceeded! current size is %d", MaxPossibleVolumeSize, v.ContentSize()))
					break
				}

				// 将请求加入当前批次
				currentRequests = append(currentRequests, request)
				currentBytesToWrite += request.ActualSize

				// 【批次大小限制】
				// submit at most 4M bytes or 128 requests at one time to decrease request delay.
				// it also need to break if there is no data in channel to avoid io hang.
				// 提交最多 4MB 或 128 个请求，以降低请求延迟
				// 同时需要在通道为空时立即提交，避免 I/O 挂起
				if currentBytesToWrite >= 4*1024*1024 || len(currentRequests) >= 128 || len(v.asyncRequestsChan) == 0 {
					break
				}
			}

			// 没有请求，继续等待
			if len(currentRequests) == 0 {
				continue
			}

			// 【批次执行阶段】
			// 获取数据文件的独占访问锁
			v.dataFileAccessLock.Lock()

			// 记录批处理前的文件位置（用于回滚）
			end, _, e := v.DataBackend.GetStat()
			if e != nil {
				// 无法获取文件状态，所有请求标记为失败
				for i := 0; i < len(currentRequests); i++ {
					currentRequests[i].Complete(0, 0, false,
						fmt.Errorf("cannot read current volume position: %v", e))
				}
				v.dataFileAccessLock.Unlock()
				continue
			}

			// 【执行所有请求】
			// 依次执行当前批次的所有写入/删除请求
			for i := 0; i < len(currentRequests); i++ {
				if currentRequests[i].IsWriteRequest {
					// 写入请求
					offset, size, isUnchanged, err := v.doWriteRequest(currentRequests[i].N, true)
					currentRequests[i].UpdateResult(offset, uint64(size), isUnchanged, err)
				} else {
					// 删除请求
					size, err := v.doDeleteRequest(currentRequests[i].N)
					currentRequests[i].UpdateResult(0, uint64(size), false, err)
				}
			}

			// 【fsync 刷盘】
			// if sync error, data is not reliable, we should mark the completed request as fail and rollback
			// 如果 fsync 失败，数据不可靠，应该标记所有成功的请求为失败并回滚
			if err := v.DataBackend.Sync(); err != nil {
				// TODO: this may generate dirty data or cause data inconsistent, may be weed need to panic?
				// 回滚：截断文件到批处理前的位置
				if te := v.DataBackend.Truncate(end); te != nil {
					glog.V(0).Infof("Failed to truncate %s back to %d with error: %v", v.DataBackend.Name(), end, te)
				}
				// 标记所有成功的请求为失败
				for i := 0; i < len(currentRequests); i++ {
					if currentRequests[i].IsSucceed() {
						currentRequests[i].UpdateResult(0, 0, false, err)
					}
				}
			}

			// 【通知所有请求完成】
			// 唤醒所有等待的 goroutine
			for i := 0; i < len(currentRequests); i++ {
				currentRequests[i].Submit()
			}

			// 释放锁
			v.dataFileAccessLock.Unlock()
		}
	}()
}

// WriteNeedleBlob - 直接写入序列化的 Needle 二进制数据
// 用于从备份或复制源恢复 Needle
//
// 参数：
//   - needleId: Needle 的唯一标识符
//   - needleBlob: 序列化的 Needle 二进制数据（完整的磁盘格式）
//   - size: Needle 的大小
//
// 返回值：
//   - error: 错误信息
//
// 功能说明：
// 这个方法接收已经序列化的 Needle 二进制数据，直接写入到 Volume。
// 不同于 writeNeedle2，它不需要构造 Needle 对象，而是直接写入原始字节。
//
// 工作流程：
//  1. 获取数据文件访问锁（独占访问）
//  2. 检查 Volume 容量是否足够
//  3. 检查是否已存在相同的 Needle（幂等性）
//  4. 调用 WriteNeedleBlob 写入二进制数据
//  5. 更新 Needle Map 索引
//
// 幂等性检查：
// - 如果 Needle 已存在且大小相同，读取现有 Needle
// - 比较 Cookie、Checksum 和 Data
// - 如果完全相同，跳过写入（避免重复）
//
// 与 writeNeedle2 的区别：
// - writeNeedle2: 接收 Needle 对象，需要序列化
// - WriteNeedleBlob: 接收已序列化的字节流，直接写入
//
// 使用场景：
// - Volume 复制：从 Master Volume 复制到 Replica
// - Volume 恢复：从备份恢复数据
// - Volume 迁移：在不同节点间移动数据
// - Volume Compaction：重写 Needle 到新文件
//
// 时间戳处理：
// - 使用 GetAppendAtNs(v.lastAppendAtNs) 生成追加时间戳
// - 保证追加顺序的单调性
// - 用于复制和一致性检查
//
// 线程安全：
// - 使用 dataFileAccessLock 保证互斥访问
// - 同一时间只有一个 goroutine 可以写入
func (v *Volume) WriteNeedleBlob(needleId NeedleId, needleBlob []byte, size Size) error {

	// 获取数据文件的独占访问锁
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	// 步骤 1: 检查 Volume 容量
	// 注意：这里使用 nm.ContentSize() 而不是 v.ContentSize()
	if MaxPossibleVolumeSize < v.nm.ContentSize()+uint64(len(needleBlob)) {
		return fmt.Errorf("volume size limit %d exceeded! current size is %d", MaxPossibleVolumeSize, v.nm.ContentSize())
	}

	// 步骤 2: 检查 Needle 是否已存在（幂等性检查）
	nv, ok := v.nm.Get(needleId)
	if ok && nv.Size == size {
		// 找到现有 Needle 且大小相同，读取并比较内容
		oldNeedle := new(needle.Needle)
		err := oldNeedle.ReadData(v.DataBackend, nv.Offset.ToActualOffset(), nv.Size, v.Version())
		if err == nil {
			// 解析新 Needle 的数据
			newNeedle := new(needle.Needle)
			err = newNeedle.ReadBytes(needleBlob, nv.Offset.ToActualOffset(), size, v.Version())
			// 比较三个关键属性：Cookie、Checksum、Data
			if err == nil && oldNeedle.Cookie == newNeedle.Cookie && oldNeedle.Checksum == newNeedle.Checksum && bytes.Equal(oldNeedle.Data, newNeedle.Data) {
				// 完全相同，跳过写入
				glog.V(0).Infof("needle %v already exists", needleId)
				return nil
			}
		}
	}

	// 步骤 3: 生成追加时间戳
	// 使用 GetAppendAtNs 保证时间戳的单调性
	appendAtNs := needle.GetAppendAtNs(v.lastAppendAtNs)

	// 步骤 4: 写入 Needle 二进制数据
	offset, err := needle.WriteNeedleBlob(v.DataBackend, needleBlob, size, appendAtNs, v.Version())

	// 检查 IO 错误（磁盘故障检测）
	v.checkReadWriteError(err)
	if err != nil {
		return err
	}

	// 更新最后追加时间戳
	v.lastAppendAtNs = appendAtNs

	// 步骤 5: 更新 Needle Map 索引
	// add to needle map
	if err = v.nm.Put(needleId, ToOffset(int64(offset)), size); err != nil {
		glog.V(4).Infof("failed to put in needle map %d: %v", needleId, err)
	}

	return err
}
