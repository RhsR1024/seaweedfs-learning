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
var ErrorNotFound = errors.New("not found")       // Needle 不存在
var ErrorDeleted = errors.New("already deleted")  // Needle 已被删除
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

func removeVolumeFiles(filename string) {
	// basic
	os.Remove(filename + ".dat")
	os.Remove(filename + ".idx")
	os.Remove(filename + ".vif")
	// sorted index file
	os.Remove(filename + ".sdx")
	// compaction
	os.Remove(filename + ".cpd")
	os.Remove(filename + ".cpx")
	// level db index file
	os.RemoveAll(filename + ".ldb")
	// marker for damaged or incomplete volume
	os.Remove(filename + ".note")
}

func (v *Volume) asyncRequestAppend(request *needle.AsyncRequest) {
	v.asyncRequestsChan <- request
}

func (v *Volume) syncWrite(n *needle.Needle, checkCookie bool) (offset uint64, size Size, isUnchanged bool, err error) {
	// glog.V(4).Infof("writing needle %s", needle.NewFileIdFromNeedle(v.Id, n).String())
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

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
//   - isUnchanged: 文件是否未改变（幂等写入）
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

func (v *Volume) syncDelete(n *needle.Needle) (Size, error) {
	// glog.V(4).Infof("delete needle %s", needle.NewFileIdFromNeedle(v.Id, n).String())
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	if v.nm == nil {
		return 0, nil
	}

	return v.doDeleteRequest(n)
}

func (v *Volume) deleteNeedle2(n *needle.Needle) (Size, error) {
	// todo: delete info is always appended no fsync, it may need fsync in future
	fsync := false

	if !fsync {
		return v.syncDelete(n)
	} else {
		asyncRequest := needle.NewAsyncRequest(n, false)
		asyncRequest.ActualSize = needle.GetActualSize(0, v.Version())

		v.asyncRequestAppend(asyncRequest)
		_, size, _, err := asyncRequest.WaitComplete()

		return Size(size), err
	}
}

func (v *Volume) doDeleteRequest(n *needle.Needle) (Size, error) {
	glog.V(4).Infof("delete needle %s", needle.NewFileIdFromNeedle(v.Id, n).String())
	nv, ok := v.nm.Get(n.Id)
	// fmt.Println("key", n.Id, "volume offset", nv.Offset, "data_size", n.Size, "cached size", nv.Size)
	if ok && !nv.Size.IsDeleted() {
		var offset uint64
		var err error
		size := nv.Size
		if !v.hasRemoteFile {
			n.Data = nil
			n.UpdateAppendAtNs(v.lastAppendAtNs)
			offset, _, _, err = n.Append(v.DataBackend, v.Version())
			v.checkReadWriteError(err)
			if err != nil {
				return size, err
			}
		}
		v.lastAppendAtNs = n.AppendAtNs
		if err = v.nm.Delete(n.Id, ToOffset(int64(offset))); err != nil {
			return size, err
		}
		return size, err
	}
	return 0, nil
}

func (v *Volume) startWorker() {
	go func() {
		chanClosed := false
		for {
			// chan closed. go thread will exit
			if chanClosed {
				break
			}
			currentRequests := make([]*needle.AsyncRequest, 0, 128)
			currentBytesToWrite := int64(0)
			for {
				request, ok := <-v.asyncRequestsChan
				// volume may be closed
				if !ok {
					chanClosed = true
					break
				}
				if MaxPossibleVolumeSize < v.ContentSize()+uint64(currentBytesToWrite+request.ActualSize) {
					request.Complete(0, 0, false,
						fmt.Errorf("volume size limit %d exceeded! current size is %d", MaxPossibleVolumeSize, v.ContentSize()))
					break
				}
				currentRequests = append(currentRequests, request)
				currentBytesToWrite += request.ActualSize
				// submit at most 4M bytes or 128 requests at one time to decrease request delay.
				// it also need to break if there is no data in channel to avoid io hang.
				if currentBytesToWrite >= 4*1024*1024 || len(currentRequests) >= 128 || len(v.asyncRequestsChan) == 0 {
					break
				}
			}
			if len(currentRequests) == 0 {
				continue
			}
			v.dataFileAccessLock.Lock()
			end, _, e := v.DataBackend.GetStat()
			if e != nil {
				for i := 0; i < len(currentRequests); i++ {
					currentRequests[i].Complete(0, 0, false,
						fmt.Errorf("cannot read current volume position: %v", e))
				}
				v.dataFileAccessLock.Unlock()
				continue
			}

			for i := 0; i < len(currentRequests); i++ {
				if currentRequests[i].IsWriteRequest {
					offset, size, isUnchanged, err := v.doWriteRequest(currentRequests[i].N, true)
					currentRequests[i].UpdateResult(offset, uint64(size), isUnchanged, err)
				} else {
					size, err := v.doDeleteRequest(currentRequests[i].N)
					currentRequests[i].UpdateResult(0, uint64(size), false, err)
				}
			}

			// if sync error, data is not reliable, we should mark the completed request as fail and rollback
			if err := v.DataBackend.Sync(); err != nil {
				// todo: this may generate dirty data or cause data inconsistent, may be weed need to panic?
				if te := v.DataBackend.Truncate(end); te != nil {
					glog.V(0).Infof("Failed to truncate %s back to %d with error: %v", v.DataBackend.Name(), end, te)
				}
				for i := 0; i < len(currentRequests); i++ {
					if currentRequests[i].IsSucceed() {
						currentRequests[i].UpdateResult(0, 0, false, err)
					}
				}
			}

			for i := 0; i < len(currentRequests); i++ {
				currentRequests[i].Submit()
			}
			v.dataFileAccessLock.Unlock()
		}
	}()
}

func (v *Volume) WriteNeedleBlob(needleId NeedleId, needleBlob []byte, size Size) error {

	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	if MaxPossibleVolumeSize < v.nm.ContentSize()+uint64(len(needleBlob)) {
		return fmt.Errorf("volume size limit %d exceeded! current size is %d", MaxPossibleVolumeSize, v.nm.ContentSize())
	}

	nv, ok := v.nm.Get(needleId)
	if ok && nv.Size == size {
		oldNeedle := new(needle.Needle)
		err := oldNeedle.ReadData(v.DataBackend, nv.Offset.ToActualOffset(), nv.Size, v.Version())
		if err == nil {
			newNeedle := new(needle.Needle)
			err = newNeedle.ReadBytes(needleBlob, nv.Offset.ToActualOffset(), size, v.Version())
			if err == nil && oldNeedle.Cookie == newNeedle.Cookie && oldNeedle.Checksum == newNeedle.Checksum && bytes.Equal(oldNeedle.Data, newNeedle.Data) {
				glog.V(0).Infof("needle %v already exists", needleId)
				return nil
			}
		}
	}
	appendAtNs := needle.GetAppendAtNs(v.lastAppendAtNs)
	offset, err := needle.WriteNeedleBlob(v.DataBackend, needleBlob, size, appendAtNs, v.Version())

	v.checkReadWriteError(err)
	if err != nil {
		return err
	}
	v.lastAppendAtNs = appendAtNs

	// add to needle map
	if err = v.nm.Put(needleId, ToOffset(int64(offset)), size); err != nil {
		glog.V(4).Infof("failed to put in needle map %d: %v", needleId, err)
	}

	return err
}
