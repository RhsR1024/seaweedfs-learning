// Package topology 实现了 SeaweedFS 的拓扑结构管理
// 本文件实现了 Vacuum 功能，用于清理已删除文件占用的磁盘空间
//
// Vacuum 工作流程：
//   1. VacuumVolumeCheck: 检查 Volume 的垃圾数据比例
//   2. VacuumVolumeCompact: 压缩 Volume，生成新的 .dat 文件
//   3. VacuumVolumeCommit: 提交压缩结果，替换旧的 .dat 文件
//   4. VacuumVolumeCleanup: 清理临时文件（如果压缩失败）
package topology

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/seaweedfs/seaweedfs/weed/pb"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

// batchVacuumVolumeCheck 批量检查 Volume 副本的垃圾数据比例
// 并发向所有副本发送检查请求，返回需要进行 Vacuum 的副本列表
//
// 参数:
//   - grpcDialOption: gRPC 连接选项
//   - vid: Volume ID
//   - locationlist: Volume 的所有副本位置列表
//   - garbageThreshold: 垃圾数据比例阈值（例如 0.3 表示 30%）
//
// 返回:
//   - *VolumeLocationList: 垃圾比例超过阈值的副本列表
//   - bool: 是否需要进行 Vacuum（true 表示有副本需要清理且没有错误）
//
// 工作流程:
//   1. 并发向所有副本发送 VacuumVolumeCheck 请求
//   2. 检查每个副本的垃圾数据比例（GarbageRatio）
//   3. 收集垃圾比例 >= garbageThreshold 的副本
//   4. 如果所有副本检查成功且至少有一个需要清理，返回 true
//
// 超时机制:
//   - 根据 Volume 大小计算超时时间：(volumeSizeLimit/1024/1024/1000 + 1) 分钟
//   - 例如 30GB Volume 超时时间约 31 分钟
func (t *Topology) batchVacuumVolumeCheck(grpcDialOption grpc.DialOption, vid needle.VolumeId,
	locationlist *VolumeLocationList, garbageThreshold float64) (*VolumeLocationList, bool) {
	// 创建通道接收检查结果，缓冲区大小等于副本数
	ch := make(chan int, locationlist.Length())
	errCount := int32(0) // 错误计数，使用原子操作保证并发安全

	// 并发检查所有副本
	for index, dn := range locationlist.list {
		go func(index int, url pb.ServerAddress, vid needle.VolumeId) {
			// 连接 Volume server 并发送检查请求
			err := operation.WithVolumeServerClient(false, url, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
				resp, err := volumeServerClient.VacuumVolumeCheck(context.Background(), &volume_server_pb.VacuumVolumeCheckRequest{
					VolumeId: uint32(vid),
				})
				if err != nil {
					atomic.AddInt32(&errCount, 1) // 记录错误
					ch <- -1                      // 返回 -1 表示检查失败
					return err
				}
				// 检查垃圾数据比例是否超过阈值
				if resp.GarbageRatio >= garbageThreshold {
					ch <- index // 返回副本索引，表示需要 Vacuum
				} else {
					ch <- -1 // 返回 -1 表示不需要 Vacuum
				}
				return nil
			})
			if err != nil {
				glog.V(0).Infof("Checking vacuuming %d on %s: %v", vid, url, err)
			}
		}(index, dn.ServerAddress(), vid)
	}

	// 收集需要 Vacuum 的副本列表
	vacuumLocationList := NewVolumeLocationList()

	// 设置超时定时器
	// 超时时间计算：(volumeSizeLimit / 1GB + 1) 分钟
	// 对于 30GB Volume，超时时间为 31 分钟
	waitTimeout := time.NewTimer(time.Minute * time.Duration(t.volumeSizeLimit/1024/1024/1000+1))
	defer waitTimeout.Stop()

	// 等待所有副本的检查结果
	for range locationlist.list {
		select {
		case index := <-ch:
			// index != -1 表示该副本需要 Vacuum
			if index != -1 {
				vacuumLocationList.list = append(vacuumLocationList.list, locationlist.list[index])
			}
		case <-waitTimeout.C:
			// 超时，返回已收集到的副本列表
			return vacuumLocationList, false
		}
	}

	// 返回结果：
	// - 第一个返回值：需要 Vacuum 的副本列表
	// - 第二个返回值：是否需要 Vacuum（没有错误且至少有一个副本需要清理）
	return vacuumLocationList, errCount == 0 && len(vacuumLocationList.list) > 0
}

// batchVacuumVolumeCompact 批量压缩 Volume 副本
// 这是 Vacuum 的第二阶段，将有效数据重新写入新文件，去除已删除的数据
//
// 参数:
//   - grpcDialOption: gRPC 连接选项
//   - vl: VolumeLayout，用于管理 Volume 的可写状态
//   - vid: Volume ID
//   - locationlist: 需要压缩的副本列表（来自 batchVacuumVolumeCheck）
//   - preallocate: 预分配磁盘空间大小（字节数）
//
// 返回:
//   - bool: 所有副本是否都压缩成功
//
// 工作流程:
//   1. 将 Volume 标记为不可写，防止新数据写入
//   2. 并发向所有副本发送 VacuumVolumeCompact 请求
//   3. Volume server 会创建 .cpd（compact data）和 .cpx（compact index）临时文件
//   4. 将有效 Needle 复制到新文件，跳过已删除的 Needle
//   5. 等待所有副本压缩完成
//
// 超时机制:
//   - 压缩超时时间是检查超时的 3 倍（更耗时）
//   - 例如 30GB Volume 压缩超时约 93 分钟
//
// 注意:
//   - 压缩期间 Volume 不可写
//   - 如果压缩失败，需要调用 batchVacuumVolumeCleanup 清理临时文件
//   - 如果压缩成功，需要调用 batchVacuumVolumeCommit 提交更改
func (t *Topology) batchVacuumVolumeCompact(grpcDialOption grpc.DialOption, vl *VolumeLayout, vid needle.VolumeId,
	locationlist *VolumeLocationList, preallocate int64) bool {
	// 【1. 将 Volume 标记为不可写】
	vl.accessLock.Lock()
	vl.removeFromWritable(vid) // 从可写列表中移除，防止新数据写入
	vl.accessLock.Unlock()

	// 【2. 并发压缩所有副本】
	ch := make(chan bool, locationlist.Length())
	for index, dn := range locationlist.list {
		go func(index int, url pb.ServerAddress, vid needle.VolumeId) {
			glog.V(0).Infoln(index, "Start vacuuming", vid, "on", url)
			// 连接 Volume server 并发送压缩请求
			err := operation.WithVolumeServerClient(true, url, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
				// VacuumVolumeCompact 是流式 RPC，会持续返回压缩进度
				stream, err := volumeServerClient.VacuumVolumeCompact(context.Background(), &volume_server_pb.VacuumVolumeCompactRequest{
					VolumeId:    uint32(vid),
					Preallocate: preallocate, // 预分配文件大小，减少文件碎片
				})
				if err != nil {
					return err
				}

				// 持续接收压缩进度
				for {
					resp, recvErr := stream.Recv()
					if recvErr != nil {
						if recvErr == io.EOF {
							// 压缩完成
							break
						} else {
							return recvErr
						}
					}
					// 打印压缩进度：已处理字节数、系统负载
					glog.V(0).Infof("%d vacuum %d on %s processed %d bytes, loadAvg %.02f%%",
						index, vid, url, resp.ProcessedBytes, resp.LoadAvg_1M*100)
				}
				return nil
			})
			if err != nil {
				glog.Errorf("Error when vacuuming %d on %s: %v", vid, url, err)
				ch <- false // 压缩失败
			} else {
				glog.V(0).Infof("Complete vacuuming %d on %s", vid, url)
				ch <- true // 压缩成功
			}
		}(index, dn.ServerAddress(), vid)
	}
	isVacuumSuccess := true

	// 【3. 等待所有副本压缩完成】
	// 压缩超时时间是检查超时的 3 倍（压缩更耗时）
	waitTimeout := time.NewTimer(3 * time.Minute * time.Duration(t.volumeSizeLimit/1024/1024/1000+1))
	defer waitTimeout.Stop()

	for range locationlist.list {
		select {
		case canCommit := <-ch:
			// 只有所有副本都成功才返回 true
			isVacuumSuccess = isVacuumSuccess && canCommit
		case <-waitTimeout.C:
			// 超时，压缩失败
			return false
		}
	}
	return isVacuumSuccess
}

// batchVacuumVolumeCommit 批量提交 Volume 压缩结果
// 这是 Vacuum 的第三阶段，用压缩后的新文件替换原文件
//
// 参数:
//   - grpcDialOption: gRPC 连接选项
//   - vl: VolumeLayout，用于更新 Volume 状态
//   - vid: Volume ID
//   - vacuumLocationList: 成功压缩的副本列表
//   - locationList: Volume 的所有副本列表
//
// 返回:
//   - bool: 是否所有副本都提交成功
//
// 工作流程:
//   1. 对压缩成功的副本发送 VacuumVolumeCommit 请求
//   2. Volume server 会将 .cpd/.cpx 重命名为 .dat/.idx，替换原文件
//   3. 检查未压缩副本的状态（如果有）
//   4. 更新 Volume 的可写状态和容量状态
//   5. 记录 Vacuum 时间，防止频繁压缩
//
// 注意:
//   - 提交操作是原子的，要么全部成功，要么全部失败
//   - 如果有副本未参与压缩，需要检查其状态，确保一致性
//   - 提交成功后会记录 Vacuum 时间，短期内不会再次压缩该 Volume
func (t *Topology) batchVacuumVolumeCommit(grpcDialOption grpc.DialOption, vl *VolumeLayout, vid needle.VolumeId, vacuumLocationList, locationList *VolumeLocationList) bool {
	isCommitSuccess := true
	isReadOnly := false       // 任一副本只读，则整个 Volume 标记为只读
	isFullCapacity := false   // 任一副本超容量，则整个 Volume 标记为满

	// 【1. 对压缩成功的副本发送提交请求】
	for _, dn := range vacuumLocationList.list {
		glog.V(0).Infoln("Start Committing vacuum", vid, "on", dn.Url())
		err := operation.WithVolumeServerClient(false, dn.ServerAddress(), grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
			// VacuumVolumeCommit 会将 .cpd/.cpx 文件重命名为 .dat/.idx
			resp, err := volumeServerClient.VacuumVolumeCommit(context.Background(), &volume_server_pb.VacuumVolumeCommitRequest{
				VolumeId: uint32(vid),
			})
			if resp != nil {
				// 检查提交后的 Volume 状态
				if resp.IsReadOnly {
					isReadOnly = true // 任一副本只读，整个 Volume 标记为只读
				}
				if resp.VolumeSize > t.volumeSizeLimit {
					isFullCapacity = true // 任一副本超容量，整个 Volume 标记为满
				}
			}
			return err
		})
		if err != nil {
			glog.Errorf("Error when committing vacuum %d on %s: %v", vid, dn.Url(), err)
			isCommitSuccess = false
		} else {
			glog.V(0).Infof("Complete Committing vacuum %d on %s", vid, dn.Url())
		}
	}

	// 【2. 检查未参与压缩的副本状态】
	// 如果有副本未参与压缩（可能垃圾比例未达阈值），需要检查其状态
	// 确保所有副本的只读和容量状态一致
	if len(locationList.list) > len(vacuumLocationList.list) {
		for _, dn := range locationList.list {
			// 检查该副本是否参与了压缩
			isFound := false
			for _, dnVaccum := range vacuumLocationList.list {
				if dn.id == dnVaccum.id {
					isFound = true
					break
				}
			}
			// 如果未参与压缩，查询其当前状态
			if !isFound {
				err := operation.WithVolumeServerClient(false, dn.ServerAddress(), grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
					resp, err := volumeServerClient.VolumeStatus(context.Background(), &volume_server_pb.VolumeStatusRequest{
						VolumeId: uint32(vid),
					})
					if resp != nil {
						if resp.IsReadOnly {
							isReadOnly = true
						}
						if resp.VolumeSize > t.volumeSizeLimit {
							isFullCapacity = true
						}
					}
					return err
				})
				if err != nil {
					glog.Errorf("Error when checking volume %d status on %s: %v", vid, dn.Url(), err)
					// 如果状态未知，保守起见标记为只读
					isReadOnly = true
				}
			}
		}
	}

	// 【3. 提交成功后，更新 Volume 状态】
	if isCommitSuccess {
		// 记录 Vacuum 时间，避免短期内重复压缩
		vl.accessLock.Lock()
		vl.vacuumedVolumes[vid] = time.Now()
		vl.accessLock.Unlock()

		// 更新所有压缩副本的可用状态
		// SetVolumeAvailable 会根据 isReadOnly 和 isFullCapacity 更新可写列表
		for _, dn := range vacuumLocationList.list {
			vl.SetVolumeAvailable(dn, vid, isReadOnly, isFullCapacity)
		}
	}
	return isCommitSuccess
}

// batchVacuumVolumeCleanup 批量清理 Volume 压缩临时文件
// 当压缩失败时调用，删除 .cpd 和 .cpx 临时文件
//
// 参数:
//   - grpcDialOption: gRPC 连接选项
//   - vl: VolumeLayout（本函数中未使用，保持接口一致性）
//   - vid: Volume ID
//   - locationlist: 需要清理的副本列表
//
// 工作流程:
//   1. 向所有副本发送 VacuumVolumeCleanup 请求
//   2. Volume server 会删除 .cpd 和 .cpx 临时文件
//   3. 保留原有的 .dat 和 .idx 文件
//
// 调用时机:
//   - batchVacuumVolumeCompact 返回 false（压缩失败）
//   - 压缩过程中发生错误
//   - 压缩超时
//
// 注意:
//   - 清理操作是幂等的，多次调用不会产生副作用
//   - 清理失败不会影响 Volume 的正常使用
func (t *Topology) batchVacuumVolumeCleanup(grpcDialOption grpc.DialOption, vl *VolumeLayout, vid needle.VolumeId, locationlist *VolumeLocationList) {
	for _, dn := range locationlist.list {
		glog.V(0).Infoln("Start cleaning up", vid, "on", dn.Url())
		err := operation.WithVolumeServerClient(false, dn.ServerAddress(), grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
			// VacuumVolumeCleanup 会删除 .cpd 和 .cpx 临时文件
			_, err := volumeServerClient.VacuumVolumeCleanup(context.Background(), &volume_server_pb.VacuumVolumeCleanupRequest{
				VolumeId: uint32(vid),
			})
			return err
		})
		if err != nil {
			glog.Errorf("Error when cleaning up vacuum %d on %s: %v", vid, dn.Url(), err)
		} else {
			glog.V(0).Infof("Complete cleaning up vacuum %d on %s", vid, dn.Url())
		}
	}
}

// Vacuum 执行 Volume 垃圾回收，清理已删除文件占用的磁盘空间
// 这是 Vacuum 功能的总入口，支持手动触发和自动定时触发
//
// 参数:
//   - grpcDialOption: gRPC 连接选项
//   - garbageThreshold: 垃圾数据比例阈值（0.0-1.0），超过此值才进行压缩
//   - maxParallelVacuumPerServer: 每个 Volume server 同时压缩的最大 Volume 数
//   - volumeId: 指定要压缩的 Volume ID（0 表示压缩所有 Volume）
//   - collection: 指定要压缩的 Collection（空字符串表示所有 Collection）
//   - preallocate: 压缩时预分配的文件大小（字节数）
//   - automatic: 是否为自动触发（自动触发时会检查 isDisableVacuum 标志）
//
// 工作流程:
//   1. 检查是否已有 Vacuum 在运行（使用原子锁）
//   2. 遍历所有 Collection 和 VolumeLayout
//   3. 对每个 Volume 执行 Vacuum 四阶段流程
//   4. 支持按 Volume ID 或 Collection 过滤
//
// 并发控制:
//   - 全局同时只能有一个 Vacuum 进程（通过 vacuumLockCounter 控制）
//   - 每个 Volume server 可以并发压缩多个 Volume（通过 maxParallelVacuumPerServer 控制）
//
// 调用场景:
//   - 手动触发：weed shell 的 volume.vacuum 命令
//   - 自动触发：Master 定时任务（默认每 15 分钟检查一次）
//
// 注意:
//   - Vacuum 是资源密集型操作，建议在业务低峰期执行
//   - 自动 Vacuum 可以通过 DisableVacuum() 临时禁用
func (t *Topology) Vacuum(grpcDialOption grpc.DialOption, garbageThreshold float64, maxParallelVacuumPerServer int, volumeId uint32, collection string, preallocate int64, automatic bool) {

	// 【1. 全局 Vacuum 锁：确保同时只有一个 Vacuum 进程】
	// 使用 CompareAndSwap 原子操作，避免竞争条件
	swapped := atomic.CompareAndSwapInt64(&t.vacuumLockCounter, 0, 1)
	if !swapped {
		// 已有 Vacuum 在运行，直接返回
		glog.V(0).Infof("Vacuum is already running")
		return
	}
	// 函数返回时释放锁
	defer atomic.StoreInt64(&t.vacuumLockCounter, 0)

	// 【2. 开始 Vacuum 流程】
	glog.V(1).Infof("Start vacuum on demand with threshold: %f collection: %s volumeId: %d",
		garbageThreshold, collection, volumeId)

	// 遍历所有 Collection
	for _, col := range t.collectionMap.Items() {
		c := col.(*Collection)
		// 如果指定了 collection，只处理匹配的 Collection
		if collection != "" && collection != c.Name {
			continue
		}

		// 遍历该 Collection 下的所有 VolumeLayout
		for _, vl := range c.storageType2VolumeLayout.Items() {
			if vl != nil {
				volumeLayout := vl.(*VolumeLayout)

				if volumeId > 0 {
					// 【模式 1：压缩指定的 Volume】
					vid := needle.VolumeId(volumeId)
					volumeLayout.accessLock.RLock()
					locationList, ok := volumeLayout.vid2location[vid]
					volumeLayout.accessLock.RUnlock()
					if ok {
						// 直接压缩指定的 Volume
						t.vacuumOneVolumeId(grpcDialOption, volumeLayout, c, garbageThreshold, locationList, vid, preallocate)
					}
				} else {
					// 【模式 2：压缩该 VolumeLayout 下的所有 Volume】
					t.vacuumOneVolumeLayout(grpcDialOption, volumeLayout, c, garbageThreshold, maxParallelVacuumPerServer, preallocate, automatic)
				}
			}

			// 自动 Vacuum 时，检查是否已禁用
			if automatic && t.isDisableVacuum {
				break
			}
		}

		// 自动 Vacuum 时，检查是否已禁用
		if automatic && t.isDisableVacuum {
			glog.V(0).Infof("Vacuum is disabled")
			break
		}
	}
}

// vacuumOneVolumeLayout 压缩一个 VolumeLayout 下的所有 Volume
// 使用配额机制控制每个 Volume server 的并发压缩数
//
// 参数:
//   - grpcDialOption: gRPC 连接选项
//   - volumeLayout: 要压缩的 VolumeLayout
//   - c: 所属的 Collection
//   - garbageThreshold: 垃圾数据比例阈值
//   - maxParallelVacuumPerServer: 每个 Volume server 的最大并发压缩数
//   - preallocate: 预分配文件大小
//   - automatic: 是否自动触发
//
// 并发控制机制:
//   - 为每个 Volume server 分配配额（maxParallelVacuumPerServer）
//   - Volume 压缩前检查配额，压缩完成后归还配额
//   - 配额不足的 Volume 放入待处理队列，等待配额释放
//   - 使用 LimitedConcurrentExecutor 限制总并发数为 100
func (t *Topology) vacuumOneVolumeLayout(grpcDialOption grpc.DialOption, volumeLayout *VolumeLayout, c *Collection, garbageThreshold float64, maxParallelVacuumPerServer int, preallocate int64, automatic bool) {

	// 【1. 复制待处理的 Volume 列表】
	volumeLayout.accessLock.RLock()
	todoVolumeMap := make(map[needle.VolumeId]*VolumeLocationList)
	for vid, locationList := range volumeLayout.vid2location {
		todoVolumeMap[vid] = locationList.Copy() // 复制副本列表，避免并发修改
	}
	volumeLayout.accessLock.RUnlock()

	// 【2. 初始化每个 Volume server 的配额】
	// 配额用于限制每个 server 同时压缩的 Volume 数
	limiter := make(map[NodeId]int)
	var limiterLock sync.Mutex
	for _, locationList := range todoVolumeMap {
		for _, dn := range locationList.list {
			if _, ok := limiter[dn.Id()]; !ok {
				limiter[dn.Id()] = maxParallelVacuumPerServer
			}
		}
	}

	// 并发执行器，最多 100 个并发 goroutine
	executor := util.NewLimitedConcurrentExecutor(100)

	var wg sync.WaitGroup

	// 【3. 循环处理所有 Volume，直到队列为空】
	for len(todoVolumeMap) > 0 {
		pendingVolumeMap := make(map[needle.VolumeId]*VolumeLocationList)
		for vid, locationList := range todoVolumeMap {
			// 检查该 Volume 的所有副本是否都有配额
			hasEnoughQuota := true
			for _, dn := range locationList.list {
				limiterLock.Lock()
				quota := limiter[dn.Id()]
				limiterLock.Unlock()
				if quota <= 0 {
					// 配额不足，放入待处理队列
					hasEnoughQuota = false
					break
				}
			}
			if !hasEnoughQuota {
				pendingVolumeMap[vid] = locationList
				continue
			}

			// 扣除配额
			for _, dn := range locationList.list {
				limiterLock.Lock()
				limiter[dn.Id()]--
				limiterLock.Unlock()
			}

			// 启动 goroutine 压缩该 Volume
			wg.Add(1)
			executor.Execute(func() {
				defer wg.Done()
				t.vacuumOneVolumeId(grpcDialOption, volumeLayout, c, garbageThreshold, locationList, vid, preallocate)
				// 压缩完成后归还配额
				for _, dn := range locationList.list {
					limiterLock.Lock()
					limiter[dn.Id()]++
					limiterLock.Unlock()
				}
			})
			if automatic && t.isDisableVacuum {
				break
			}
		}
		if automatic && t.isDisableVacuum {
			break
		}
		// 如果没有 Volume 可以压缩（配额全部用完），等待 10 秒
		if len(todoVolumeMap) == len(pendingVolumeMap) {
			time.Sleep(10 * time.Second)
		}
		todoVolumeMap = pendingVolumeMap
	}

	// 等待所有压缩任务完成
	wg.Wait()

}

// vacuumOneVolumeId 对单个 Volume 执行完整的 Vacuum 流程
// 包含四个阶段：检查、压缩、提交/清理
//
// 参数:
//   - grpcDialOption: gRPC 连接选项
//   - volumeLayout: VolumeLayout
//   - c: 所属的 Collection
//   - garbageThreshold: 垃圾数据比例阈值
//   - locationList: Volume 的所有副本列表
//   - vid: Volume ID
//   - preallocate: 预分配文件大小
//
// Vacuum 四阶段流程:
//   1. Check: 检查垃圾比例是否超过阈值
//   2. Compact: 压缩 Volume，生成 .cpd/.cpx 临时文件
//   3. Commit: 如果压缩成功，提交更改（重命名为 .dat/.idx）
//   4. Cleanup: 如果压缩失败，清理临时文件
//
// 前置检查:
//   - Volume 不能是只读的
//   - Volume 必须有足够的副本数（避免数据丢失）
func (t *Topology) vacuumOneVolumeId(grpcDialOption grpc.DialOption, volumeLayout *VolumeLayout, c *Collection, garbageThreshold float64, locationList *VolumeLocationList, vid needle.VolumeId, preallocate int64) {
	// 【前置检查】
	volumeLayout.accessLock.RLock()
	isReadOnly := volumeLayout.readonlyVolumes.IsTrue(vid)
	isEnoughCopies := volumeLayout.enoughCopies(vid)
	volumeLayout.accessLock.RUnlock()

	if isReadOnly {
		// 只读 Volume 不能压缩
		return
	}
	if !isEnoughCopies {
		// 副本数不足，跳过压缩（避免数据丢失风险）
		glog.Warningf("skip vacuuming: not enough copies for volume:%d", vid)
		return
	}

	// 【Vacuum 四阶段流程】
	glog.V(1).Infof("check vacuum on collection:%s volume:%d", c.Name, vid)

	// 阶段 1：检查垃圾比例
	if vacuumLocationList, needVacuum := t.batchVacuumVolumeCheck(
		grpcDialOption, vid, locationList, garbageThreshold); needVacuum {

		// 阶段 2：压缩 Volume
		if t.batchVacuumVolumeCompact(grpcDialOption, volumeLayout, vid, vacuumLocationList, preallocate) {
			// 阶段 3：压缩成功，提交更改
			t.batchVacuumVolumeCommit(grpcDialOption, volumeLayout, vid, vacuumLocationList, locationList)
		} else {
			// 阶段 4：压缩失败，清理临时文件
			t.batchVacuumVolumeCleanup(grpcDialOption, volumeLayout, vid, vacuumLocationList)
		}
	}
}
