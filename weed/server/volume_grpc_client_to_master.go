// Package weed_server 实现 Volume Server 与 Master 的通信功能
// 本文件提供心跳机制和 Volume 状态同步
//
// 核心功能:
//   - 心跳机制：定期向 Master 报告状态
//   - Volume 同步：实时通知 Master Volume 变化
//   - EC Shard 同步：实时通知 Master EC 分片变化
//   - 配置同步：从 Master 获取全局配置
//   - UUID 冲突检测：避免多个 Volume Server 使用相同目录
//
// 心跳内容:
//   - Volume 列表及容量信息
//   - EC Shard 列表
//   - 磁盘使用情况
//   - 数据中心和机架位置
//
// 实时通知:
//   - 新增 Volume/EC Shard
//   - 删除 Volume/EC Shard
//   - 配置变更（预分配、卷大小限制）
//
// Leader 切换:
//   - 自动检测 Master Leader 变化
//   - 切换到新 Leader 继续心跳
//
// 关键设计:
//   - 使用 gRPC 双向流保持长连接
//   - 多个定时器触发不同类型的心跳
//   - Channel 机制实现实时增量更新
//   - 指数退避重试机制处理 UUID 冲突
package weed_server

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"

	"golang.org/x/net/context"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// GetMaster 返回当前连接的 Master 地址
// 用于其他组件需要与 Master 通信时获取地址
//
// 返回:
//   - pb.ServerAddress: 当前 Master 地址，例如 "192.168.1.10:9333"
func (vs *VolumeServer) GetMaster(ctx context.Context) pb.ServerAddress {
	return vs.currentMaster
}

// checkWithMaster 从 Master 获取全局配置
// 在 Volume Server 启动时调用，获取初始配置
//
// 功能:
//   - 获取 Master 配置（metrics 地址、存储后端等）
//   - 循环尝试所有 Seed Master 节点
//   - 失败时自动重试
//
// 获取的配置:
//   - MetricsAddress: Prometheus 监控地址
//   - MetricsIntervalSeconds: 监控上报间隔
//   - StorageBackends: 存储后端配置（Tiered Storage）
//
// 重试机制:
//   - 遍历所有 Seed Master 节点
//   - 失败后等待 1.79 秒重试
//   - 直到成功获取配置
//
// 返回:
//   - error: 获取配置失败（仅在成功时返回 nil）
func (vs *VolumeServer) checkWithMaster() (err error) {
	// 循环直到成功获取配置
	for {
		// 遍历所有 Seed Master 节点
		for _, master := range vs.SeedMasterNodes {
			err = operation.WithMasterServerClient(false, master, vs.grpcDialOption, func(masterClient master_pb.SeaweedClient) error {
				// 调用 GetMasterConfiguration RPC
				resp, err := masterClient.GetMasterConfiguration(context.Background(), &master_pb.GetMasterConfigurationRequest{})
				if err != nil {
					return fmt.Errorf("get master %s configuration: %v", master, err)
				}

				// 保存 Metrics 配置
				vs.metricsAddress, vs.metricsIntervalSec = resp.MetricsAddress, int(resp.MetricsIntervalSeconds)

				// 加载存储后端配置（Tiered Storage）
				backend.LoadFromPbStorageBackends(resp.StorageBackends)
				return nil
			})
			if err == nil {
				// 成功获取配置，返回
				return
			} else {
				glog.V(0).Infof("checkWithMaster %s: %v", master, err)
			}
		}
		// 所有 Master 都失败，等待后重试
		// 1.79 秒避免与其他定时器冲突
		time.Sleep(1790 * time.Millisecond)
	}
}

// heartbeat Volume Server 的心跳主循环
// 在 Volume Server 启动时以 goroutine 运行
//
// 功能:
//   - 定期向 Master 发送心跳
//   - 实时同步 Volume 和 EC Shard 变化
//   - 自动检测和切换 Master Leader
//   - 处理 UUID 冲突并重试
//
// 心跳内容:
//   - Volume 列表（ID、Collection、副本策略、TTL、大小等）
//   - EC Shard 列表（ID、分片 ID、Collection）
//   - 磁盘使用情况（总量、已用、可用）
//   - 服务器位置（数据中心、机架、IP、端口）
//
// UUID 冲突处理:
//   - 检测到重复 UUID 时使用指数退避重试
//   - 重试延迟：2s、4s、8s
//   - 最多重试 3 次
//   - 持续冲突则退出进程
//
// Leader 切换:
//   - 心跳响应中包含新 Leader 地址
//   - 自动切换到新 Leader 继续心跳
//   - 等待 3 秒避免竞争条件
//
// 循环逻辑:
//   1. 遍历所有 Seed Master 节点
//   2. 尝试与当前 Master 建立心跳
//   3. 失败时尝试下一个 Master
//   4. 检测到新 Leader 时切换
//   5. 直到 isHeartbeating=false
func (vs *VolumeServer) heartbeat() {

	glog.V(0).Infof("Volume server start with seed master nodes: %v", vs.SeedMasterNodes)
	// 设置数据中心和机架信息
	vs.store.SetDataCenter(vs.dataCenter)
	vs.store.SetRack(vs.rack)

	// 加载 TLS 配置（如果有）
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.volume")

	var err error
	var newLeader pb.ServerAddress
	duplicateRetryCount := 0  // UUID 冲突重试计数

	// 心跳主循环
	for vs.isHeartbeating {
		// 遍历所有 Seed Master 节点
		for _, master := range vs.SeedMasterNodes {
			if newLeader != "" {
				// 检测到新 Leader，切换到新 Leader
				// 等待 3 秒避免竞争条件
				// 新 Leader 可能是同一个 Master（Raft Leader 切换）
				time.Sleep(3 * time.Second)
				master = newLeader
			}

			// 设置当前 Master 地址
			vs.store.MasterAddress = master

			// 执行心跳，带重试机制
			newLeader, err = vs.doHeartbeatWithRetry(master, grpcDialOption, time.Duration(vs.pulseSeconds)*time.Second, duplicateRetryCount)
			if err != nil {
				glog.V(0).Infof("heartbeat to %s error: %v", master, err)

				// 检查是否是 UUID 冲突错误
				if strings.Contains(err.Error(), "duplicate UUIDs detected, retrying connection") {
					// UUID 冲突，增加重试计数
					duplicateRetryCount++
					// 指数退避：2s、4s、8s
					retryDelay := time.Duration(1<<(duplicateRetryCount-1)) * 2 * time.Second
					glog.V(0).Infof("Waiting %v before retrying due to duplicate UUID detection...", retryDelay)
					time.Sleep(retryDelay)
				} else {
					// 普通错误，重置 UUID 重试计数
					duplicateRetryCount = 0
					time.Sleep(time.Duration(vs.pulseSeconds) * time.Second)
				}

				// 清空 Leader 信息
				newLeader = ""
				vs.store.MasterAddress = ""
			} else {
				// 连接成功，重置重试计数
				duplicateRetryCount = 0
			}

			// 检查是否停止心跳
			if !vs.isHeartbeating {
				break
			}
		}
	}
}

// StopHeartbeat 停止心跳循环
// 用于优雅关闭 Volume Server
//
// 功能:
//   - 设置 isHeartbeating=false 停止心跳循环
//   - 关闭 stopChan 通知心跳 goroutine
//
// 返回:
//   - isAlreadyStopping: true 表示已经停止，false 表示刚停止
//
// 使用场景:
//   - Volume Server 关闭
//   - 离开集群（VolumeServerLeave）
func (vs *VolumeServer) StopHeartbeat() (isAlreadyStopping bool) {
	if !vs.isHeartbeating {
		// 已经停止
		return true
	}
	// 设置停止标志
	vs.isHeartbeating = false
	// 关闭 channel 通知 goroutine
	close(vs.stopChan)
	return false
}

// doHeartbeat 执行心跳（不带重试计数）
// 简单包装 doHeartbeatWithRetry
//
// 参数:
//   - masterAddress: Master 地址
//   - grpcDialOption: gRPC 拨号选项
//   - sleepInterval: 心跳间隔
//
// 返回:
//   - newLeader: 新 Leader 地址（如果检测到）
//   - err: 心跳错误
func (vs *VolumeServer) doHeartbeat(masterAddress pb.ServerAddress, grpcDialOption grpc.DialOption, sleepInterval time.Duration) (newLeader pb.ServerAddress, err error) {
	return vs.doHeartbeatWithRetry(masterAddress, grpcDialOption, sleepInterval, 0)
}

// doHeartbeatWithRetry 执行心跳的核心实现（带 UUID 冲突重试）
// 建立 gRPC 双向流，持续发送心跳并接收 Master 响应
//
// 功能:
//   - 建立与 Master 的 gRPC 双向流连接
//   - 发送初始心跳（Volume 列表 + EC Shard 列表）
//   - 启动接收 goroutine 监听 Master 响应
//   - 使用多个定时器触发不同类型的心跳
//   - 通过 Channel 接收 Volume/EC Shard 变化并实时同步
//   - 检测 UUID 冲突并触发重试
//   - 检测 Master Leader 变化并切换
//
// 参数:
//   - masterAddress: Master 地址
//   - grpcDialOption: gRPC 拨号选项
//   - sleepInterval: 心跳间隔（默认 30 秒）
//   - duplicateRetryCount: UUID 冲突重试计数
//
// 心跳类型:
//   1. 定时全量心跳（Volume 列表）- 每 sleepInterval 触发
//   2. 定时 EC 心跳（EC Shard 列表）- 每 17*sleepInterval 触发
//   3. 增量心跳（新增 Volume）- 立即通知
//   4. 增量心跳（新增 EC Shard）- 立即通知
//   5. 增量心跳（删除 Volume）- 立即通知
//   6. 增量心跳（删除 EC Shard）- 立即通知
//
// Master 响应处理:
//   - DuplicatedUuids: UUID 冲突，触发重试或退出
//   - Preallocate: 更新预分配配置
//   - VolumeSizeLimit: 更新卷大小限制
//   - Leader: 检测到新 Leader，切换连接
//
// UUID 冲突处理:
//   - Master 返回重复的 UUID 列表
//   - 重试次数 < 3: 返回错误触发重试
//   - 重试次数 >= 3: 认为是真实冲突，退出进程
//
// 返回:
//   - newLeader: 新 Leader 地址（如果检测到 Leader 变化）
//   - err: 心跳错误（连接失败、UUID 冲突重试等）
func (vs *VolumeServer) doHeartbeatWithRetry(masterAddress pb.ServerAddress, grpcDialOption grpc.DialOption, sleepInterval time.Duration, duplicateRetryCount int) (newLeader pb.ServerAddress, err error) {

	// 创建可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 建立 gRPC 连接
	grpcConnection, err := pb.GrpcDial(ctx, masterAddress.ToGrpcAddress(), false, grpcDialOption)
	if err != nil {
		return "", fmt.Errorf("fail to dial %s : %v", masterAddress, err)
	}
	defer grpcConnection.Close()

	// 创建 gRPC 客户端
	client := master_pb.NewSeaweedClient(grpcConnection)

	// 建立双向流
	stream, err := client.SendHeartbeat(ctx)
	if err != nil {
		glog.V(0).Infof("SendHeartbeat to %s: %v", masterAddress, err)
		return "", err
	}
	glog.V(0).Infof("Heartbeat to: %v", masterAddress)
	vs.currentMaster = masterAddress

	// 【接收 goroutine】用于接收 Master 响应的错误或完成信号
	// 负责处理 Master 返回的配置更新、UUID 冲突、Leader 变化等
	doneChan := make(chan error, 1)

	go func() {
		// 持续监听 Master 的响应消息
		for {
			// 接收 Master 的心跳响应
			in, err := stream.Recv()
			if err != nil {
				// 连接错误，通知主循环重连
				doneChan <- err
				return
			}

			// 【UUID 冲突检测】
			// Master 检测到此 Volume Server 使用的目录 UUID 与其他节点重复
			// 这可能是因为：
			//   1. 多个 Volume Server 错误地使用了同一个存储目录
			//   2. 重启时的瞬时竞争条件（罕见）
			if len(in.DuplicatedUuids) > 0 {
				// 找出哪些本地目录的 UUID 重复了
				var duplicateDir []string
				for _, loc := range vs.store.Locations {
					for _, uuid := range in.DuplicatedUuids {
						if uuid == loc.DirectoryUuid {
							duplicateDir = append(duplicateDir, loc.Directory)
						}
					}
				}

				// 实现重试逻辑处理可能的竞争条件
				// 最多重试 3 次，避免瞬时冲突被误判
				const maxRetries = 3
				if duplicateRetryCount < maxRetries {
					// 指数退避：2s、4s、8s
					retryDelay := time.Duration(1<<duplicateRetryCount) * 2 * time.Second
					glog.Errorf("Master 报告重复的卷目录: %v (重试 %d/%d)", duplicateDir, duplicateRetryCount+1, maxRetries)
					glog.Errorf("这可能是重连时的竞争条件。等待 %v 后重试...", retryDelay)

					// 返回错误触发重试，增加重试计数
					doneChan <- fmt.Errorf("duplicate UUIDs detected, retrying connection (attempt %d/%d)", duplicateRetryCount+1, maxRetries)
					return
				} else {
					// 重试 3 次后仍然冲突，这是真正的重复目录问题
					glog.Errorf("在 %d 次重试后由于持续的重复卷目录而关闭 Volume Server: %v", maxRetries, duplicateDir)
					glog.Errorf("请检查是否有另一个 Volume Server 正在使用相同的目录")
					os.Exit(1)
				}
			}

			// 【配置更新处理】
			// Master 可以动态调整 Volume Server 的配置
			volumeOptsChanged := false

			// 预分配（Preallocate）配置变更
			// Preallocate=true 时，创建 Volume 会预先分配全部 32GB 空间
			// 优点：避免碎片化，性能更稳定
			// 缺点：占用磁盘空间，即使未存储文件
			if vs.store.GetPreallocate() != in.GetPreallocate() {
				vs.store.SetPreallocate(in.GetPreallocate())
				volumeOptsChanged = true
			}

			// Volume 大小限制配置变更
			// 限制每个 Volume 的最大大小（默认 32GB）
			if in.GetVolumeSizeLimit() != 0 && vs.store.GetVolumeSizeLimit() != in.GetVolumeSizeLimit() {
				vs.store.SetVolumeSizeLimit(in.GetVolumeSizeLimit())
				volumeOptsChanged = true
			}

			// 如果配置发生变化，重新计算最大 Volume 数
			// 并立即发送更新后的心跳信息
			if volumeOptsChanged {
				// 根据新配置调整 Volume 数量限制
				if vs.store.MaybeAdjustVolumeMax() {
					// 发送更新后的心跳
					if err = stream.Send(vs.store.CollectHeartbeat()); err != nil {
						glog.V(0).Infof("Volume Server 无法与 master %s 通信: %v", vs.currentMaster, err)
						return
					}
				}
			}

			// 【Leader 切换检测】
			// Master 集群中的 Leader 可能发生变化（Raft Leader 选举）
			// 需要切换到新 Leader 继续心跳
			if in.GetLeader() != "" && string(vs.currentMaster) != in.GetLeader() {
				glog.V(0).Infof("Volume Server 发现新的 master newLeader: %v 而不是 %v", in.GetLeader(), vs.currentMaster)
				newLeader = pb.ServerAddress(in.GetLeader())
				doneChan <- nil
				return
			}
		}
	}()

	// 【发送初始心跳】
	// 连接成功后立即发送 Volume 列表心跳
	// 让 Master 知道这个 Volume Server 的当前状态
	if err = stream.Send(vs.store.CollectHeartbeat()); err != nil {
		glog.V(0).Infof("Volume Server 无法与 master %s 通信: %v", masterAddress, err)
		return "", err
	}

	// 发送 EC Shard 列表心跳
	// 让 Master 知道这个 Volume Server 的 EC Shard 状态
	if err = stream.Send(vs.store.CollectErasureCodingHeartbeat()); err != nil {
		glog.V(0).Infof("Volume Server 无法与 master %s 通信: %v", masterAddress, err)
		return "", err
	}

	// 【心跳定时器】
	// volumeTickChan: 每 sleepInterval（默认 30 秒）触发一次全量 Volume 心跳
	volumeTickChan := time.NewTicker(sleepInterval)
	defer volumeTickChan.Stop()

	// ecShardTickChan: 每 17*sleepInterval（默认 510 秒 = 8.5 分钟）触发一次 EC Shard 心跳
	// 17 倍是为了避免与其他定时器冲突，同时 EC Shard 变化频率较低
	ecShardTickChan := time.NewTicker(17 * sleepInterval)
	defer ecShardTickChan.Stop()

	// 缓存位置信息，避免重复读取
	dataCenter := vs.store.GetDataCenter()
	rack := vs.store.GetRack()
	ip := vs.store.Ip
	port := uint32(vs.store.Port)

	// 【心跳主循环】
	// 监听多种事件：定时心跳、Volume 变化、EC Shard 变化、停止信号
	for {
		select {
		// 【新增 Volume 通知】
		// 当有新 Volume 创建时，立即通知 Master
		case volumeMessage := <-vs.store.NewVolumesChan:
			// 构造增量心跳消息（仅包含新增的 Volume）
			deltaBeat := &master_pb.Heartbeat{
				Ip:         ip,
				Port:       port,
				DataCenter: dataCenter,
				Rack:       rack,
				NewVolumes: []*master_pb.VolumeShortInformationMessage{
					&volumeMessage,
				},
			}
			glog.V(0).Infof("volume server %s:%d 添加 volume %d", vs.store.Ip, vs.store.Port, volumeMessage.Id)
			// 立即发送，让 Master 尽快知道新 Volume
			if err = stream.Send(deltaBeat); err != nil {
				glog.V(0).Infof("Volume Server 无法向 master %s 更新: %v", masterAddress, err)
				return "", err
			}

		// 【新增 EC Shard 通知】
		// 当有新 EC Shard 创建时，立即通知 Master
		case ecShardMessage := <-vs.store.NewEcShardsChan:
			// 构造增量心跳消息（仅包含新增的 EC Shard）
			deltaBeat := &master_pb.Heartbeat{
				Ip:         ip,
				Port:       port,
				DataCenter: dataCenter,
				Rack:       rack,
				NewEcShards: []*master_pb.VolumeEcShardInformationMessage{
					&ecShardMessage,
				},
			}
			glog.V(0).Infof("volume server %s:%d 添加 ec shard %d:%d", vs.store.Ip, vs.store.Port, ecShardMessage.Id,
				erasure_coding.ShardBits(ecShardMessage.EcIndexBits).ShardIds())
			// 立即发送，让 Master 尽快知道新 EC Shard
			if err = stream.Send(deltaBeat); err != nil {
				glog.V(0).Infof("Volume Server 无法向 master %s 更新: %v", masterAddress, err)
				return "", err
			}

		// 【删除 Volume 通知】
		// 当 Volume 被删除时，立即通知 Master
		case volumeMessage := <-vs.store.DeletedVolumesChan:
			// 构造增量心跳消息（仅包含删除的 Volume）
			deltaBeat := &master_pb.Heartbeat{
				Ip:         ip,
				Port:       port,
				DataCenter: dataCenter,
				Rack:       rack,
				DeletedVolumes: []*master_pb.VolumeShortInformationMessage{
					&volumeMessage,
				},
			}
			glog.V(0).Infof("volume server %s:%d 删除 volume %d", vs.store.Ip, vs.store.Port, volumeMessage.Id)
			// 立即发送，让 Master 尽快更新拓扑
			if err = stream.Send(deltaBeat); err != nil {
				glog.V(0).Infof("Volume Server 无法向 master %s 更新: %v", masterAddress, err)
				return "", err
			}

		// 【删除 EC Shard 通知】
		// 当 EC Shard 被删除时，立即通知 Master
		case ecShardMessage := <-vs.store.DeletedEcShardsChan:
			// 构造增量心跳消息（仅包含删除的 EC Shard）
			deltaBeat := &master_pb.Heartbeat{
				Ip:         ip,
				Port:       port,
				DataCenter: dataCenter,
				Rack:       rack,
				DeletedEcShards: []*master_pb.VolumeEcShardInformationMessage{
					&ecShardMessage,
				},
			}
			glog.V(0).Infof("volume server %s:%d 删除 ec shard %d:%d", vs.store.Ip, vs.store.Port, ecShardMessage.Id,
				erasure_coding.ShardBits(ecShardMessage.EcIndexBits).ShardIds())
			// 立即发送，让 Master 尽快更新拓扑
			if err = stream.Send(deltaBeat); err != nil {
				glog.V(0).Infof("Volume Server 无法向 master %s 更新: %v", masterAddress, err)
				return "", err
			}

		// 【定时全量 Volume 心跳】
		// 每 sleepInterval（默认 30 秒）触发一次
		// 发送所有 Volume 的完整信息
		case <-volumeTickChan.C:
			glog.V(4).Infof("volume server %s:%d heartbeat", vs.store.Ip, vs.store.Port)
			// 检查是否需要调整 Volume 数量上限
			vs.store.MaybeAdjustVolumeMax()
			// 发送全量 Volume 心跳
			if err = stream.Send(vs.store.CollectHeartbeat()); err != nil {
				glog.V(0).Infof("Volume Server 无法与 master %s 通信: %v", masterAddress, err)
				return "", err
			}

		// 【定时 EC Shard 心跳】
		// 每 17*sleepInterval（默认 8.5 分钟）触发一次
		// 发送所有 EC Shard 的完整信息
		case <-ecShardTickChan.C:
			glog.V(4).Infof("volume server %s:%d ec heartbeat", vs.store.Ip, vs.store.Port)
			// 发送全量 EC Shard 心跳
			if err = stream.Send(vs.store.CollectErasureCodingHeartbeat()); err != nil {
				glog.V(0).Infof("Volume Server 无法与 master %s 通信: %v", masterAddress, err)
				return "", err
			}

		// 【接收 goroutine 完成】
		// doneChan 接收到消息表示连接断开或需要切换 Leader
		case err = <-doneChan:
			return

		// 【停止信号】
		// Volume Server 关闭或离开集群时触发
		case <-vs.stopChan:
			// 发送空心跳，表示此 Volume Server 不再提供服务
			// Master 会从拓扑中移除此节点
			var volumeMessages []*master_pb.VolumeInformationMessage
			emptyBeat := &master_pb.Heartbeat{
				Ip:           ip,
				Port:         port,
				PublicUrl:    vs.store.PublicUrl,
				MaxFileKey:   uint64(0),
				DataCenter:   dataCenter,
				Rack:         rack,
				Volumes:      volumeMessages,
				HasNoVolumes: len(volumeMessages) == 0,
			}
			glog.V(1).Infof("volume server %s:%d 停止并删除所有 volumes", vs.store.Ip, vs.store.Port)
			// 发送空心跳通知 Master
			if err = stream.Send(emptyBeat); err != nil {
				glog.V(0).Infof("Volume Server 无法向 master %s 更新: %v", masterAddress, err)
				return "", err
			}
			return
		}
	}
}
