// Package weed_server 中的 master_grpc_server_admin.go 实现 Master 的管理功能 gRPC 服务
//
// 核心功能：
//   1. 分布式排他锁（Admin Lock）：用于 Shell 执行敏感操作时加锁，防止并发冲突
//   2. Ping 功能：检测 Master、Filer、Volume Server 的连通性和时间同步
//
// 管理锁机制：
//   - Shell 在执行敏感操作（如集群维护、数据迁移）时需要先获取管理锁
//   - 锁有 10 秒的有效期，需要定期续期（renew）
//   - 使用随机 token 确保锁的唯一性，防止误操作
//   - 支持多个命名锁，不同操作可以使用不同的锁名
//
// Ping 功能：
//   - 检测目标节点是否在线
//   - 测量网络延迟（往返时间）
//   - 检测时钟偏移（通过比对本地时间和远程时间）
package weed_server

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/seaweedfs/raft"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

/*
管理锁（Admin Lock）工作原理
==================================

Shell 客户端行为：
-----------------
获取锁时：
  * 向 Master 申请管理锁（lockName, clientName）
  * Master 返回 (lockTime, token)
  * 启动后台 goroutine 定期续期（每 5 秒续一次，锁有效期 10 秒）

释放锁时：
  * 停止续期 goroutine
  * 向 Master 发送释放锁请求（带上 lockTime 和 token）

Master 服务端行为：
------------------
Master 为每个锁名（lockName）维护：
  * accessSecret（随机 token）
  * accessLockTime（锁获取时间）
  * lastClient（最后一个持有锁的客户端）
  * lastMessage（锁的用途说明）

收到租约/续期请求时：
  If lastLockTime 仍然有效（未超过 10 秒）{
    if 是续期请求 && token 有效 {
      // 续期成功
      生成新的 randomNumber => token
      更新 accessLockTime
      return (新 token, 新 lockTime)
    }
    // 拒绝：锁被其他客户端占用
    return error "already locked by XXX"
  } else {
    // 首次获取锁
    生成 randomNumber => token
    记录 lockTime、客户端等信息
    return (token, lockTime)
  }

收到释放锁请求时：
  验证 token 和 lockTime
  if 验证通过 {
    删除锁记录
  }

设计要点：
---------
1. Volume Server 不需要验证锁：锁机制仅在 Master 层面实现，简化了架构
2. 锁是可选的：类似 Go 语言的 sync.Mutex，不强制要求，由调用方决定是否加锁
3. 支持续期：避免长时间操作因锁超时而失败
4. 命名锁：不同类型的操作可以使用不同的锁名（如 "volume.balance"、"volume.fix.replication"）
*/

const (
	// LockDuration 管理锁的有效期
	// 10 秒后锁自动失效，客户端需要在此之前续期
	LockDuration = 10 * time.Second
)

// AdminLock 表示一个管理锁实例
// 每个命名锁（lockName）对应一个 AdminLock 对象
type AdminLock struct {
	accessSecret   int64     // 随机生成的 token，用于验证锁的持有者
	accessLockTime time.Time // 锁获取的时间戳，用于判断锁是否过期
	lastClient     string    // 最后一个持有锁的客户端名称（如 "shell-hostname-pid"）
	lastMessage    string    // 锁的用途说明（如 "balancing volumes"）
}

// AdminLocks 管理所有的管理锁
// 使用 map 存储不同命名的锁，支持并发访问
type AdminLocks struct {
	locks map[string]*AdminLock // key: lockName, value: 锁实例
	sync.RWMutex                // 读写锁，保护 locks map 的并发访问
}

// NewAdminLocks 创建管理锁管理器
func NewAdminLocks() *AdminLocks {
	return &AdminLocks{
		locks: make(map[string]*AdminLock),
	}
}

// isLocked 检查指定的锁是否被占用
//
// 参数:
//   - lockName: 锁名称
//
// 返回:
//   - clientName: 持有锁的客户端名称
//   - message: 锁的用途说明
//   - isLocked: 锁是否仍然有效（未超过 10 秒）
//
// 判断逻辑:
//   - 锁不存在 → 未被占用
//   - 锁存在但已超过 LockDuration（10 秒）→ 已过期，视为未占用
//   - 锁存在且未过期 → 被占用
func (locks *AdminLocks) isLocked(lockName string) (clientName string, message string, isLocked bool) {
	locks.RLock()
	defer locks.RUnlock()
	adminLock, found := locks.locks[lockName]
	if !found {
		return "", "", false
	}
	glog.V(4).Infof("isLocked %v: %v", adminLock.lastClient, adminLock.lastMessage)
	// 检查锁是否过期：accessLockTime + LockDuration > 当前时间
	return adminLock.lastClient, adminLock.lastMessage, adminLock.accessLockTime.Add(LockDuration).After(time.Now())
}

// isValidToken 验证锁的 token 是否有效
//
// 参数:
//   - lockName: 锁名称
//   - ts: 锁获取的时间戳
//   - token: 锁的 token
//
// 返回:
//   - bool: token 是否有效
//
// 验证条件:
//   - 锁必须存在
//   - 时间戳必须完全匹配（精确到纳秒）
//   - token 必须完全匹配
func (locks *AdminLocks) isValidToken(lockName string, ts time.Time, token int64) bool {
	locks.RLock()
	defer locks.RUnlock()
	adminLock, found := locks.locks[lockName]
	if !found {
		return false
	}
	return adminLock.accessLockTime.Equal(ts) && adminLock.accessSecret == token
}

// generateToken 生成新的锁 token
//
// 参数:
//   - lockName: 锁名称
//   - clientName: 客户端名称
//
// 返回:
//   - ts: 锁获取的时间戳
//   - token: 随机生成的 token
//
// 功能:
//   - 生成随机 token（64 位整数）
//   - 记录锁获取时间
//   - 记录客户端信息
//   - 更新 Prometheus metrics（锁状态设为 1）
func (locks *AdminLocks) generateToken(lockName string, clientName string) (ts time.Time, token int64) {
	locks.Lock()
	defer locks.Unlock()
	lock := &AdminLock{
		accessSecret:   rand.Int64(),  // 生成随机 token
		accessLockTime: time.Now(),     // 记录当前时间
		lastClient:     clientName,     // 记录客户端名称
	}
	locks.locks[lockName] = lock
	// 更新 metrics：锁被获取
	stats.MasterAdminLock.WithLabelValues(clientName).Set(1)
	return lock.accessLockTime, lock.accessSecret
}

// deleteLock 删除指定的锁
//
// 参数:
//   - lockName: 锁名称
//
// 功能:
//   - 从 map 中删除锁记录
//   - 更新 Prometheus metrics（锁状态设为 0）
func (locks *AdminLocks) deleteLock(lockName string) {
	locks.Lock()
	// 更新 metrics：锁被释放
	stats.MasterAdminLock.WithLabelValues(locks.locks[lockName].lastClient).Set(0)
	defer locks.Unlock()
	delete(locks.locks, lockName)
}

// LeaseAdminToken 租用管理锁（Lease Admin Token）
//
// gRPC 方法：用于 Shell 获取或续期管理锁
//
// 参数:
//   - ctx: 上下文
//   - req: 请求对象，包含：
//     - LockName: 锁名称（如 "volume.balance"）
//     - ClientName: 客户端名称（用于标识持有锁的客户端）
//     - PreviousToken: 上一次的 token（续期时使用）
//     - PreviousLockTime: 上一次的锁获取时间（续期时使用）
//
// 返回:
//   - resp: 响应对象，包含：
//     - Token: 新生成的 token
//     - LockTsNs: 锁获取时间（纳秒）
//   - error: 错误信息
//
// 工作流程:
//   1. 检查当前节点是否为 Raft Leader（只有 Leader 可以分配锁）
//   2. 检查锁是否被占用：
//      a. 如果锁未被占用 → 分配新锁
//      b. 如果锁被占用 && 是续期请求 && token 有效 → 续期成功
//      c. 如果锁被占用 && 不是续期请求 → 拒绝，返回锁的持有者信息
//
// 使用场景:
//   - Shell 执行 volume.balance 前获取锁
//   - Shell 执行 volume.fix.replication 前获取锁
//   - 长时间操作需要定期调用此方法续期
func (ms *MasterServer) LeaseAdminToken(ctx context.Context, req *master_pb.LeaseAdminTokenRequest) (*master_pb.LeaseAdminTokenResponse, error) {
	resp := &master_pb.LeaseAdminTokenResponse{}

	// 【检查 1：只有 Leader 可以分配锁】
	if !ms.Topo.IsLeader() {
		return resp, raft.NotLeaderError
	}

	// 【检查 2：判断锁是否被占用】
	if lastClient, lastMessage, isLocked := ms.adminLocks.isLocked(req.LockName); isLocked {
		glog.V(4).Infof("LeaseAdminToken %v", lastClient)

		// 【情况 1：续期请求】
		// 如果客户端提供了 PreviousToken，说明是续期请求
		// 验证 token 和 lockTime 是否匹配
		if req.PreviousToken != 0 && ms.adminLocks.isValidToken(req.LockName, time.Unix(0, req.PreviousLockTime), req.PreviousToken) {
			// token 有效，续期成功
			ts, token := ms.adminLocks.generateToken(req.LockName, req.ClientName)
			resp.Token, resp.LockTsNs = token, ts.UnixNano()
			return resp, nil
		}

		// 【情况 2：锁被其他客户端占用】
		// 拒绝分配锁，返回持有锁的客户端信息
		return resp, fmt.Errorf("already locked by %v: %v", lastClient, lastMessage)
	}

	// 【情况 3：锁未被占用，分配新锁】
	ts, token := ms.adminLocks.generateToken(req.LockName, req.ClientName)
	resp.Token, resp.LockTsNs = token, ts.UnixNano()
	return resp, nil
}

// ReleaseAdminToken 释放管理锁
//
// gRPC 方法：用于 Shell 释放持有的管理锁
//
// 参数:
//   - ctx: 上下文
//   - req: 请求对象，包含：
//     - LockName: 锁名称
//     - PreviousToken: 锁的 token（用于验证）
//     - PreviousLockTime: 锁获取时间（用于验证）
//
// 返回:
//   - resp: 响应对象（空）
//   - error: 错误信息
//
// 工作流程:
//   1. 验证 token 和 lockTime 是否有效
//   2. 如果有效，删除锁记录
//   3. 如果无效，静默失败（不返回错误）
//
// 注意事项:
//   - 即使 token 无效也不会返回错误，确保幂等性
//   - 客户端可以重复调用此方法，不会有副作用
func (ms *MasterServer) ReleaseAdminToken(ctx context.Context, req *master_pb.ReleaseAdminTokenRequest) (*master_pb.ReleaseAdminTokenResponse, error) {
	resp := &master_pb.ReleaseAdminTokenResponse{}

	// 验证 token 和 lockTime，只有验证通过才删除锁
	if ms.adminLocks.isValidToken(req.LockName, time.Unix(0, req.PreviousLockTime), req.PreviousToken) {
		ms.adminLocks.deleteLock(req.LockName)
	}

	return resp, nil
}

// Ping 检测目标节点的连通性和时间同步
//
// gRPC 方法：用于检查集群中其他节点的状态
//
// 参数:
//   - ctx: 上下文
//   - req: 请求对象，包含：
//     - TargetType: 目标节点类型（Master、Filer、VolumeServer）
//     - Target: 目标节点地址（如 "localhost:9333"）
//
// 返回:
//   - resp: 响应对象，包含：
//     - StartTimeNs: 本地开始时间（纳秒）
//     - StopTimeNs: 本地结束时间（纳秒）
//     - RemoteTimeNs: 远程节点的时间（纳秒）
//   - pingErr: 错误信息
//
// 功能:
//   1. 检测连通性：目标节点是否在线
//   2. 测量延迟：StopTimeNs - StartTimeNs = 往返时间（RTT）
//   3. 检测时钟偏移：RemoteTimeNs vs StartTimeNs/StopTimeNs
//
// 时钟偏移计算：
//   假设网络延迟对称，则：
//   - 单向延迟 ≈ (StopTimeNs - StartTimeNs) / 2
//   - 远程时间 ≈ StartTimeNs + 单向延迟
//   - 时钟偏移 ≈ RemoteTimeNs - (StartTimeNs + 单向延迟)
//
// 使用场景:
//   - 集群健康检查
//   - 网络延迟监控
//   - 时钟同步检测（警告时钟偏移过大）
func (ms *MasterServer) Ping(ctx context.Context, req *master_pb.PingRequest) (resp *master_pb.PingResponse, pingErr error) {
	resp = &master_pb.PingResponse{
		StartTimeNs: time.Now().UnixNano(), // 记录本地开始时间
	}

	// 【情况 1：Ping Filer】
	if req.TargetType == cluster.FilerType {
		pingErr = pb.WithFilerClient(false, 0, pb.ServerAddress(req.Target), ms.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			// 调用 Filer 的 Ping 方法
			pingResp, err := client.Ping(ctx, &filer_pb.PingRequest{})
			if pingResp != nil {
				// 记录远程节点的时间
				resp.RemoteTimeNs = pingResp.StartTimeNs
			}
			return err
		})
	}

	// 【情况 2：Ping Volume Server】
	if req.TargetType == cluster.VolumeServerType {
		pingErr = pb.WithVolumeServerClient(false, pb.ServerAddress(req.Target), ms.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			// 调用 Volume Server 的 Ping 方法
			pingResp, err := client.Ping(ctx, &volume_server_pb.PingRequest{})
			if pingResp != nil {
				// 记录远程节点的时间
				resp.RemoteTimeNs = pingResp.StartTimeNs
			}
			return err
		})
	}

	// 【情况 3：Ping Master】
	if req.TargetType == cluster.MasterType {
		pingErr = pb.WithMasterClient(false, pb.ServerAddress(req.Target), ms.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
			// 调用另一个 Master 的 Ping 方法
			pingResp, err := client.Ping(ctx, &master_pb.PingRequest{})
			if pingResp != nil {
				// 记录远程节点的时间
				resp.RemoteTimeNs = pingResp.StartTimeNs
			}
			return err
		})
	}

	// 【错误处理】
	if pingErr != nil {
		pingErr = fmt.Errorf("ping %s %s: %v", req.TargetType, req.Target, pingErr)
	}

	// 记录本地结束时间
	resp.StopTimeNs = time.Now().UnixNano()
	return
}
