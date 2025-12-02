// Package weed_server 中的 filer_grpc_server_dlm.go 实现分布式锁管理相关的 gRPC 接口
// 提供锁的申请、释放、查询以及在集群拓扑变化时的迁移。
package weed_server

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DistributedLock 处理 FilerServer 的锁申请请求
// 支持当锁不在当前节点时自动转发，确保客户端透明
func (fs *FilerServer) DistributedLock(ctx context.Context, req *filer_pb.LockRequest) (resp *filer_pb.LockResponse, err error) {

	// 记录锁请求的详细信息,便于调试和追踪
	glog.V(4).Infof("FILER LOCK: Received DistributedLock request - name=%s owner=%s renewToken=%s secondsToLock=%d isMoved=%v",
		req.Name, req.Owner, req.RenewToken, req.SecondsToLock, req.IsMoved)

	// 初始化响应对象
	resp = &filer_pb.LockResponse{}

	// 计算锁的过期时间戳(纳秒级)
	// 当前时间 + 锁定秒数 = 过期时间
	var movedTo pb.ServerAddress
	expiredAtNs := time.Now().Add(time.Duration(req.SecondsToLock) * time.Second).UnixNano()

	// 尝试在本地 DLM (分布式锁管理器) 中获取锁
	// 返回值:
	//   - LockOwner: 当前锁的持有者
	//   - RenewToken: 续期令牌,用于后续释放或续期
	//   - movedTo: 如果锁不在当前节点,返回目标节点地址
	//   - err: 错误信息
	resp.LockOwner, resp.RenewToken, movedTo, err = fs.filer.Dlm.LockWithTimeout(req.Name, expiredAtNs, req.RenewToken, req.Owner)
	glog.V(4).Infof("FILER LOCK: LockWithTimeout result - name=%s lockOwner=%s renewToken=%s movedTo=%s err=%v",
		req.Name, resp.LockOwner, resp.RenewToken, movedTo, err)
	glog.V(4).Infof("lock %s %v %v %v, isMoved=%v %v", req.Name, req.SecondsToLock, req.RenewToken, req.Owner, req.IsMoved, movedTo)

	// 【锁转发逻辑】
	// 如果锁已经迁移到其他节点,并且不是已经转发过的请求,则转发到正确的节点
	// 条件:
	//   1. movedTo 不为空 (锁在其他节点)
	//   2. movedTo 不等于当前主机 (避免自己转发给自己)
	//   3. !req.IsMoved (不是已经转发过的请求,避免无限循环)
	if movedTo != "" && movedTo != fs.option.Host && !req.IsMoved {
		glog.V(0).Infof("FILER LOCK: Forwarding to correct filer - from=%s to=%s", fs.option.Host, movedTo)
		// 连接到目标 Filer 节点并重新请求锁
		err = pb.WithFilerClient(false, 0, movedTo, fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			// 设置 IsMoved=true 防止目标节点再次转发
			secondResp, err := client.DistributedLock(ctx, &filer_pb.LockRequest{
				Name:          req.Name,
				SecondsToLock: req.SecondsToLock,
				RenewToken:    req.RenewToken,
				IsMoved:       true, // 标记为已转发
				Owner:         req.Owner,
			})
			if err == nil {
				// 转发成功,使用目标节点返回的结果
				resp.RenewToken = secondResp.RenewToken
				resp.LockOwner = secondResp.LockOwner
				resp.Error = secondResp.Error
				glog.V(0).Infof("FILER LOCK: Forwarded lock acquired - name=%s renewToken=%s", req.Name, resp.RenewToken)
			} else {
				glog.V(0).Infof("FILER LOCK: Forward failed - name=%s err=%v", req.Name, err)
			}
			return err
		})
	}

	// 如果有错误,将错误信息添加到响应中
	if err != nil {
		resp.Error = fmt.Sprintf("%v", err)
		glog.V(0).Infof("FILER LOCK: Error - name=%s error=%s", req.Name, resp.Error)
	}

	// 如果锁已迁移,在响应中记录目标地址
	if movedTo != "" {
		resp.LockHostMovedTo = string(movedTo)
	}

	// 记录最终返回的响应信息
	glog.V(4).Infof("FILER LOCK: Returning response - name=%s renewToken=%s lockOwner=%s error=%s movedTo=%s",
		req.Name, resp.RenewToken, resp.LockOwner, resp.Error, resp.LockHostMovedTo)

	return resp, nil
}

// DistributedUnlock 释放指定的锁
// 若锁已经迁移则自动转发到新节点执行
func (fs *FilerServer) DistributedUnlock(ctx context.Context, req *filer_pb.UnlockRequest) (resp *filer_pb.UnlockResponse, err error) {

	// 初始化响应对象
	resp = &filer_pb.UnlockResponse{}

	// 尝试在本地 DLM 中释放锁
	// 需要提供锁名称和续期令牌进行验证
	var movedTo pb.ServerAddress
	movedTo, err = fs.filer.Dlm.Unlock(req.Name, req.RenewToken)

	// 【锁转发逻辑】
	// 如果锁已经迁移到其他节点,并且不是已经转发过的请求,则转发到正确的节点
	// 条件:
	//   1. !req.IsMoved (不是已经转发过的请求)
	//   2. movedTo 不为空 (锁在其他节点)
	if !req.IsMoved && movedTo != "" {
		// 连接到目标 Filer 节点并重新请求解锁
		err = pb.WithFilerClient(false, 0, movedTo, fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			// 设置 IsMoved=true 防止目标节点再次转发
			secondResp, err := client.DistributedUnlock(ctx, &filer_pb.UnlockRequest{
				Name:       req.Name,
				RenewToken: req.RenewToken,
				IsMoved:    true, // 标记为已转发
			})
			// 使用目标节点返回的错误信息
			resp.Error = secondResp.Error
			return err
		})
	}

	// 如果有错误,将错误信息添加到响应中
	if err != nil {
		resp.Error = fmt.Sprintf("%v", err)
	}

	// 如果锁已迁移,在响应中记录目标地址
	if movedTo != "" {
		resp.MovedTo = string(movedTo)
	}

	return resp, nil

}

// FindLockOwner 查询指定锁当前的持有者
// 若锁已迁移则再次转发，找不到时返回 NotFound
func (fs *FilerServer) FindLockOwner(ctx context.Context, req *filer_pb.FindLockOwnerRequest) (*filer_pb.FindLockOwnerResponse, error) {
	// 尝试在本地 DLM 中查找锁的持有者
	// 返回值:
	//   - owner: 锁的当前持有者
	//   - movedTo: 如果锁不在当前节点,返回目标节点地址
	//   - err: 错误信息(可能是 LockNotFound)
	owner, movedTo, err := fs.filer.Dlm.FindLockOwner(req.Name)

	// 【锁转发逻辑】
	// 如果满足以下任一条件,需要转发到目标节点查询:
	//   1. 锁已迁移 (!req.IsMoved && movedTo != "")
	//   2. 锁未找到 (err == lock_manager.LockNotFound)
	if !req.IsMoved && movedTo != "" || err == lock_manager.LockNotFound {
		// 连接到目标 Filer 节点并重新查询
		err = pb.WithFilerClient(false, 0, movedTo, fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			// 设置 IsMoved=true 防止目标节点再次转发
			secondResp, err := client.FindLockOwner(ctx, &filer_pb.FindLockOwnerRequest{
				Name:    req.Name,
				IsMoved: true, // 标记为已转发
			})
			if err != nil {
				return err
			}
			// 使用目标节点返回的持有者信息
			owner = secondResp.Owner
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	// 如果最终仍然没有找到持有者,返回 NotFound 错误
	if owner == "" {
		glog.V(0).Infof("find lock %s moved to %v: %v", req.Name, movedTo, err)
		return nil, status.Error(codes.NotFound, fmt.Sprintf("lock %s not found", req.Name))
	}

	// 如果有其他错误,返回 Internal 错误
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// 返回锁的持有者信息
	return &filer_pb.FindLockOwnerResponse{
		Owner: owner,
	}, nil
}

// TransferLocks 在节点上线或拓扑调整时批量同步锁信息
func (fs *FilerServer) TransferLocks(ctx context.Context, req *filer_pb.TransferLocksRequest) (*filer_pb.TransferLocksResponse, error) {

	// 批量接收并插入迁移过来的锁信息
	// 这通常发生在以下场景:
	//   1. 集群拓扑发生变化 (新节点加入或节点下线)
	//   2. 一致性哈希环重新分配锁的归属
	//   3. 节点主动推送不再归属自己的锁
	for _, lock := range req.Locks {
		// 将锁信息插入本地 DLM
		// 参数:
		//   - lock.Name: 锁名称
		//   - lock.ExpiredAtNs: 过期时间戳(纳秒)
		//   - lock.RenewToken: 续期令牌
		//   - lock.Owner: 锁的持有者
		fs.filer.Dlm.InsertLock(lock.Name, lock.ExpiredAtNs, lock.RenewToken, lock.Owner)
	}

	// 返回空响应表示接收成功
	return &filer_pb.TransferLocksResponse{}, nil

}

// OnDlmChangeSnapshot 响应 DLM 拓扑快照变更事件
// 会将不再归属当前节点的锁主动推送到目标节点
func (fs *FilerServer) OnDlmChangeSnapshot(snapshot []pb.ServerAddress) {
	// 【DLM 拓扑变更处理】
	// 当集群拓扑发生变化时(节点加入/退出),此函数被调用
	// snapshot: 新的 Filer 节点列表

	// 从本地 DLM 中筛选出不再归属当前节点的锁
	// 基于一致性哈希算法,根据新的节点列表重新计算锁的归属
	locks := fs.filer.Dlm.SelectNotOwnedLocks(snapshot)
	if len(locks) == 0 {
		// 没有需要迁移的锁,直接返回
		return
	}

	// 遍历所有需要迁移的锁,将它们推送到正确的目标节点
	for _, lock := range locks {
		// 根据一致性哈希算法计算这个锁应该归属哪个节点
		server := fs.filer.Dlm.CalculateTargetServer(lock.Key, snapshot)

		// 使用超时上下文,避免锁迁移在网络异常时长时间阻塞
		// 5 秒超时是一个合理的值:
		//   - 足够完成正常的网络传输
		//   - 不会因为网络问题长时间阻塞
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		// 连接到目标节点并转移锁信息
		err := pb.WithFilerClient(false, 0, server, fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			// 调用目标节点的 TransferLocks 接口
			_, err := client.TransferLocks(ctx, &filer_pb.TransferLocksRequest{
				Locks: []*filer_pb.Lock{
					{
						Name:        lock.Key,        // 锁名称
						RenewToken:  lock.Token,      // 续期令牌
						ExpiredAtNs: lock.ExpiredAtNs, // 过期时间
						Owner:       lock.Owner,      // 持有者
					},
				},
			})
			return err
		})
		cancel() // 及时释放超时定时器资源

		if err != nil {
			// 此处不重试的原因:
			//   1. 锁可能已经过期,无需迁移
			//   2. 目标节点可能已经通过其他方式获得了这个锁
			//   3. 避免因重试导致的额外网络开销
			// 仅记录错误日志供运维人员排查
			glog.Errorf("transfer lock %v to %v: %v", lock.Key, server, err)
		}
	}

}
