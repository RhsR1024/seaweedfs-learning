// Package topology 的容量预留测试文件
// 本文件测试卷创建时的容量预留（Capacity Reservation）机制
//
// 容量预留机制的核心价值：
//   1. 避免竞态条件：多个并发请求同时创建卷时，防止超额分配
//   2. 原子性保证：要么所有副本都成功创建，要么全部回滚
//   3. 防止资源竞争：在分配空间前先"锁定"容量，创建完成后释放
//
// 测试覆盖场景：
//   1. 基本预留和释放机制
//   2. 并发场景下的竞态条件预防
//   3. 预留失败时的回滚机制
//   4. 预留超时和自动清理
package topology

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// MockGrpcDialOption 模拟 gRPC 连接选项，用于测试
type MockGrpcDialOption struct{}

// simulateVolumeAllocation 模拟卷分配过程
// 用于测试环境，模拟实际卷创建的耗时操作
func simulateVolumeAllocation(server *DataNode, vid needle.VolumeId, option *VolumeGrowOption) error {
	// 模拟一些处理时间（10 毫秒）
	time.Sleep(time.Millisecond * 10)
	return nil
}

// TestVolumeGrowth_ReservationBasedAllocation 测试基于容量预留的卷分配机制
// 测试目标：
//   1. 验证容量预留机制的正确性
//   2. 确保预留容量正确反映在可用空间中
//   3. 验证卷创建后预留的正确释放
//
// 测试场景：
//   - 创建一个只有 5 个卷槽位的服务器
//   - 依次创建 5 个卷，每次都验证可用空间
//   - 最后验证第 6 个卷创建失败（容量已满）
func TestVolumeGrowth_ReservationBasedAllocation(t *testing.T) {
	// 【准备测试环境】
	// 创建一个简单的拓扑：1 个数据中心 → 1 个机架 → 1 个服务器
	// 这样可以确保测试行为可预测
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)

	// 【构建拓扑结构】
	// 数据中心
	dc := NewDataCenter("dc1")
	topo.LinkChildNode(dc)

	// 机架
	rack := NewRack("rack1")
	dc.LinkChildNode(rack)

	// 单个数据节点（有限容量）
	dn := NewDataNode("server1")
	rack.LinkChildNode(dn)

	// 【设置磁盘容量限制】
	// 创建磁盘对象，设置最大卷数量为 5
	// 这是测试的关键：只有 5 个槽位，用于验证预留和容量耗尽
	disk := NewDisk(types.HardDriveType.String())
	disk.diskUsages.getOrCreateDisk(types.HardDriveType).maxVolumeCount = 5
	dn.LinkChildNode(disk)

	// 【初始化卷增长管理器】
	vg := NewDefaultVolumeGrowth()

	// 副本策略 "000"：无副本，只有主副本（1 个物理卷）
	rp, _ := super_block.NewReplicaPlacementFromString("000")

	// 卷创建选项
	option := &VolumeGrowOption{
		Collection:       "test",               // 测试集合
		ReplicaPlacement: rp,                   // 无副本策略
		DiskType:         types.HardDriveType,  // HDD 磁盘类型
	}

	// 【核心测试：依次创建 5 个卷】
	// 每次创建都验证容量预留机制的正确性
	for i := 0; i < 5; i++ {
		// 【步骤 1：查找空闲槽位并预留容量】
		// useReservations=true：启用容量预留机制
		servers, reservation, err := vg.findEmptySlotsForOneVolume(topo, option, true)
		if err != nil {
			t.Errorf("Failed to find slots with reservation on iteration %d: %v", i, err)
			continue
		}

		// 【步骤 2：验证返回结果】
		// 验证服务器数量：副本策略 "000" 只需要 1 个服务器
		if len(servers) != 1 {
			t.Errorf("Expected 1 server for replica placement 000, got %d", len(servers))
		}

		// 验证预留 ID 数量：应该有 1 个预留记录
		if len(reservation.reservationIds) != 1 {
			t.Errorf("Expected 1 reservation ID, got %d", len(reservation.reservationIds))
		}

		// 验证预留在预期的服务器上
		server := servers[0]
		if server != dn {
			t.Errorf("Expected volume to be allocated on server1, got %s", server.Id())
		}

		// 【步骤 3：验证预留前的可用空间】
		// 可用空间应该等于：总容量 - 已创建的卷数量
		availableBeforeCreation := server.AvailableSpaceFor(option)
		expectedBefore := int64(5 - i)
		if availableBeforeCreation != expectedBefore {
			t.Errorf("Iteration %d: Expected %d base available space, got %d", i, expectedBefore, availableBeforeCreation)
		}

		// 【步骤 4：模拟卷创建成功】
		// 在实际代码中，这会通过 gRPC 调用 Volume Server
		// 这里我们直接修改磁盘使用统计来模拟创建

		// 获取磁盘对象（需要加锁访问 children map）
		dn.RLock()
		disk := dn.children[NodeId(types.HardDriveType.String())].(*Disk)
		dn.RUnlock()

		// 增加卷计数（模拟创建了 1 个卷）
		deltaDiskUsage := &DiskUsageCounts{
			volumeCount: 1,
		}
		disk.UpAdjustDiskUsageDelta(types.HardDriveType, deltaDiskUsage)

		// 【步骤 5：释放预留】
		// 卷创建成功后，必须释放预留
		// 这会将预留转换为实际占用
		reservation.releaseAllReservations()

		// 【步骤 6：验证创建后的可用空间】
		// 可用空间应该减少 1
		availableAfterCreation := server.AvailableSpaceFor(option)
		expectedAfter := int64(5 - i - 1)
		if availableAfterCreation != expectedAfter {
			t.Errorf("Iteration %d: Expected %d available space after creation, got %d", i, expectedAfter, availableAfterCreation)
		}
	}

	// 【验证容量耗尽场景】
	// 已经创建了 5 个卷，服务器容量已满
	// 第 6 次尝试应该失败
	_, _, err := vg.findEmptySlotsForOneVolume(topo, option, true)
	if err == nil {
		t.Error("Expected volume allocation to fail when server is at capacity")
	}
}

// TestVolumeGrowth_ConcurrentAllocationPreventsRaceCondition 测试并发分配场景下的竞态条件预防
// 测试目标：
//   1. 验证容量预留机制在并发场景下的正确性
//   2. 确保不会超额分配（最多只能成功 5 个）
//   3. 验证失败的请求数量符合预期
//
// 测试场景：
//   - 10 个并发请求同时尝试创建卷
//   - 服务器只有 5 个槽位
//   - 应该有 5 个成功，5 个失败
//
// 关键机制：
//   - 容量预留确保原子性
//   - commitMutex 确保卷创建和预留释放的原子性
func TestVolumeGrowth_ConcurrentAllocationPreventsRaceCondition(t *testing.T) {
	// 【准备测试环境】
	// 创建一个容量非常有限的拓扑结构
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)

	dc := NewDataCenter("dc1")
	topo.LinkChildNode(dc)
	rack := NewRack("rack1")
	dc.LinkChildNode(rack)

	// 单个数据节点，只有 5 个卷槽位
	// 这是关键：容量限制用于测试并发竞争
	dn := NewDataNode("server1")
	rack.LinkChildNode(dn)

	disk := NewDisk(types.HardDriveType.String())
	disk.diskUsages.getOrCreateDisk(types.HardDriveType).maxVolumeCount = 5
	dn.LinkChildNode(disk)

	vg := NewDefaultVolumeGrowth()
	rp, _ := super_block.NewReplicaPlacementFromString("000") // 无副本策略

	option := &VolumeGrowOption{
		Collection:       "test",
		ReplicaPlacement: rp,
		DiskType:         types.HardDriveType,
	}

	// 【并发测试设置】
	// 10 个并发请求竞争 5 个槽位
	const concurrentRequests = 10
	var wg sync.WaitGroup
	var successCount, failureCount atomic.Int32
	// commitMutex 确保卷创建和预留释放的原子性
	// 防止在卷创建后、预留释放前被其他goroutine看到不一致的状态
	var commitMutex sync.Mutex

	// 【启动并发 goroutine】
	for i := 0; i < concurrentRequests; i++ {
		wg.Add(1)
		go func(requestId int) {
			defer wg.Done()

			// 【步骤 1：尝试查找空闲槽位并预留】
			_, reservation, err := vg.findEmptySlotsForOneVolume(topo, option, true)

			if err != nil {
				// 【失败路径】：容量不足，预留失败
				failureCount.Add(1)
				t.Logf("Request %d failed as expected: %v", requestId, err)
			} else {
				// 【成功路径】：预留成功
				successCount.Add(1)
				t.Logf("Request %d succeeded, got reservation", requestId)

				// 【步骤 2：模拟卷创建完成】
				// 重要：必须在释放预留之前增加卷计数
				// 这确保了原子性：要么两个操作都完成，要么都不完成
				if reservation != nil {
					commitMutex.Lock()

					// 【2.1：增加卷计数】
					// 获取磁盘对象
					dn.RLock()
					disk := dn.children[NodeId(types.HardDriveType.String())].(*Disk)
					dn.RUnlock()

					// 模拟创建了 1 个卷
					deltaDiskUsage := &DiskUsageCounts{
						volumeCount: 1,
					}
					disk.UpAdjustDiskUsageDelta(types.HardDriveType, deltaDiskUsage)

					// 【2.2：释放预留】
					// 卷创建完成后，释放预留
					reservation.releaseAllReservations()

					commitMutex.Unlock()
				}
			}
		}(i)
	}

	// 【等待所有 goroutine 完成】
	wg.Wait()

	// 【验证测试结果】
	successes := successCount.Load()
	failures := failureCount.Load()
	total := successes + failures

	// 验证总请求数
	if total != concurrentRequests {
		t.Fatalf("Expected %d total attempts recorded, got %d", concurrentRequests, total)
	}

	// 验证成功数不超过容量
	const capacity = 5
	if successes > capacity {
		t.Errorf("Expected no more than %d successful reservations, got %d", capacity, successes)
	}

	// 验证失败数至少为超出容量的部分
	minExpectedFailures := concurrentRequests - capacity
	if failures < int32(minExpectedFailures) {
		t.Errorf("Expected at least %d failed reservations, got %d", minExpectedFailures, failures)
	}

	// 验证最终状态：可用空间应该等于容量减去成功数
	finalAvailable := dn.AvailableSpaceFor(option)
	expectedAvailable := int64(capacity - successes)
	if finalAvailable != expectedAvailable {
		t.Errorf("Expected %d available space after allocations, got %d", expectedAvailable, finalAvailable)
	}

	t.Logf("Concurrent test completed: %d successes, %d failures", successes, failures)
}

// TestVolumeGrowth_ReservationFailureRollback 测试预留失败时的回滚机制
// 测试目标：
//   1. 验证无法满足副本要求时预留失败
//   2. 确保失败的预留被正确回滚（不留下悬挂的预留）
//   3. 验证所有服务器的可用容量恢复正常
//
// 测试场景：
//   - 2 台服务器：server1 有 5 个槽位，server2 已满
//   - 副本策略 "010"：需要 2 台服务器（跨机架副本）
//   - 但是 rack1 只有 1 台服务器有空间
//   - 预留应该失败，且不留下任何预留记录
func TestVolumeGrowth_ReservationFailureRollback(t *testing.T) {
	// 【准备测试环境】
	// 创建包含多台服务器的拓扑，但总容量有限
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)

	dc := NewDataCenter("dc1")
	topo.LinkChildNode(dc)
	rack := NewRack("rack1")
	dc.LinkChildNode(rack)

	// 【创建两台容量不同的服务器】
	dn1 := NewDataNode("server1")
	dn2 := NewDataNode("server2")
	rack.LinkChildNode(dn1)
	rack.LinkChildNode(dn2)

	// Server 1: 5 个可用槽位
	disk1 := NewDisk(types.HardDriveType.String())
	disk1.diskUsages.getOrCreateDisk(types.HardDriveType).maxVolumeCount = 5
	dn1.LinkChildNode(disk1)

	// Server 2: 0 个可用槽位（已满）
	// 设置 maxVolumeCount=5, volumeCount=5，表示已满
	disk2 := NewDisk(types.HardDriveType.String())
	diskUsage2 := disk2.diskUsages.getOrCreateDisk(types.HardDriveType)
	diskUsage2.maxVolumeCount = 5
	diskUsage2.volumeCount = 5
	dn2.LinkChildNode(disk2)

	vg := NewDefaultVolumeGrowth()
	// 副本策略 "010"：跨机架 1 个副本（需要 2 台服务器）
	rp, _ := super_block.NewReplicaPlacementFromString("010")

	option := &VolumeGrowOption{
		Collection:       "test",
		ReplicaPlacement: rp,
		DiskType:         types.HardDriveType,
	}

	// 【执行预留尝试】
	// 应该失败，因为无法满足副本要求：
	//   - 需要 2 台服务器
	//   - 但只有 1 台服务器有空间
	_, _, err := vg.findEmptySlotsForOneVolume(topo, option, true)
	if err == nil {
		t.Error("Expected reservation to fail due to insufficient replica capacity")
	}

	// 【验证预留已回滚】
	// Server 1 应该仍然有全部 5 个槽位可用
	// 没有悬挂的预留记录
	available1 := dn1.AvailableSpaceForReservation(option)
	if available1 != 5 {
		t.Errorf("Expected server1 to have all capacity available after failed reservation, got %d", available1)
	}

	// Server 2 应该仍然是 0 个槽位（已满）
	available2 := dn2.AvailableSpaceForReservation(option)
	if available2 != 0 {
		t.Errorf("Expected server2 to have no capacity available, got %d", available2)
	}
}

// TestVolumeGrowth_ReservationTimeout 测试预留超时和自动清理机制
// 测试目标：
//   1. 验证过期的预留会被自动清理
//   2. 确保清理后新的预留可以成功
//   3. 验证可用空间计算正确
//
// 测试场景：
//   - 创建一个预留（2 个槽位）
//   - 手动设置预留时间为 10 分钟前（模拟超时）
//   - 尝试创建新预留（3 个槽位）
//   - 应该触发清理，新预留成功
//
// 预留超时机制：
//   - 预留有一个超时时间（通常 5 分钟）
//   - 超时的预留在下次分配时会被自动清理
//   - 这防止因客户端崩溃导致的预留泄漏
func TestVolumeGrowth_ReservationTimeout(t *testing.T) {
	// 【准备测试环境】
	dn := NewDataNode("server1")
	diskType := types.HardDriveType

	// 设置容量：5 个槽位
	diskUsage := dn.diskUsages.getOrCreateDisk(diskType)
	diskUsage.maxVolumeCount = 5

	// 【步骤 1：创建第一个预留】
	// 预留 2 个槽位
	reservationId, success := dn.TryReserveCapacity(diskType, 2)
	if !success {
		t.Fatal("Expected successful reservation")
	}

	// 【步骤 2：模拟预留超时】
	// 手动将预留时间设置为 10 分钟前
	// 这模拟了一个长时间未释放的预留（可能是客户端崩溃）
	dn.capacityReservations.Lock()
	if reservation, exists := dn.capacityReservations.reservations[reservationId]; exists {
		reservation.createdAt = time.Now().Add(-10 * time.Minute)
	}
	dn.capacityReservations.Unlock()

	// 【步骤 3：尝试创建新预留】
	// 预留 3 个槽位
	// 这应该触发过期预留的清理，然后成功
	_, success = dn.TryReserveCapacity(diskType, 3)
	if !success {
		t.Error("Expected reservation to succeed after cleanup of expired reservation")
	}

	// 【步骤 4：验证最终状态】
	// 原来的预留（2 个槽位）应该被清理
	// 新的预留（3 个槽位）应该生效
	// 可用空间 = 5 - 3 = 2
	option := &VolumeGrowOption{DiskType: diskType}
	available := dn.AvailableSpaceForReservation(option)
	if available != 2 {
		t.Errorf("Expected 2 available slots after cleanup and new reservation, got %d", available)
	}
}

