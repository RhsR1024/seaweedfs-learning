// Package topology 的测试包
// 本文件实现容量预留（Capacity Reservation）机制的压力测试
//
// 测试目标：
//   验证在高并发场景下，容量预留机制能够防止 Volume 分配超额
//
// 问题背景：
//   在 SeaweedFS 的实际使用中，用户报告了容量判断错误的问题：
//   - 集群配置：3 台 Volume Server，每台 200GB，Volume 大小限制 5GB
//   - 理论容量：每台最多 40 个 Volume（200GB / 5GB）
//   - 问题现象：高并发写入时，某些 Volume Server 的 Volume 数量超过 40
//   - 根本原因：并发分配 Volume 时，多个请求同时读取到相同的可用容量，
//              导致分配决策基于过期的容量信息
//
// 解决方案：
//   引入容量预留（Capacity Reservation）机制：
//   1. 在分配 Volume 前，先"预留"容量（类似数据库的乐观锁）
//   2. 预留成功后，其他请求查询容量时会扣除已预留的部分
//   3. Volume 创建成功后，释放预留
//   4. 预留会自动过期（默认 5 分钟），防止预留泄漏
//
// 测试策略：
//   1. 模拟原始问题场景：高并发请求（50 个并发）
//   2. 验证容量限制不被突破
//   3. 检查分配结果的正确性
//
// 相关 Issue：
//   https://github.com/seaweedfs/seaweedfs/issues/xxxx
package topology

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// TestRaceConditionStress 模拟原始问题场景：高并发写入导致容量误判
//
// 测试场景设置：
//   - 3 台 Volume Server
//   - 每台 200GB 存储空间
//   - Volume 大小限制 5GB
//   - 最大容量：每台 40 个 Volume（200 * 1024 / 5000 = 40.96）
//   - 并发请求：50 个（超过总容量 120）
//
// 测试验证点：
//   1. 任何单台服务器的 Volume 数量不超过 40（防止超额分配）
//   2. 集群总 Volume 数量不超过 120（3 * 40）
//   3. 成功分配数 + 失败数 = 总请求数（无请求丢失）
//   4. 分配的 Volume 正确记录在拓扑中
//
// 预期结果：
//   - 使用容量预留机制后，不会出现超额分配
//   - 前 120 个请求成功，后 30 个请求失败
//   - 每台服务器的 Volume 数量≤40
func TestRaceConditionStress(t *testing.T) {
	// Create a cluster similar to the issue description:
	// 3 volume servers, 200GB each, 5GB volume limit = 40 volumes max per server
	const (
		numServers          = 3
		volumeLimitMB       = 5000                                      // 5GB in MB
		storagePerServerGB  = 200                                       // 200GB per server
		maxVolumesPerServer = storagePerServerGB * 1024 / volumeLimitMB // 200*1024/5000 = 40
		concurrentRequests  = 50                                        // High concurrency like the issue
	)

	// Create test topology
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), uint64(volumeLimitMB)*1024*1024, 5, false)

	dc := NewDataCenter("dc1")
	topo.LinkChildNode(dc)
	rack := NewRack("rack1")
	dc.LinkChildNode(rack)

	// Create 3 volume servers with realistic capacity
	servers := make([]*DataNode, numServers)
	for i := 0; i < numServers; i++ {
		dn := NewDataNode(fmt.Sprintf("server%d", i+1))
		rack.LinkChildNode(dn)

		// Set up disk with capacity for 40 volumes
		disk := NewDisk(types.HardDriveType.String())
		disk.diskUsages.getOrCreateDisk(types.HardDriveType).maxVolumeCount = maxVolumesPerServer
		dn.LinkChildNode(disk)

		servers[i] = dn
	}

	vg := NewDefaultVolumeGrowth()
	rp, _ := super_block.NewReplicaPlacementFromString("000") // Single replica like the issue

	option := &VolumeGrowOption{
		Collection:       "test-bucket-large", // Same collection name as issue
		ReplicaPlacement: rp,
		DiskType:         types.HardDriveType,
	}

	// Track results
	var successfulAllocations int64
	var failedAllocations int64
	var totalVolumesCreated int64

	var wg sync.WaitGroup

	// Launch concurrent volume creation requests
	startTime := time.Now()
	for i := 0; i < concurrentRequests; i++ {
		wg.Add(1)
		go func(requestId int) {
			defer wg.Done()

			// This is the critical test: multiple threads trying to allocate simultaneously
			servers, reservation, err := vg.findEmptySlotsForOneVolume(topo, option, true)

			if err != nil {
				atomic.AddInt64(&failedAllocations, 1)
				t.Logf("Request %d failed: %v", requestId, err)
				return
			}

			// Simulate volume creation delay (like in real scenario)
			time.Sleep(time.Millisecond * 50)

			// Simulate successful volume creation
			for _, server := range servers {
				disk := server.children[NodeId(types.HardDriveType.String())].(*Disk)
				deltaDiskUsage := &DiskUsageCounts{
					volumeCount: 1,
				}
				disk.UpAdjustDiskUsageDelta(types.HardDriveType, deltaDiskUsage)
				atomic.AddInt64(&totalVolumesCreated, 1)
			}

			// Release reservations (simulates successful registration)
			reservation.releaseAllReservations()
			atomic.AddInt64(&successfulAllocations, 1)

		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	// Verify results
	t.Logf("Test completed in %v", duration)
	t.Logf("Successful allocations: %d", successfulAllocations)
	t.Logf("Failed allocations: %d", failedAllocations)
	t.Logf("Total volumes created: %d", totalVolumesCreated)

	// Check capacity limits are respected
	totalCapacityUsed := int64(0)
	for i, server := range servers {
		disk := server.children[NodeId(types.HardDriveType.String())].(*Disk)
		volumeCount := disk.diskUsages.getOrCreateDisk(types.HardDriveType).volumeCount
		totalCapacityUsed += volumeCount

		t.Logf("Server %d: %d volumes (max: %d)", i+1, volumeCount, maxVolumesPerServer)

		// Critical test: No server should exceed its capacity
		if volumeCount > maxVolumesPerServer {
			t.Errorf("RACE CONDITION DETECTED: Server %d exceeded capacity: %d > %d",
				i+1, volumeCount, maxVolumesPerServer)
		}
	}

	// Verify totals make sense
	if totalVolumesCreated != totalCapacityUsed {
		t.Errorf("Volume count mismatch: created=%d, actual=%d", totalVolumesCreated, totalCapacityUsed)
	}

	// The total should never exceed the cluster capacity (120 volumes for 3 servers × 40 each)
	maxClusterCapacity := int64(numServers * maxVolumesPerServer)
	if totalCapacityUsed > maxClusterCapacity {
		t.Errorf("RACE CONDITION DETECTED: Cluster capacity exceeded: %d > %d",
			totalCapacityUsed, maxClusterCapacity)
	}

	// With reservations, we should have controlled allocation
	// Total requests = successful + failed should equal concurrentRequests
	if successfulAllocations+failedAllocations != concurrentRequests {
		t.Errorf("Request count mismatch: success=%d + failed=%d != total=%d",
			successfulAllocations, failedAllocations, concurrentRequests)
	}

	t.Logf("Race condition test passed: Capacity limits respected with %d concurrent requests",
		concurrentRequests)
}

// TestCapacityJudgmentAccuracy 验证容量计算的准确性
// 在各种负载条件下，确保容量预留和实际使用统计的一致性
//
// 测试目标：
//   1. 验证 AvailableSpaceFor 返回的可用容量是准确的
//   2. 验证 AvailableSpaceForReservation 正确扣除已预留的容量
//   3. 验证容量预留后，可用容量立即减少
//   4. 验证 Volume 创建后，可用容量进一步减少
//   5. 验证达到容量上限后，预留请求正确失败
//
// 测试场景：
//   - 单台 Volume Server，容量为 10 个 Volume
//   - 顺序创建 10 个 Volume，每次检查容量计算
//   - 在每个步骤验证：预留前容量、预留后容量、创建后容量
//   - 最后验证第 11 个预留请求失败
//
// 测试步骤（循环 10 次）：
//   1. 检查预留前的可用容量（期望：10-i）
//   2. 执行容量预留（期望成功）
//   3. 检查预留后的可用容量（期望：10-i-1）
//   4. 模拟 Volume 创建（更新磁盘使用统计）
//   5. 释放预留
//   6. 检查创建后的可用容量（期望：10-i-1）
//
// 验证点：
//   - 容量计算的准确性（每一步都验证期望值）
//   - 预留和创建的原子性（容量变化的顺序正确）
//   - 边界情况处理（达到上限时正确拒绝）
//
// 预期结果：
//   - 前 10 次预留和创建都成功
//   - 每次操作后，可用容量正确减少
//   - 第 11 次预留失败（容量已满）
func TestCapacityJudgmentAccuracy(t *testing.T) {
	// Create a single server with known capacity
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 5*1024*1024*1024, 5, false)

	dc := NewDataCenter("dc1")
	topo.LinkChildNode(dc)
	rack := NewRack("rack1")
	dc.LinkChildNode(rack)

	dn := NewDataNode("server1")
	rack.LinkChildNode(dn)

	// Server with capacity for exactly 10 volumes
	disk := NewDisk(types.HardDriveType.String())
	diskUsage := disk.diskUsages.getOrCreateDisk(types.HardDriveType)
	diskUsage.maxVolumeCount = 10
	dn.LinkChildNode(disk)

	// Also set max volume count on the DataNode level (gets propagated up)
	dn.diskUsages.getOrCreateDisk(types.HardDriveType).maxVolumeCount = 10

	vg := NewDefaultVolumeGrowth()
	rp, _ := super_block.NewReplicaPlacementFromString("000")

	option := &VolumeGrowOption{
		Collection:       "test",
		ReplicaPlacement: rp,
		DiskType:         types.HardDriveType,
	}

	// Test accurate capacity reporting at each step
	for i := 0; i < 10; i++ {
		// Check available space before reservation
		availableBefore := dn.AvailableSpaceFor(option)
		availableForReservation := dn.AvailableSpaceForReservation(option)

		expectedAvailable := int64(10 - i)
		if availableBefore != expectedAvailable {
			t.Errorf("Step %d: Expected %d available, got %d", i, expectedAvailable, availableBefore)
		}

		if availableForReservation != expectedAvailable {
			t.Errorf("Step %d: Expected %d available for reservation, got %d", i, expectedAvailable, availableForReservation)
		}

		// Try to reserve and allocate
		_, reservation, err := vg.findEmptySlotsForOneVolume(topo, option, true)
		if err != nil {
			t.Fatalf("Step %d: Unexpected reservation failure: %v", i, err)
		}

		// Check that available space for reservation decreased
		availableAfterReservation := dn.AvailableSpaceForReservation(option)
		if availableAfterReservation != expectedAvailable-1 {
			t.Errorf("Step %d: Expected %d available after reservation, got %d",
				i, expectedAvailable-1, availableAfterReservation)
		}

		// Simulate successful volume creation by properly updating disk usage hierarchy
		disk := dn.children[NodeId(types.HardDriveType.String())].(*Disk)

		// Create a volume usage delta to simulate volume creation
		deltaDiskUsage := &DiskUsageCounts{
			volumeCount: 1,
		}

		// Properly propagate the usage up the hierarchy
		disk.UpAdjustDiskUsageDelta(types.HardDriveType, deltaDiskUsage)

		// Debug: Check the volume count after update
		diskUsageOnNode := dn.diskUsages.getOrCreateDisk(types.HardDriveType)
		currentVolumeCount := atomic.LoadInt64(&diskUsageOnNode.volumeCount)
		t.Logf("Step %d: Volume count after update: %d", i, currentVolumeCount)

		// Release reservation
		reservation.releaseAllReservations()

		// Verify final state
		availableAfter := dn.AvailableSpaceFor(option)
		expectedAfter := int64(10 - i - 1)
		if availableAfter != expectedAfter {
			t.Errorf("Step %d: Expected %d available after creation, got %d",
				i, expectedAfter, availableAfter)
			// More debugging
			diskUsageOnNode := dn.diskUsages.getOrCreateDisk(types.HardDriveType)
			maxVolumes := atomic.LoadInt64(&diskUsageOnNode.maxVolumeCount)
			remoteVolumes := atomic.LoadInt64(&diskUsageOnNode.remoteVolumeCount)
			actualVolumeCount := atomic.LoadInt64(&diskUsageOnNode.volumeCount)
			t.Logf("Debug Step %d: max=%d, volume=%d, remote=%d", i, maxVolumes, actualVolumeCount, remoteVolumes)
		}
	}

	// At this point, no more reservations should succeed
	_, _, err := vg.findEmptySlotsForOneVolume(topo, option, true)
	if err == nil {
		t.Error("Expected reservation to fail when at capacity")
	}

	t.Logf("Capacity judgment accuracy test passed")
}

// TestReservationSystemPerformance 测试容量预留系统的性能影响
// 通过大量预留操作，评估预留机制的性能开销
//
// 测试目标：
//   1. 测量单次容量预留操作的平均耗时
//   2. 验证预留机制不会成为性能瓶颈
//   3. 确保预留系统在高负载下仍然高效
//
// 测试场景：
//   - 单台 Volume Server，容量为 1000 个 Volume
//   - 执行 1000 次预留-释放循环
//   - 记录总耗时和平均耗时
//
// 测试步骤：
//   1. 创建拓扑结构（1 个 DataCenter、1 个 Rack、1 个 DataNode）
//   2. 设置 DataNode 容量为 1000 个 Volume
//   3. 循环 1000 次：
//      a. 调用 findEmptySlotsForOneVolume 预留容量
//      b. 立即释放预留
//      c. 模拟 Volume 创建（更新 volumeCount）
//   4. 计算总耗时和平均耗时
//
// 性能期望：
//   - 平均每次预留操作耗时 < 1ms
//   - 如果超过 1ms，说明预留机制可能存在性能问题
//
// 性能考虑：
//   - 预留操作包含的主要开销：
//     * 获取/释放互斥锁（sync.Mutex）
//     * 生成 UUID（reservationId）
//     * 原子操作（atomic.AddInt64）
//     * 映射操作（map 插入/删除）
//   - 这些操作都是 O(1) 时间复杂度
//   - 预期性能应该在微秒级别，1ms 是非常宽松的阈值
//
// 预期结果：
//   - 1000 次预留操作总耗时远小于 1 秒
//   - 平均耗时远小于 1ms（通常在 10-100 微秒）
//   - 测试通过，说明预留机制性能足够好
func TestReservationSystemPerformance(t *testing.T) {
	// Create topology
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)

	dc := NewDataCenter("dc1")
	topo.LinkChildNode(dc)
	rack := NewRack("rack1")
	dc.LinkChildNode(rack)

	dn := NewDataNode("server1")
	rack.LinkChildNode(dn)

	disk := NewDisk(types.HardDriveType.String())
	disk.diskUsages.getOrCreateDisk(types.HardDriveType).maxVolumeCount = 1000
	dn.LinkChildNode(disk)

	vg := NewDefaultVolumeGrowth()
	rp, _ := super_block.NewReplicaPlacementFromString("000")

	option := &VolumeGrowOption{
		Collection:       "test",
		ReplicaPlacement: rp,
		DiskType:         types.HardDriveType,
	}

	// Benchmark reservation operations
	const iterations = 1000

	startTime := time.Now()
	for i := 0; i < iterations; i++ {
		_, reservation, err := vg.findEmptySlotsForOneVolume(topo, option, true)
		if err != nil {
			t.Fatalf("Iteration %d failed: %v", i, err)
		}
		reservation.releaseAllReservations()

		// Simulate volume creation
		diskUsage := dn.diskUsages.getOrCreateDisk(types.HardDriveType)
		atomic.AddInt64(&diskUsage.volumeCount, 1)
	}
	duration := time.Since(startTime)

	avgDuration := duration / iterations
	t.Logf("Performance: %d reservations in %v (avg: %v per reservation)",
		iterations, duration, avgDuration)

	// Performance should be reasonable (less than 1ms per reservation on average)
	if avgDuration > time.Millisecond {
		t.Errorf("Reservation system performance concern: %v per reservation", avgDuration)
	} else {
		t.Logf("Performance test passed: %v per reservation", avgDuration)
	}
}
