// Package topology 实现了 SeaweedFS 的拓扑管理功能
// 本文件是容量预留机制的测试文件，验证并发安全性和正确性
package topology

import (
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// TestCapacityReservations_BasicOperations 测试容量预留的基本操作
// 测试场景：
//   - 初始状态验证
//   - 添加预留
//   - 多次预留累加
//   - 删除预留
//   - 删除不存在的预留
func TestCapacityReservations_BasicOperations(t *testing.T) {
	// 创建新的容量预留管理器
	cr := newCapacityReservations()
	diskType := types.HardDriveType

	// 【测试 1：初始状态】
	// 新创建的预留管理器应该没有任何预留
	if count := cr.getReservedCount(diskType); count != 0 {
		t.Errorf("Expected 0 reserved count initially, got %d", count)
	}

	// 【测试 2：添加预留】
	// 添加 5 个卷的预留，应该返回非空的预留 ID
	reservationId := cr.addReservation(diskType, 5)
	if reservationId == "" {
		t.Error("Expected non-empty reservation ID")
	}

	// 验证预留计数器更新正确
	if count := cr.getReservedCount(diskType); count != 5 {
		t.Errorf("Expected 5 reserved count, got %d", count)
	}

	// 【测试 3：多次预留累加】
	// 再添加 3 个卷的预留，总预留数应该是 5+3=8
	cr.addReservation(diskType, 3)
	if count := cr.getReservedCount(diskType); count != 8 {
		t.Errorf("Expected 8 reserved count after second reservation, got %d", count)
	}

	// 【测试 4：删除预留】
	// 删除第一个预留（5 个卷），剩余应该是 3 个卷
	success := cr.removeReservation(reservationId)
	if !success {
		t.Error("Expected successful removal of existing reservation")
	}

	// 验证预留计数器减少正确
	if count := cr.getReservedCount(diskType); count != 3 {
		t.Errorf("Expected 3 reserved count after removal, got %d", count)
	}

	// 【测试 5：删除不存在的预留】
	// 删除不存在的预留 ID 应该返回失败
	success = cr.removeReservation("non-existent-id")
	if success {
		t.Error("Expected failure when removing non-existent reservation")
	}
}

// TestCapacityReservations_ExpiredCleaning 测试过期预留的自动清理机制
// 测试场景：
//   - 创建多个预留
//   - 手动设置部分预留为"过期"
//   - 触发清理操作
//   - 验证只保留未过期的预留
func TestCapacityReservations_ExpiredCleaning(t *testing.T) {
	cr := newCapacityReservations()
	diskType := types.HardDriveType

	// 【准备：创建两个预留】
	// 预留 1: 3 个卷
	reservationId1 := cr.addReservation(diskType, 3)
	// 预留 2: 2 个卷
	reservationId2 := cr.addReservation(diskType, 2)

	// 【模拟：将预留 1 设置为"过期"】
	// 手动修改创建时间为 10 分钟前
	cr.Lock()
	if reservation, exists := cr.reservations[reservationId1]; exists {
		reservation.createdAt = time.Now().Add(-10 * time.Minute) // 10 分钟前创建
	}
	cr.Unlock()

	// 【执行：清理 5 分钟前的过期预留】
	// 预留 1（10 分钟前）会被清理
	// 预留 2（刚刚创建）会保留
	cr.cleanExpiredReservations(5 * time.Minute)

	// 【验证 1：只保留未过期的预留】
	// 应该只剩下预留 2 的 2 个卷
	if count := cr.getReservedCount(diskType); count != 2 {
		t.Errorf("Expected 2 reserved count after cleaning, got %d", count)
	}

	// 【验证 2：未过期的预留仍然存在】
	// 预留 2 应该可以正常删除
	if !cr.removeReservation(reservationId2) {
		t.Error("Expected recent reservation to still exist")
	}

	// 【验证 3：过期的预留已被清理】
	// 预留 1 应该已经不存在，删除应该失败
	if cr.removeReservation(reservationId1) {
		t.Error("Expected old reservation to be cleaned up")
	}
}

// TestCapacityReservations_DifferentDiskTypes 测试不同磁盘类型的预留隔离
// 测试场景：
//   - 为不同磁盘类型创建预留
//   - 验证预留计数器按磁盘类型分别统计
func TestCapacityReservations_DifferentDiskTypes(t *testing.T) {
	cr := newCapacityReservations()

	// 【准备：为不同磁盘类型创建预留】
	// HDD（机械硬盘）预留 5 个卷
	cr.addReservation(types.HardDriveType, 5)
	// SSD（固态硬盘）预留 3 个卷
	cr.addReservation(types.SsdType, 3)

	// 【验证 1：HDD 预留计数器独立】
	// HDD 的预留数应该是 5，不受 SSD 影响
	if count := cr.getReservedCount(types.HardDriveType); count != 5 {
		t.Errorf("Expected 5 HDD reserved count, got %d", count)
	}

	// 【验证 2：SSD 预留计数器独立】
	// SSD 的预留数应该是 3，不受 HDD 影响
	if count := cr.getReservedCount(types.SsdType); count != 3 {
		t.Errorf("Expected 3 SSD reserved count, got %d", count)
	}
}

// TestNodeImpl_ReservationMethods 测试数据节点的容量预留方法
// 测试场景：
//   - 可用空间计算（含/不含预留）
//   - 成功预留容量
//   - 预留后的可用空间更新
//   - 容量不足时预留失败
//   - 释放预留后恢复空间
func TestNodeImpl_ReservationMethods(t *testing.T) {
	// 【准备：创建测试数据节点】
	dn := NewDataNode("test-node")
	diskType := types.HardDriveType

	// 【准备：设置节点容量】
	// 最大容量：10 个卷
	// 已使用：5 个卷
	// 空闲：5 个卷
	diskUsage := dn.diskUsages.getOrCreateDisk(diskType)
	diskUsage.maxVolumeCount = 10
	diskUsage.volumeCount = 5 // 5 volumes free initially

	option := &VolumeGrowOption{DiskType: diskType}

	// 【测试 1：基础可用空间计算】
	// AvailableSpaceFor 不考虑预留，应该返回实际空闲槽位数
	available := dn.AvailableSpaceFor(option)
	if available != 5 {
		t.Errorf("Expected 5 available slots, got %d", available)
	}

	// 【测试 2：预留模式可用空间计算】
	// AvailableSpaceForReservation 考虑预留，初始状态应该与 AvailableSpaceFor 相同
	availableForReservation := dn.AvailableSpaceForReservation(option)
	if availableForReservation != 5 {
		t.Errorf("Expected 5 available slots for reservation, got %d", availableForReservation)
	}

	// 【测试 3：成功预留容量】
	// 尝试预留 3 个卷，应该成功并返回预留 ID
	reservationId, success := dn.TryReserveCapacity(diskType, 3)
	if !success {
		t.Error("Expected successful reservation")
	}
	if reservationId == "" {
		t.Error("Expected non-empty reservation ID")
	}

	// 【测试 4：预留后可用空间减少】
	// 可预留的空间应该从 5 减少到 2（5 - 3 = 2）
	availableForReservation = dn.AvailableSpaceForReservation(option)
	if availableForReservation != 2 {
		t.Errorf("Expected 2 available slots after reservation, got %d", availableForReservation)
	}

	// 【测试 5：基础可用空间不受影响】
	// AvailableSpaceFor 不考虑预留，应该仍然是 5
	available = dn.AvailableSpaceFor(option)
	if available != 5 {
		t.Errorf("Expected base available to remain 5, got %d", available)
	}

	// 【测试 6：容量不足时预留失败】
	// 只剩 2 个可预留槽位，尝试预留 3 个应该失败
	_, success = dn.TryReserveCapacity(diskType, 3)
	if success {
		t.Error("Expected reservation failure due to insufficient capacity")
	}

	// 【测试 7：释放预留恢复空间】
	// 释放之前预留的 3 个卷，可预留空间应该恢复到 5
	dn.ReleaseReservedCapacity(reservationId)
	availableForReservation = dn.AvailableSpaceForReservation(option)
	if availableForReservation != 5 {
		t.Errorf("Expected 5 available slots after release, got %d", availableForReservation)
	}
}

// TestNodeImpl_ConcurrentReservations 测试并发预留的线程安全性
// 测试场景：
//   - 多个 goroutine 同时尝试预留容量
//   - 验证总预留数不超过最大容量（无竞态条件）
//   - 验证超额预留会被正确拒绝
//   - 验证批量释放后空间恢复
func TestNodeImpl_ConcurrentReservations(t *testing.T) {
	dn := NewDataNode("test-node")
	diskType := types.HardDriveType

	// 【准备：设置节点容量】
	// 最大容量：10 个卷
	// 已使用：0 个卷（全部空闲）
	diskUsage := dn.diskUsages.getOrCreateDisk(diskType)
	diskUsage.maxVolumeCount = 10
	diskUsage.volumeCount = 0 // 10 volumes free initially

	// 【测试：并发预留】
	// 启动 10 个 goroutine，每个尝试预留 1 个卷
	var wg sync.WaitGroup
	var reservationIds sync.Map // 线程安全的 map，记录成功的预留 ID
	concurrentRequests := 10
	wg.Add(concurrentRequests)

	for i := 0; i < concurrentRequests; i++ {
		go func(i int) {
			defer wg.Done()
			// 尝试预留 1 个卷
			if reservationId, success := dn.TryReserveCapacity(diskType, 1); success {
				// 成功：记录预留 ID
				reservationIds.Store(reservationId, true)
				t.Logf("goroutine %d: Successfully reserved %s", i, reservationId)
			} else {
				// 失败：在并发测试中，所有预留都应该成功（因为正好 10 个请求 = 10 个槽位）
				t.Errorf("goroutine %d: Expected successful reservation", i)
			}
		}(i)
	}

	// 等待所有 goroutine 完成
	wg.Wait()

	// 【验证 1：所有容量已被预留】
	// 可预留空间应该降到 0
	option := &VolumeGrowOption{DiskType: diskType}
	if available := dn.AvailableSpaceForReservation(option); available != 0 {
		t.Errorf("Expected 0 available slots after all reservations, got %d", available)
		// Debug: check total reserved
		reservedCount := dn.capacityReservations.getReservedCount(diskType)
		t.Logf("Debug: Total reserved count: %d", reservedCount)
	}

	// 【验证 2：超额预留被拒绝】
	// 尝试再预留 1 个卷应该失败（已满）
	_, success := dn.TryReserveCapacity(diskType, 1)
	if success {
		t.Error("Expected reservation failure when at capacity")
	}

	// 【测试：批量释放预留】
	// 释放所有预留
	reservationIds.Range(func(key, value interface{}) bool {
		dn.ReleaseReservedCapacity(key.(string))
		return true
	})

	// 【验证 3：释放后容量恢复】
	// 所有预留释放后，可预留空间应该恢复到 10
	if available := dn.AvailableSpaceForReservation(option); available != 10 {
		t.Errorf("Expected 10 available slots after releasing all, got %d", available)
	}
}
