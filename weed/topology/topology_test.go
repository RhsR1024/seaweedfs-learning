// Package topology 实现了 SeaweedFS 的拓扑结构管理
// 本文件包含拓扑功能的单元测试
package topology

import (
	"reflect"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"

	"testing"
)

// TestRemoveDataCenter 测试删除数据中心功能
// 验证删除数据中心后，拓扑的活跃 Volume 计数是否正确更新
//
// 测试流程:
//   1. 创建包含多个数据中心的拓扑结构
//   2. 删除 dc2，验证活跃 Volume 数从初始值减少到 15
//   3. 删除 dc3，验证活跃 Volume 数进一步减少到 12
//
// 验证点:
//   - UnlinkChildNode 正确移除数据中心
//   - diskUsages 正确更新活跃 Volume 计数
func TestRemoveDataCenter(t *testing.T) {
	topo := setup(topologyLayout)
	// 删除 dc2，验证活跃 Volume 数
	topo.UnlinkChildNode(NodeId("dc2"))
	if topo.diskUsages.usages[types.HardDriveType].activeVolumeCount != 15 {
		t.Fail()
	}
	// 删除 dc3，验证活跃 Volume 数
	topo.UnlinkChildNode(NodeId("dc3"))
	if topo.diskUsages.usages[types.HardDriveType].activeVolumeCount != 12 {
		t.Fail()
	}
}

// TestHandlingVolumeServerHeartbeat 测试 Volume server 心跳处理
// 验证拓扑正确处理 Volume server 的注册、Volume 同步和注销
//
// 测试场景:
//   1. Volume server 注册并上报 Volume 列表
//   2. Volume server 上报 Volume 变更（删除、修改）
//   3. Volume server 增量同步 Volume（新增、删除）
//   4. Volume server 注销
//
// 验证点:
//   - SyncDataNodeRegistration 正确处理全量 Volume 同步
//   - IncrementalSyncDataNodeRegistration 正确处理增量同步
//   - 可写 Volume 列表正确更新
//   - 磁盘类型（HDD、SSD）正确统计
//   - UnRegisterDataNode 正确清理节点信息
func TestHandlingVolumeServerHeartbeat(t *testing.T) {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)

	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	// 设置不同磁盘类型的最大 Volume 数
	maxVolumeCounts := make(map[string]uint32)
	maxVolumeCounts[""] = 25    // HDD 最多 25 个 Volume
	maxVolumeCounts["ssd"] = 12 // SSD 最多 12 个 Volume
	dn := rack.GetOrCreateDataNode("127.0.0.1", 34534, 0, "127.0.0.1", maxVolumeCounts)

	// 【测试场景 1：首次全量同步 Volume】
	{
		volumeCount := 7
		var volumeMessages []*master_pb.VolumeInformationMessage

		// 创建 7 个 HDD Volume (ID 1-7)
		for k := 1; k <= volumeCount; k++ {
			volumeMessage := &master_pb.VolumeInformationMessage{
				Id:               uint32(k),
				Size:             uint64(25432),
				Collection:       "",
				FileCount:        uint64(2343),
				DeleteCount:      uint64(345),
				DeletedByteCount: 34524,
				ReadOnly:         false,
				ReplicaPlacement: uint32(0),
				Version:          uint32(needle.GetCurrentVersion()),
				Ttl:              0,
			}
			volumeMessages = append(volumeMessages, volumeMessage)
		}

		// 创建 7 个 SSD Volume (ID 8-14)
		for k := 1; k <= volumeCount; k++ {
			volumeMessage := &master_pb.VolumeInformationMessage{
				Id:               uint32(volumeCount + k),
				Size:             uint64(25432),
				Collection:       "",
				FileCount:        uint64(2343),
				DeleteCount:      uint64(345),
				DeletedByteCount: 34524,
				ReadOnly:         false,
				ReplicaPlacement: uint32(0),
				Version:          uint32(needle.GetCurrentVersion()),
				Ttl:              0,
				DiskType:         "ssd", // SSD 磁盘类型
			}
			volumeMessages = append(volumeMessages, volumeMessage)
		}

		// 全量同步 Volume 列表
		topo.SyncDataNodeRegistration(volumeMessages, dn)

		usageCounts := topo.diskUsages.usages[types.HardDriveType]

		// 验证 HDD Volume 计数
		assert(t, "activeVolumeCount1", int(usageCounts.activeVolumeCount), volumeCount)
		assert(t, "volumeCount", int(usageCounts.volumeCount), volumeCount)
		// 验证 SSD Volume 计数
		assert(t, "ssdVolumeCount", int(topo.diskUsages.usages[types.SsdType].volumeCount), volumeCount)
	}

	// 【测试场景 2：Volume 变更同步（删除一个 Volume）】
	{
		volumeCount := 7 - 1 // 现在只有 6 个 Volume
		var volumeMessages []*master_pb.VolumeInformationMessage

		// 创建 6 个 HDD Volume (ID 1-6)，Volume 7 被删除了
		for k := 1; k <= volumeCount; k++ {
			volumeMessage := &master_pb.VolumeInformationMessage{
				Id:               uint32(k),
				Size:             uint64(254320), // 大小有变化
				Collection:       "",
				FileCount:        uint64(2343),
				DeleteCount:      uint64(345),
				DeletedByteCount: 345240,
				ReadOnly:         false,
				ReplicaPlacement: uint32(0),
				Version:          uint32(needle.GetCurrentVersion()),
				Ttl:              0,
			}
			volumeMessages = append(volumeMessages, volumeMessage)
		}

		// 再次全量同步，Master 会检测到 Volume 7 被删除
		topo.SyncDataNodeRegistration(volumeMessages, dn)

		usageCounts := topo.diskUsages.usages[types.HardDriveType]

		// 验证 Volume 数减少到 6
		assert(t, "activeVolumeCount1", int(usageCounts.activeVolumeCount), volumeCount)
		assert(t, "volumeCount", int(usageCounts.volumeCount), volumeCount)
	}

	// 【测试场景 3：增量同步 Volume（重复添加、删除、再添加）】
	{
		volumeCount := 6
		// Volume 3 的简短信息（用于增量同步）
		newVolumeShortMessage := &master_pb.VolumeShortInformationMessage{
			Id:               uint32(3),
			Collection:       "",
			ReplicaPlacement: uint32(0),
			Version:          uint32(needle.GetCurrentVersion()),
			Ttl:              0,
		}

		// 增量同步：重复添加 Volume 3（已存在）
		topo.IncrementalSyncDataNodeRegistration(
			[]*master_pb.VolumeShortInformationMessage{newVolumeShortMessage},
			nil,
			dn)
		rp, _ := super_block.NewReplicaPlacementFromString("000")
		layout := topo.GetVolumeLayout("", rp, needle.EMPTY_TTL, types.HardDriveType)
		// 验证重复添加不会增加 Volume 数
		assert(t, "writables after repeated add", len(layout.writables), volumeCount)

		usageCounts := topo.diskUsages.usages[types.HardDriveType]

		assert(t, "activeVolumeCount1", int(usageCounts.activeVolumeCount), volumeCount)
		assert(t, "volumeCount", int(usageCounts.volumeCount), volumeCount)

		// 增量同步：删除 Volume 3
		topo.IncrementalSyncDataNodeRegistration(
			nil,
			[]*master_pb.VolumeShortInformationMessage{newVolumeShortMessage},
			dn)
		// 验证 Volume 数减少到 5
		assert(t, "writables after deletion", len(layout.writables), volumeCount-1)
		assert(t, "activeVolumeCount1", int(usageCounts.activeVolumeCount), volumeCount-1)
		assert(t, "volumeCount", int(usageCounts.volumeCount), volumeCount-1)

		// 增量同步：重新添加 Volume 3
		topo.IncrementalSyncDataNodeRegistration(
			[]*master_pb.VolumeShortInformationMessage{newVolumeShortMessage},
			nil,
			dn)

		// 打印调试信息
		for vid := range layout.vid2location {
			println("after add volume id", vid)
		}
		for _, vid := range layout.writables {
			println("after add writable volume id", vid)
		}

		// 验证 Volume 数恢复到 6
		assert(t, "writables after add back", len(layout.writables), volumeCount)

	}

	// 【测试场景 4：DataNode 注销】
	topo.UnRegisterDataNode(dn)

	usageCounts := topo.diskUsages.usages[types.HardDriveType]

	// 验证所有 Volume 被清除
	assert(t, "activeVolumeCount2", int(usageCounts.activeVolumeCount), 0)

}

// assert 辅助函数，验证实际值是否等于期望值
// 如果不相等，测试失败并打印错误信息
func assert(t *testing.T, message string, actual, expected int) {
	if actual != expected {
		t.Fatalf("unexpected %s: %d, expected: %d", message, actual, expected)
	}
}

// TestAddRemoveVolume 测试 Volume 的注册和注销功能
// 验证 Volume 注册后 Collection 被创建，注销后 Collection 被删除
//
// 测试流程:
//   1. 创建拓扑结构和 DataNode
//   2. 创建一个 SSD Volume 并注册到拓扑
//   3. 重复注册同一个 Volume（测试幂等性）
//   4. 验证 Collection 已创建
//   5. 注销 Volume
//   6. 验证 Collection 已删除（因为没有其他 Volume 了）
//
// 验证点:
//   - RegisterVolumeLayout 正确创建 Collection
//   - 重复注册不会产生副作用
//   - UnRegisterVolumeLayout 正确清理 Collection
func TestAddRemoveVolume(t *testing.T) {

	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)

	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	maxVolumeCounts := make(map[string]uint32)
	maxVolumeCounts[""] = 25
	maxVolumeCounts["ssd"] = 12
	dn := rack.GetOrCreateDataNode("127.0.0.1", 34534, 0, "127.0.0.1", maxVolumeCounts)

	// 创建一个 SSD Volume
	v := storage.VolumeInfo{
		Id:               needle.VolumeId(1),
		Size:             100,
		Collection:       "xcollection", // Collection 名称
		DiskType:         "ssd",
		FileCount:        123,
		DeleteCount:      23,
		DeletedByteCount: 45,
		ReadOnly:         false,
		Version:          needle.GetCurrentVersion(),
		ReplicaPlacement: &super_block.ReplicaPlacement{},
		Ttl:              needle.EMPTY_TTL,
	}

	// 更新 DataNode 的 Volume 列表
	dn.UpdateVolumes([]storage.VolumeInfo{v})
	// 注册 Volume 到拓扑
	topo.RegisterVolumeLayout(v, dn)
	// 重复注册（测试幂等性）
	topo.RegisterVolumeLayout(v, dn)

	// 验证 Collection 已创建
	if _, hasCollection := topo.FindCollection(v.Collection); !hasCollection {
		t.Errorf("collection %v should exist", v.Collection)
	}

	// 注销 Volume
	topo.UnRegisterVolumeLayout(v, dn)

	// 验证 Collection 已删除（因为没有 Volume 了）
	if _, hasCollection := topo.FindCollection(v.Collection); hasCollection {
		t.Errorf("collection %v should not exist", v.Collection)
	}
}

// TestListCollections 测试 Collection 列表功能
// 验证能够正确列出普通 Volume 和 EC Volume 的 Collection
//
// 测试数据:
//   - 3 个普通 Volume：空 Collection、vol_collection_a、vol_collection_b
//   - 2 个 EC Volume：ec_collection_a、ec_collection_b
//
// 测试用例:
//   1. 不包含任何类型 -> 返回空列表
//   2. 只包含普通 Volume -> 返回 3 个 Collection
//   3. 只包含 EC Volume -> 返回 2 个 Collection
//   4. 包含普通 + EC Volume -> 返回 5 个 Collection
//
// 验证点:
//   - ListCollections 正确过滤 Volume 类型
//   - 空 Collection 名称（""）正确处理
//   - 返回的 Collection 列表已排序
func TestListCollections(t *testing.T) {
	rp, _ := super_block.NewReplicaPlacementFromString("002")

	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	dn := rack.GetOrCreateDataNode("127.0.0.1", 34534, 0, "127.0.0.1", nil)

	// 注册 3 个普通 Volume
	topo.RegisterVolumeLayout(storage.VolumeInfo{
		Id:               needle.VolumeId(1111),
		ReplicaPlacement: rp,
	}, dn)
	topo.RegisterVolumeLayout(storage.VolumeInfo{
		Id:               needle.VolumeId(2222),
		ReplicaPlacement: rp,
		Collection:       "vol_collection_a",
	}, dn)
	topo.RegisterVolumeLayout(storage.VolumeInfo{
		Id:               needle.VolumeId(3333),
		ReplicaPlacement: rp,
		Collection:       "vol_collection_b",
	}, dn)

	// 注册 2 个 EC Volume
	topo.RegisterEcShards(&erasure_coding.EcVolumeInfo{
		VolumeId:   needle.VolumeId(4444),
		Collection: "ec_collection_a",
	}, dn)
	topo.RegisterEcShards(&erasure_coding.EcVolumeInfo{
		VolumeId:   needle.VolumeId(5555),
		Collection: "ec_collection_b",
	}, dn)

	// 定义测试用例
	testCases := []struct {
		name                 string
		includeNormalVolumes bool
		includeEcVolumes     bool
		want                 []string
	}{
		{
			name:                 "no volume types selected",
			includeNormalVolumes: false,
			includeEcVolumes:     false,
			want:                 nil,
		}, {
			name:                 "normal volumes",
			includeNormalVolumes: true,
			includeEcVolumes:     false,
			want:                 []string{"", "vol_collection_a", "vol_collection_b"},
		}, {
			name:                 "EC volumes",
			includeNormalVolumes: false,
			includeEcVolumes:     true,
			want:                 []string{"ec_collection_a", "ec_collection_b"},
		}, {
			name:                 "normal + EC volumes",
			includeNormalVolumes: true,
			includeEcVolumes:     true,
			want:                 []string{"", "ec_collection_a", "ec_collection_b", "vol_collection_a", "vol_collection_b"},
		},
	}

	// 运行所有测试用例
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := topo.ListCollections(tc.includeNormalVolumes, tc.includeEcVolumes)

			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}
