// Package topology 的测试文件
// 本文件测试 VolumeLayout 的核心功能：卷的二进制状态管理
package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

// TestVolumesBinaryState 测试卷的二进制状态跟踪机制
// 测试目标：volumesBinaryState 的 Add/Remove 和 IsTrue 方法
//
// 核心概念 - 二进制状态：
//   volumesBinaryState 用于跟踪卷的某种二进制状态（只读/过大等）
//   通过 stateIndicator 函数判断卷是否应该标记为该状态
//
// 两种状态指示器：
//   1. ExistCopies(): 当有副本存在时返回 true（用于只读、过大状态）
//   2. NoCopies(): 当没有副本时返回 true（用于反向逻辑）
//
// 测试场景：
//   1. ExistCopies 模式：至少有一个副本时标记为 true
//   2. NoCopies 模式：所有副本都被移除时标记为 true
func TestVolumesBinaryState(t *testing.T) {
	// 【测试数据准备】
	// 准备 5 个测试 Volume ID
	vids := []needle.VolumeId{
		needle.VolumeId(1),
		needle.VolumeId(2),
		needle.VolumeId(3),
		needle.VolumeId(4),
		needle.VolumeId(5),
	}

	// 准备 3 个测试 DataNode
	// 模拟 3 台服务器，IP 相同但端口不同
	dns := []*DataNode{
		&DataNode{
			Ip:   "127.0.0.1",
			Port: 8081,
		},
		&DataNode{
			Ip:   "127.0.0.1",
			Port: 8082,
		},
		&DataNode{
			Ip:   "127.0.0.1",
			Port: 8083,
		},
	}

	// 副本策略：002 表示同数据中心不同机架 2 个副本
	// 即需要 3 个副本：1 个主副本 + 2 个同数据中心不同机架副本
	rp, _ := super_block.NewReplicaPlacementFromString("002")

	// 【场景 1：ExistCopies 模式】
	// 创建一个"只读"状态的跟踪器，当有副本存在时标记为 true
	// 这是最常见的模式，用于跟踪只读卷、过大卷等
	state_exist := NewVolumesBinaryState(readOnlyState, rp, ExistCopies())

	// 为卷添加副本位置：
	// vid=1: 有 2 个副本（dns[0], dns[1]）→ IsTrue 应该返回 true
	state_exist.Add(vids[0], dns[0])
	state_exist.Add(vids[0], dns[1])

	// vid=2: 有 1 个副本（dns[2]）→ IsTrue 应该返回 true
	state_exist.Add(vids[1], dns[2])

	// vid=3: 有 1 个副本（dns[1]）→ IsTrue 应该返回 true
	state_exist.Add(vids[2], dns[1])

	// vid=4: 没有副本（未添加）→ IsTrue 应该返回 false

	// vid=5: 有 2 个副本（dns[1], dns[2]）→ IsTrue 应该返回 true
	state_exist.Add(vids[4], dns[1])
	state_exist.Add(vids[4], dns[2])

	// 【场景 2：NoCopies 模式】
	// 创建一个"只读"状态的跟踪器，当没有副本时标记为 true
	// 这是反向逻辑，用于跟踪"应该被清理的卷"等场景
	state_no := NewVolumesBinaryState(readOnlyState, rp, NoCopies())

	// 为卷添加副本位置：
	// vid=1: 有 2 个副本 → IsTrue 应该返回 false（有副本）
	state_no.Add(vids[0], dns[0])
	state_no.Add(vids[0], dns[1])

	// vid=4: 有 1 个副本 → IsTrue 应该返回 false（有副本）
	state_no.Add(vids[3], dns[1])

	// 【测试用例定义】
	// 使用表驱动测试模式，测试两种状态指示器
	tests := []struct {
		name                    string             // 测试用例名称
		state                   *volumesBinaryState // 要测试的状态跟踪器
		expectResult            []bool             // 初始状态的预期结果（5 个卷的状态）
		update                  func()             // 更新操作（添加/删除副本）
		expectResultAfterUpdate []bool             // 更新后的预期结果
	}{
		{
			// 【测试用例 1：ExistCopies 模式】
			// 验证"有副本就标记为 true"的逻辑
			name:  "mark true when copies exist",
			state: state_exist,
			// 初始状态：
			//   vid=1: true（有 2 个副本）
			//   vid=2: true（有 1 个副本）
			//   vid=3: true（有 1 个副本）
			//   vid=4: false（无副本）
			//   vid=5: true（有 2 个副本）
			expectResult: []bool{true, true, true, false, true},
			update: func() {
				// 执行移除操作：
				// - 尝试移除不存在的副本（dns[2]）→ 无影响
				state_exist.Remove(vids[0], dns[2])
				// - 移除 vid=2 的唯一副本 → 状态变为 false
				state_exist.Remove(vids[1], dns[2])
				// - 尝试移除不存在的副本 → 无影响
				state_exist.Remove(vids[3], dns[2])
				// - 移除 vid=5 的所有副本 → 状态变为 false
				state_exist.Remove(vids[4], dns[1])
				state_exist.Remove(vids[4], dns[2])
			},
			// 更新后状态：
			//   vid=1: true（仍有 2 个副本）
			//   vid=2: false（副本被移除）
			//   vid=3: true（仍有 1 个副本）
			//   vid=4: false（仍无副本）
			//   vid=5: false（所有副本被移除）
			expectResultAfterUpdate: []bool{true, false, true, false, false},
		},
		{
			// 【测试用例 2：NoCopies 模式】
			// 验证"没有副本就标记为 true"的反向逻辑
			name:  "mark true when no copies exist",
			state: state_no,
			// 初始状态：
			//   vid=1: false（有 2 个副本）
			//   vid=2: true（无副本）
			//   vid=3: true（无副本）
			//   vid=4: false（有 1 个副本）
			//   vid=5: true（无副本）
			expectResult: []bool{false, true, true, false, true},
			update: func() {
				// 执行混合操作：
				// - 尝试移除不存在的副本 → 无影响
				state_no.Remove(vids[0], dns[2])
				state_no.Remove(vids[1], dns[2])
				// - 为 vid=3 添加副本 → 状态变为 false
				state_no.Add(vids[2], dns[1])
				// - 移除 vid=4 的唯一副本 → 状态变为 true
				state_no.Remove(vids[3], dns[1])
				// - 尝试移除不存在的副本 → 无影响
				state_no.Remove(vids[4], dns[2])
			},
			// 更新后状态：
			//   vid=1: false（仍有 2 个副本）
			//   vid=2: true（仍无副本）
			//   vid=3: false（添加了副本）
			//   vid=4: true（副本被移除）
			//   vid=5: true（仍无副本）
			expectResultAfterUpdate: []bool{false, true, false, true, true},
		},
	}
	// 【执行测试】
	// 遍历所有测试用例
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 【阶段 1：验证初始状态】
			// 查询所有卷的状态，构建结果数组
			var result []bool
			for index, _ := range vids {
				result = append(result, test.state.IsTrue(vids[index]))
			}

			// 验证结果数量是否正确
			if len(result) != len(test.expectResult) {
				t.Fatalf("len(result) != len(expectResult), got %d, expected %d\n",
					len(result), len(test.expectResult))
			}

			// 逐个验证每个卷的状态是否符合预期
			for index, val := range result {
				if val != test.expectResult[index] {
					t.Fatalf("result not matched, index %d, got %v, expected %v\n",
						index, val, test.expectResult[index])
				}
			}

			// 【阶段 2：执行更新操作】
			// 调用测试用例定义的更新函数（添加/删除副本）
			test.update()

			// 【阶段 3：验证更新后的状态】
			// 再次查询所有卷的状态
			var updateResult []bool
			for index, _ := range vids {
				updateResult = append(updateResult, test.state.IsTrue(vids[index]))
			}

			// 验证结果数量是否正确
			if len(updateResult) != len(test.expectResultAfterUpdate) {
				t.Fatalf("len(updateResult) != len(expectResultAfterUpdate), got %d, expected %d\n",
					len(updateResult), len(test.expectResultAfterUpdate))
			}

			// 逐个验证每个卷的更新后状态是否符合预期
			for index, val := range updateResult {
				if val != test.expectResultAfterUpdate[index] {
					t.Fatalf("update result not matched, index %d, got %v, expected %v\n",
						index, val, test.expectResultAfterUpdate[index])
				}
			}
		})
	}
}

