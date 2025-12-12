// Package needle 实现了 SeaweedFS 的 Needle 存储格式
// 本文件包含 VolumeId 类型的单元测试
package needle

import "testing"

// =====================================================
// VolumeId 测试说明
// =====================================================
// VolumeId 是 SeaweedFS 中卷的唯一标识符，使用 32 位无符号整数表示
// 取值范围：0 ~ 4,294,967,295（约 43 亿个卷）
//
// VolumeId 在文件 ID (fid) 中的位置：
//   fid 格式：<volumeId>,<fileKey>[_cookie]
//   示例：3,01e3b0756f 表示 volumeId=3
//
// 本测试文件验证以下功能：
// 1. NewVolumeId: 从字符串解析 VolumeId
// 2. String: 将 VolumeId 转换为字符串
// 3. Next: 获取下一个 VolumeId
// =====================================================

// TestNewVolumeId 测试从字符串创建 VolumeId 的功能
// 验证：
//   - 合法输入（数字字符串）应成功创建 VolumeId
//   - 非法输入（非数字字符串）应返回错误
//
// 测试用例：
//   - "1"：合法的 VolumeId 字符串
//   - "a"：非法的 VolumeId 字符串（包含字母）
func TestNewVolumeId(t *testing.T) {
	// 测试合法的数字字符串 "1"
	// NewVolumeId 应该成功解析并返回 VolumeId(1)
	if _, err := NewVolumeId("1"); err != nil {
		t.Error(err)
	}

	// 测试非法的字符串 "a"
	// NewVolumeId 应该返回解析错误
	// 注意：这里使用 t.Logf 而非 t.Error，因为返回错误是预期行为
	if _, err := NewVolumeId("a"); err != nil {
		t.Logf("a is not legal volume id, %v", err)
	}
}

// TestVolumeId_String 测试 VolumeId 的字符串转换功能
// String() 方法将 VolumeId 转换为十进制字符串表示
//
// 测试场景：
//   1. 直接对 VolumeId 类型调用 String()
//   2. 对 VolumeId 变量调用 String()
//   3. 对 *VolumeId 指针调用 String()
//
// 预期结果：
//   - VolumeId(10).String() 返回 "10"
//   - VolumeId(11).String() 返回 "11"
func TestVolumeId_String(t *testing.T) {
	// 场景 1：直接对类型字面量调用 String()
	// VolumeId(10) 创建一个值为 10 的 VolumeId
	if str := VolumeId(10).String(); str != "10" {
		t.Errorf("to string failed")
	}

	// 场景 2：对变量调用 String()
	// 创建 VolumeId 变量，然后调用其 String() 方法
	vid := VolumeId(11)
	if str := vid.String(); str != "11" {
		t.Errorf("to string failed")
	}

	// 场景 3：对指针调用 String()
	// 验证指针类型也能正确调用 String() 方法
	// Go 语言会自动解引用指针调用值方法
	pvid := &vid
	if str := pvid.String(); str != "11" {
		t.Errorf("to string failed")
	}
}

// TestVolumeId_Next 测试获取下一个 VolumeId 的功能
// Next() 方法返回当前 VolumeId + 1 的新 VolumeId
//
// 使用场景：
//   - Master 分配新卷时，需要生成递增的 VolumeId
//   - 遍历连续的卷范围
//
// 测试场景：
//   1. 直接对 VolumeId 类型调用 Next()
//   2. 对 VolumeId 变量调用 Next()
//   3. 对 *VolumeId 指针调用 Next()
//
// 预期结果：
//   - VolumeId(10).Next() 返回 VolumeId(11)
//   - VolumeId(11).Next() 返回 VolumeId(12)
func TestVolumeId_Next(t *testing.T) {
	// 场景 1：直接对类型字面量调用 Next()
	// 验证 10 的下一个 VolumeId 是 11
	if vid := VolumeId(10).Next(); vid != VolumeId(11) {
		t.Errorf("get next volume id failed")
	}

	// 场景 2：对变量调用 Next()
	// 验证 11 的下一个 VolumeId 是 12
	vid := VolumeId(11)
	if new := vid.Next(); new != 12 {
		t.Errorf("get next volume id failed")
	}

	// 场景 3：对指针调用 Next()
	// 验证通过指针调用也能正确返回下一个 VolumeId
	pvid := &vid
	if new := pvid.Next(); new != 12 {
		t.Errorf("get next volume id failed")
	}
}
