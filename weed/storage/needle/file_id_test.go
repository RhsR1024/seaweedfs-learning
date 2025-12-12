// Package needle 提供 SeaweedFS 的 Needle 存储结构和相关操作
// 本文件测试 FileId 的解析功能
//
// ============================================================================
// FileId 格式说明
// ============================================================================
//
// FileId 是 SeaweedFS 中文件的唯一标识符，格式为：
//   <VolumeId>,<NeedleId><Cookie>
//
// 示例：
//   - "3,01e3b0756f" -> VolumeId=3, NeedleId+Cookie 组合
//   - "100,123456789012345678901234" -> 完整格式
//
// NeedleId+Cookie 部分（KeyHash）：
//   - 长度必须在 9-24 个十六进制字符之间
//   - 前面是 NeedleId（最多 16 个十六进制字符，即 8 字节）
//   - 后面 8 个十六进制字符是 Cookie（4 字节）
//
// ============================================================================
package needle

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// TestParseFileIdFromString 测试 FileId 字符串解析功能
//
// 测试场景覆盖：
// 1. KeyHash 过短（少于 9 个字符）
// 2. VolumeId 和 KeyHash 之间有空格（格式错误）
// 3. 正常长度的 KeyHash（9 个字符）
// 4. 完整长度的 KeyHash（24 个字符）
// 5. 带前导零的 KeyHash
// 6. KeyHash 过长（超过 24 个字符）
func TestParseFileIdFromString(t *testing.T) {
	// ========== 测试用例 1：KeyHash 过短 ==========
	// KeyHash = "12345678"（8 个字符，少于最小要求的 9 个）
	// Cookie 需要 8 个十六进制字符，所以至少需要 9 个字符（1 个 NeedleId + 8 个 Cookie）
	fidStr1 := "100,12345678"
	_, err := ParseFileIdFromString(fidStr1)
	if err == nil {
		t.Errorf("%s : KeyHash is too short", fidStr1)
	}

	// ========== 测试用例 2：格式错误（包含空格）==========
	// 逗号后面有空格，会导致解析 NeedleId 失败
	fidStr1 = "100, 12345678"
	_, err = ParseFileIdFromString(fidStr1)
	if err == nil {
		t.Errorf("%s : needleId invalid syntax", fidStr1)
	}

	// ========== 测试用例 3：最小有效长度 ==========
	// KeyHash = "123456789"（9 个字符）
	// 解析结果：NeedleId = 0x1, Cookie = 0x23456789
	fidStr1 = "100,123456789"
	_, err = ParseFileIdFromString(fidStr1)
	if err != nil {
		t.Errorf("%s : should be OK", fidStr1)
	}

	// ========== 测试用例 4：完整长度（24 个字符）==========
	// KeyHash = "123456789012345678901234"
	// 解析规则：
	//   - 总长度 24 字符
	//   - NeedleId = 前 16 个字符 = 0x1234567890123456
	//   - Cookie = 后 8 个字符 = 0x78901234
	var fileId *FileId
	fidStr1 = "100,123456789012345678901234"
	fileId, err = ParseFileIdFromString(fidStr1)
	if err != nil {
		t.Errorf("%s : should be OK", fidStr1)
	}
	// 验证解析结果
	if !(fileId.VolumeId == VolumeId(100) &&
		fileId.Key == types.NeedleId(0x1234567890123456) &&
		fileId.Cookie == types.Cookie(types.Uint32ToCookie(uint32(0x78901234)))) {
		t.Errorf("src : %s, dest : %v", fidStr1, fileId)
	}

	// ========== 测试用例 5：带前导零的 KeyHash ==========
	// KeyHash = "abcd0000abcd"（12 个字符）
	// 解析规则：
	//   - NeedleId = 前 4 个字符 = 0xabcd（前导零被省略）
	//   - Cookie = 后 8 个字符 = 0x0000abcd
	// 注意：十六进制字符串中的前导零不影响解析
	fidStr1 = "100,abcd0000abcd"
	fileId, err = ParseFileIdFromString(fidStr1)
	if err != nil {
		t.Errorf("%s : should be OK", fidStr1)
	}
	if !(fileId.VolumeId == VolumeId(100) &&
		fileId.Key == types.NeedleId(0xabcd) &&
		fileId.Cookie == types.Cookie(types.Uint32ToCookie(uint32(0xabcd)))) {
		t.Errorf("src : %s, dest : %v", fidStr1, fileId)
	}

	// ========== 测试用例 6：KeyHash 过长 ==========
	// KeyHash = "1234567890123456789012345"（25 个字符，超过最大 24 个）
	// NeedleId 最多 8 字节（16 个十六进制字符）+ Cookie 4 字节（8 个十六进制字符）= 24 个字符
	fidStr1 = "100,1234567890123456789012345"
	_, err = ParseFileIdFromString(fidStr1)
	if err == nil {
		t.Errorf("%s : needleId is too long", fidStr1)
	}
}
