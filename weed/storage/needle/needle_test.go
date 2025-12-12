// Package needle 提供 SeaweedFS 的 Needle 存储结构和相关操作
// 本文件测试 NeedleId 和 Cookie 的解析功能
//
// ============================================================================
// KeyHash 解析规则
// ============================================================================
//
// KeyHash 是 NeedleId 和 Cookie 的十六进制组合字符串：
//
// 格式：<NeedleId><Cookie>
//
// 长度规则：
// - 最小长度：9 个字符（至少 1 个字符的 NeedleId + 8 个字符的 Cookie）
// - 最大长度：24 个字符（16 个字符的 NeedleId + 8 个字符的 Cookie）
//
// 解析规则：
// - Cookie 固定占用最后 8 个十六进制字符（4 字节）
// - NeedleId 占用剩余的字符（1-16 个字符，即 0.5-8 字节）
//
// 示例：
// - "4ed4c8116e41" -> NeedleId=0x4ed4, Cookie=0xc8116e41
// - "fed4c8114ed4c811f0116e41" -> NeedleId=0xfed4c8114ed4c811, Cookie=0xf0116e41
//
// ============================================================================
package needle

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// TestParseKeyHash 测试 ParseNeedleIdCookie 函数
//
// 测试场景覆盖：
// 1. 正常长度的 KeyHash
// 2. Cookie 带前导零的情况
// 3. 奇数长度的 KeyHash
// 4. 最大长度的 KeyHash（完整 uint64）
// 5. 过短的 KeyHash（错误情况）
// 6. 过长的 KeyHash（错误情况）
// 7. 包含非法字符的 KeyHash（错误情况）
func TestParseKeyHash(t *testing.T) {
	// 定义测试用例结构
	testcases := []struct {
		KeyHash string         // 输入的十六进制字符串
		ID      types.NeedleId // 期望的 NeedleId
		Cookie  types.Cookie   // 期望的 Cookie
		Err     bool           // 是否期望返回错误
	}{
		// ========== 正常情况 ==========

		// 用例 1：标准格式
		// KeyHash = "4ed4c8116e41"（12 字符）
		// 解析：NeedleId = 0x4ed4（前 4 字符），Cookie = 0xc8116e41（后 8 字符）
		{"4ed4c8116e41", 0x4ed4, 0xc8116e41, false},

		// 用例 2：Cookie 带前导零
		// KeyHash = "4ed401116e41"（12 字符）
		// 解析：NeedleId = 0x4ed4，Cookie = 0x01116e41
		// 注意：前导零被保留，Cookie 值小于 0x10000000
		{"4ed401116e41", 0x4ed4, 0x01116e41, false},

		// 用例 3：奇数长度
		// KeyHash = "ed400116e41"（11 字符）
		// 解析：NeedleId = 0xed4（前 3 字符），Cookie = 0x00116e41（后 8 字符）
		// 奇数长度会在解析时被正确处理
		{"ed400116e41", 0xed4, 0x00116e41, false},

		// 用例 4：完整的 uint64 NeedleId
		// KeyHash = "fed4c8114ed4c811f0116e41"（24 字符，最大长度）
		// 解析：NeedleId = 0xfed4c8114ed4c811（前 16 字符），Cookie = 0xf0116e41（后 8 字符）
		{"fed4c8114ed4c811f0116e41", 0xfed4c8114ed4c811, 0xf0116e41, false},

		// ========== 错误情况 ==========

		// 用例 5：过短（少于 9 字符）
		// KeyHash = "4ed4c811"（8 字符）
		// Cookie 需要 8 字符，NeedleId 至少需要 1 字符，所以最少 9 字符
		{"4ed4c811", 0, 0, true},

		// 用例 6：过长（超过 24 字符）
		// KeyHash = "4ed4c8114ed4c8114ed4c8111"（25 字符）
		// NeedleId 最多 16 字符 + Cookie 8 字符 = 24 字符
		{"4ed4c8114ed4c8114ed4c8111", 0, 0, true},

		// 用例 7：非法字符
		// KeyHash = "helloworld"（包含非十六进制字符）
		{"helloworld", 0, 0, true},
	}

	// 执行测试
	for _, tc := range testcases {
		id, cookie, err := ParseNeedleIdCookie(tc.KeyHash)

		// 检查错误情况
		if err != nil && !tc.Err {
			// 不期望错误但发生了错误
			t.Fatalf("Parse %s error: %v", tc.KeyHash, err)
		} else if err == nil && tc.Err {
			// 期望错误但没有错误
			t.Fatalf("Parse %s expected error got nil", tc.KeyHash)
		} else if id != tc.ID || cookie != tc.Cookie {
			// 解析结果不匹配
			t.Fatalf("Parse %s wrong result. Expected: (%d, %d) got: (%d, %d)",
				tc.KeyHash, tc.ID, tc.Cookie, id, cookie)
		}
	}
}

// BenchmarkParseKeyHash 基准测试 ParseNeedleIdCookie 的性能
//
// 测试目的：
// - 评估解析函数的性能
// - 检查是否有不必要的内存分配
//
// 使用方法：
//
//	go test -bench=BenchmarkParseKeyHash -benchmem
//
// 期望结果：
// - 高吞吐量（每秒处理大量请求）
// - 低内存分配（理想情况下零分配）
func BenchmarkParseKeyHash(b *testing.B) {
	// 报告内存分配情况
	b.ReportAllocs()

	// 使用最大长度的 KeyHash 进行测试
	// 这是最坏情况，需要解析完整的 24 个字符
	for i := 0; i < b.N; i++ {
		ParseNeedleIdCookie("4ed44ed44ed44ed4c8116e41")
	}
}
