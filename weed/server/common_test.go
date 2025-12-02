// Package weed_server 的测试文件
// 本文件测试 common.go 中的 URL 解析功能
package weed_server

import (
	"strings"
	"testing"
)

// TestParseURL 测试 URL 路径解析功能
// 验证 parseURLPath 函数能否正确解析 SeaweedFS 的文件 URL
//
// 测试场景:
//   1. 标准格式: /volumeId,fileKey (如 "/1,06dfa8a684")
//   2. 带 Cookie 格式: /volumeId,fileKey_cookie (如 "/1,06dfa8a684_1")
//
// SeaweedFS 文件 ID 格式说明:
//   - volumeId: 卷 ID，标识文件存储在哪个卷中
//   - fileKey: 文件键，卷内的唯一标识符
//   - cookie: 可选的安全令牌，防止 URL 被猜测
//
// 测试逻辑:
//   1. 测试不带 cookie 的标准格式
//   2. 测试带 cookie 的格式
//   3. 验证可以正确提取出不含 cookie 的纯 fileKey
func TestParseURL(t *testing.T) {
	// 测试用例 1: 标准格式 "/volumeId,fileKey"
	// 期望: vid="1", fid="06dfa8a684"
	if vid, fid, _, _, _ := parseURLPath("/1,06dfa8a684"); true {
		// 验证卷 ID 解析正确
		if vid != "1" {
			t.Errorf("fail to parse vid: %s", vid)
		}
		// 验证文件 ID 解析正确
		if fid != "06dfa8a684" {
			t.Errorf("fail to parse fid: %s", fid)
		}
	}

	// 测试用例 2: 带 Cookie 格式 "/volumeId,fileKey_cookie"
	// 期望: vid="1", fid="06dfa8a684_1" (含 cookie)
	//       去除 cookie 后: fid="06dfa8a684"
	if vid, fid, _, _, _ := parseURLPath("/1,06dfa8a684_1"); true {
		// 验证卷 ID 解析正确
		if vid != "1" {
			t.Errorf("fail to parse vid: %s", vid)
		}
		// 验证完整的文件 ID（含 cookie）解析正确
		if fid != "06dfa8a684_1" {
			t.Errorf("fail to parse fid: %s", fid)
		}

		// 测试去除 cookie 的逻辑
		// Cookie 使用下划线 "_" 分隔，位于 fileKey 之后
		// 例如: "06dfa8a684_1" -> "06dfa8a684"
		if sepIndex := strings.LastIndex(fid, "_"); sepIndex > 0 {
			// 截取下划线之前的部分，即纯文件键
			fid = fid[:sepIndex]
		}

		// 验证去除 cookie 后的纯文件 ID
		if fid != "06dfa8a684" {
			t.Errorf("fail to parse fid: %s", fid)
		}
	}
}
