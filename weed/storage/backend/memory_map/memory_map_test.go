// Package memory_map 内存映射包的单元测试
package memory_map

import "testing"

// TestMemoryMapMaxSizeReadWrite 测试内存映射大小配置的读取和解析功能
//
// 测试目标：
//   - ReadMemoryMapMaxSizeMb 函数能正确解析字符串形式的配置值
//
// 测试用例：
//   - 输入："5000"（字符串）
//   - 预期输出：5000（uint32）
//
// 测试覆盖：
//   - 正常的数字字符串解析
//   - 类型转换（string -> uint64 -> uint32）
//
// 未覆盖的场景（可扩展）：
//   - 空字符串 "" -> 应返回 0
//   - 无效字符串 "abc" -> 应返回错误
//   - 超大数值 "99999999999" -> 应返回错误
//   - 负数 "-100" -> 应返回错误
func TestMemoryMapMaxSizeReadWrite(t *testing.T) {
	// 测试：解析 "5000" 字符串为 5000 MB
	memoryMapSize, _ := ReadMemoryMapMaxSizeMb("5000")

	// 断言：解析结果应为 5000
	if memoryMapSize != 5000 {
		t.Errorf("empty memoryMapSize:%v", memoryMapSize)
	}
}
