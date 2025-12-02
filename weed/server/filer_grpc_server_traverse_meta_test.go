// Package weed_server 中的 filer_grpc_server_traverse_meta_test.go
// 测试前缀树 (ptrie) 的匹配行为，用于验证路径排除功能
package weed_server

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/ptrie"
	"testing"
)

// TestPtrie 测试前缀树 (Prefix Trie) 的各种匹配方法
// 验证 MatchPrefix、MatchAll、Has 三个方法的正确性
//
// 前缀树的核心概念:
//   - MatchPrefix: 匹配路径的任意前缀
//     例如: 路径 "/topics/abc/dev" 可以匹配前缀 "/topics/abc"
//   - MatchAll: 匹配路径的所有前缀
//     例如: 路径 "/topics/abc/dev" 匹配 "/topics/abc" 和 "/topics/abc/d"
//   - Has: 精确匹配，路径必须完全相同
//     例如: "/topics/abc/dev" 不等于 "/topics/abc"
func TestPtrie(t *testing.T) {
	// 【准备测试数据】
	// 测试路径: "/topics/abc/dev"
	b := []byte("/topics/abc/dev")

	// 创建前缀树并插入两个排除前缀
	excludedTrie := ptrie.New[bool]()
	excludedTrie.Put([]byte("/topics/abc/d"), true)   // 前缀 1: "/topics/abc/d"
	excludedTrie.Put([]byte("/topics/abc"), true)     // 前缀 2: "/topics/abc"

	// 【测试 1: MatchPrefix】
	// 期望: 返回 true
	// 原因: "/topics/abc/dev" 的前缀包含 "/topics/abc"
	//       MatchPrefix 只要找到任意一个匹配的前缀就返回 true
	assert.True(t, excludedTrie.MatchPrefix(b, func(key []byte, value bool) bool {
		// 回调函数会被调用，打印匹配到的前缀
		// 预期输出: "matched1 /topics/abc"
		println("matched1", string(key))
		return true // 返回 true 表示匹配成功
	}))

	// 【测试 2: MatchAll - 成功情况】
	// 期望: 返回 true
	// 原因: "/topics/abc/dev" 的前缀包含 "/topics/abc" 和 "/topics/abc/d"
	//       MatchAll 会遍历所有匹配的前缀，只有当回调函数对所有前缀都返回 true 时才返回 true
	assert.True(t, excludedTrie.MatchAll(b, func(key []byte, value bool) bool {
		// 回调函数会被调用多次（每个匹配的前缀一次）
		// 预期输出:
		//   "matched2 /topics/abc"
		//   "matched2 /topics/abc/d"
		println("matched2", string(key))
		return true // 对所有前缀都返回 true
	}))

	// 【测试 3: MatchAll - 失败情况】
	// 期望: 返回 false
	// 原因: "/topics/ab" 没有任何前缀匹配树中的前缀
	//       "/topics/ab" 不是 "/topics/abc" 的前缀
	//       MatchAll 找不到任何匹配，返回 false
	assert.False(t, excludedTrie.MatchAll([]byte("/topics/ab"), func(key []byte, value bool) bool {
		// 回调函数不会被调用，因为没有任何前缀匹配
		println("matched3", string(key))
		return true
	}))

	// 【测试 4: Has - 精确匹配】
	// 期望: 返回 false
	// 原因: Has 要求精确匹配，而树中只有:
	//       - "/topics/abc"
	//       - "/topics/abc/d"
	//       都不等于 "/topics/abc/dev"
	assert.False(t, excludedTrie.Has(b))
}
