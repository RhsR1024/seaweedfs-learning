// Package filer_ui 的测试文件
// 测试面包屑导航功能的正确性
package filer_ui

import (
	"reflect"
	"testing"
)

// TestToBreadcrumb 测试 ToBreadcrumb 函数的各种路径转换场景
// 验证面包屑导航生成的正确性，包括：
//   - 空路径处理
//   - 根目录处理
//   - 单级路径
//   - 多级路径
func TestToBreadcrumb(t *testing.T) {
	// 定义测试参数结构
	type args struct {
		fullpath string
	}

	// 定义测试用例数组
	tests := []struct {
		name       string        // 测试用例名称
		args       args          // 输入参数
		wantCrumbs []Breadcrumb  // 期望的面包屑结果
	}{
		{
			// 测试场景 1：空路径
			// 空字符串应该被处理为根目录 "/"
			name: "empty",
			args: args{
				fullpath: "",
			},
			wantCrumbs: []Breadcrumb{
				{
					Name: "/",
					Link: "/",
				},
			},
		},
		{
			// 测试场景 2：根目录
			// "/" 应该生成单个根节点
			name: "test1",
			args: args{
				fullpath: "/",
			},
			wantCrumbs: []Breadcrumb{
				{
					Name: "/",
					Link: "/",
				},
			},
		},
		{
			// 测试场景 3：单级路径
			// "/abc" 应该生成两个节点：根 + abc
			name: "test2",
			args: args{
				fullpath: "/abc",
			},
			wantCrumbs: []Breadcrumb{
				{
					Name: "/",      // 第一个节点：根目录
					Link: "/",
				},
				{
					Name: "abc",    // 第二个节点：abc 目录
					Link: "/abc/",  // 链接包含完整路径，以 / 结尾
				},
			},
		},
		{
			// 测试场景 4：多级路径
			// "/abc/def" 应该生成三个节点：根 + abc + def
			name: "test3",
			args: args{
				fullpath: "/abc/def",
			},
			wantCrumbs: []Breadcrumb{
				{
					Name: "/",      // 第一个节点：根目录
					Link: "/",
				},
				{
					Name: "abc",    // 第二个节点：abc 目录
					Link: "/abc/",
				},
				{
					Name: "def",         // 第三个节点：def 目录
					Link: "/abc/def/",   // 链接是从根到 def 的完整路径
				},
			},
		},
	}

	// 运行所有测试用例
	for _, tt := range tests {
		// 使用 t.Run 为每个测试用例创建子测试
		t.Run(tt.name, func(t *testing.T) {
			// 调用被测函数
			gotCrumbs := ToBreadcrumb(tt.args.fullpath)

			// 使用 reflect.DeepEqual 深度比较结果
			// 这会比较数组长度、每个元素的 Name 和 Link 字段
			if !reflect.DeepEqual(gotCrumbs, tt.wantCrumbs) {
				t.Errorf("ToBreadcrumb() = %v, want %v", gotCrumbs, tt.wantCrumbs)
			}
		})
	}
}
