// Package request_id 提供请求 ID 管理功能
// 用于在分布式系统中追踪和关联请求，方便日志分析和问题排查
//
// 主要功能:
//  1. 在 Context 中存储和获取请求 ID
//  2. 将请求 ID 注入到 HTTP 请求头中
//  3. 支持 AWS S3 兼容的请求 ID 格式
//
// 使用场景:
//   - 分布式请求追踪：在多个服务间传递请求 ID
//   - 日志关联：通过请求 ID 关联同一请求的所有日志
//   - 问题排查：根据请求 ID 查找特定请求的完整调用链
package request_id

import (
	"context"
	"net/http"
)

// AmzRequestIDHeader 定义 Amazon S3 兼容的请求 ID HTTP 头名称
// 使用 AWS 标准头名称 "x-amz-request-id"，确保与 S3 API 兼容
//
// 说明:
//   - SeaweedFS 支持 S3 兼容 API，因此使用相同的请求 ID 头
//   - 客户端可以通过此头获取请求 ID，用于问题报告和追踪
//   - 这是 AWS S3 的标准做法，所有响应都会包含此头
const AmzRequestIDHeader = "x-amz-request-id"

// Set 将请求 ID 存储到 Context 中
// 参数:
//   - ctx: 父 Context 对象
//   - id: 请求 ID 字符串（通常是 UUID 或其他唯一标识符）
//
// 返回值:
//   - context.Context: 包含请求 ID 的新 Context
//
// 工作原理:
//   使用 context.WithValue 创建新的 Context，以 AmzRequestIDHeader 为 key 存储请求 ID
//
// 使用场景:
//   - 在请求处理开始时设置请求 ID
//   - 将请求 ID 传递给下游函数和服务
//
// 示例:
//   requestID := uuid.New().String()
//   ctx = request_id.Set(ctx, requestID)
//   // 后续所有使用此 ctx 的地方都能获取到这个 ID
func Set(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, AmzRequestIDHeader, id)
}

// Get 从 Context 中获取请求 ID
// 参数:
//   - ctx: 包含请求 ID 的 Context 对象
//
// 返回值:
//   - string: 请求 ID，如果不存在则返回空字符串
//
// 工作原理:
//  1. 首先检查 ctx 是否为 nil，避免 panic
//  2. 使用 ctx.Value 获取存储的请求 ID
//  3. 使用类型断言将 interface{} 转换为 string
//  4. 如果类型断言失败（返回 false），返回空字符串
//
// 使用场景:
//   - 在日志中记录请求 ID
//   - 将请求 ID 传递给其他服务
//   - 在错误信息中包含请求 ID
//
// 示例:
//   requestID := request_id.Get(ctx)
//   log.Printf("[%s] Processing request", requestID)
func Get(ctx context.Context) string {
	// 防御性编程：检查 ctx 是否为 nil
	if ctx == nil {
		return ""
	}
	// 从 Context 中获取请求 ID，使用类型断言转换为 string
	// 如果 key 不存在或类型不匹配，id 为空字符串，ok 为 false
	id, _ := ctx.Value(AmzRequestIDHeader).(string)
	return id
}

// InjectToRequest 将请求 ID 注入到 HTTP 请求头中
// 参数:
//   - ctx: 包含请求 ID 的 Context 对象
//   - req: 要注入请求 ID 的 HTTP 请求对象
//
// 工作原理:
//  1. 检查 HTTP 请求对象是否为 nil
//  2. 从 Context 中获取请求 ID
//  3. 将请求 ID 设置到 HTTP 请求头的 x-amz-request-id 字段
//
// 使用场景:
//   - 在发起 HTTP 请求前调用，将请求 ID 传递给下游服务
//   - 在代理或网关中传递请求 ID
//   - 在微服务架构中保持请求追踪链
//
// 示例:
//   req, _ := http.NewRequest("GET", url, nil)
//   request_id.InjectToRequest(ctx, req)
//   // 现在 req 的 Header 中包含了请求 ID
//   resp, err := client.Do(req)
//
// 注意:
//   - 如果 req 为 nil，函数不会执行任何操作，避免 panic
//   - 如果 ctx 中没有请求 ID，会设置空字符串到 Header
func InjectToRequest(ctx context.Context, req *http.Request) {
	// 防御性编程：检查请求对象是否为 nil
	if req != nil {
		// 从 Context 中获取请求 ID 并设置到 HTTP Header
		// 如果 Get(ctx) 返回空字符串，也会设置到 Header（值为空）
		req.Header.Set(AmzRequestIDHeader, Get(ctx))
	}
}
