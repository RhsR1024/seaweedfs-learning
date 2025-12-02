// Package weed_server 中的 filer_grpc_server_kv.go 提供简单的键值存储接口
// 基于 Filer Store 后端实现,支持 Get/Put 操作
package weed_server

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// KvGet 从 Filer Store 中读取指定键的值
// 如果键不存在,返回空响应而不是错误
func (fs *FilerServer) KvGet(ctx context.Context, req *filer_pb.KvGetRequest) (*filer_pb.KvGetResponse, error) {

	// 调用 Filer Store 的 KvGet 接口查询键值
	// Store 可以是 MySQL、PostgreSQL、LevelDB 等任意后端
	value, err := fs.filer.Store.KvGet(ctx, req.Key)

	// 【特殊处理】键不存在的情况
	// 返回空响应而不是错误,方便客户端判断
	if err == filer.ErrKvNotFound {
		return &filer_pb.KvGetResponse{}, nil
	}

	// 如果是其他错误(如网络错误、存储后端错误),将错误信息放入响应中
	// 注意:这里不返回 gRPC 错误,而是将错误信息放在响应的 Error 字段
	if err != nil {
		return &filer_pb.KvGetResponse{Error: err.Error()}, nil
	}

	// 成功查询到值,返回给客户端
	return &filer_pb.KvGetResponse{
		Value: value, // 键对应的值(字节数组)
	}, nil

}

// KvPut 设置键值对,如果值为空则删除该键
// 参数:
//   - req.Key: 键名
//   - req.Value: 键值,如果为空则执行删除操作
// 返回:
//   - KvPutResponse: 包含可能的错误信息
func (fs *FilerServer) KvPut(ctx context.Context, req *filer_pb.KvPutRequest) (*filer_pb.KvPutResponse, error) {

	// 【特殊逻辑】值为空时执行删除操作
	// 这是一种便捷设计:
	//   - 客户端可以通过 Put(key, "") 来删除键
	//   - 避免需要单独提供 KvDelete 接口
	if len(req.Value) == 0 {
		// 执行删除操作
		if err := fs.filer.Store.KvDelete(ctx, req.Key); err != nil {
			// 删除失败,将错误信息放入响应中
			return &filer_pb.KvPutResponse{Error: err.Error()}, nil
		}
		// 删除成功,返回空响应
		return &filer_pb.KvPutResponse{}, nil
	}

	// 【正常流程】值不为空,执行插入或更新操作
	// 如果键已存在,会覆盖旧值
	err := fs.filer.Store.KvPut(ctx, req.Key, req.Value)
	if err != nil {
		// 插入/更新失败,将错误信息放入响应中
		return &filer_pb.KvPutResponse{Error: err.Error()}, nil
	}

	// 插入/更新成功,返回空响应
	return &filer_pb.KvPutResponse{}, nil

}
