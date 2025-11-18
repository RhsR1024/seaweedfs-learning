// Package operation 提供与 SeaweedFS 操作相关的功能
// 包括文件查找、上传、下载等核心操作
package operation

import (
	"context"
	"errors"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"google.golang.org/grpc"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// Location 表示一个卷的物理位置
// 包含访问该卷所需的所有网络信息
type Location struct {
	// Url HTTP 访问地址（内部使用）
	// 格式: "host:port"
	Url        string `json:"url,omitempty"`

	// PublicUrl 公开访问地址（客户端使用）
	// 通常是负载均衡器或公网地址
	PublicUrl  string `json:"publicUrl,omitempty"`

	// DataCenter 数据中心标识
	// 用于跨数据中心的数据副本管理
	DataCenter string `json:"dataCenter,omitempty"`

	// GrpcPort gRPC 服务端口
	// 用于服务器间的内部通信
	GrpcPort   int    `json:"grpcPort,omitempty"`
}

// ServerAddress 返回该位置的服务器地址
//
// 返回值:
//   - pb.ServerAddress: 包含 URL 和 gRPC 端口的服务器地址对象
//
// 用途:
//   - 用于建立到该服务器的 gRPC 连接
func (l *Location) ServerAddress() pb.ServerAddress {
	return pb.NewServerAddressWithGrpcPort(l.Url, l.GrpcPort)
}

// LookupResult 卷查找的结果
// 包含卷的位置信息和访问凭证
type LookupResult struct {
	// VolumeOrFileId 卷 ID 或文件 ID
	// 卷 ID 格式: "3"
	// 文件 ID 格式: "3,01637037d6"
	VolumeOrFileId string     `json:"volumeOrFileId,omitempty"`

	// Locations 卷的所有副本位置列表
	// 包含该卷所有副本的服务器地址
	Locations      []Location `json:"locations,omitempty"`

	// Jwt JWT 访问令牌
	// 如果启用了安全验证，需要此令牌访问文件
	Jwt            string     `json:"jwt,omitempty"`

	// Error 查找过程中的错误信息
	// 为空表示查找成功
	Error          string     `json:"error,omitempty"`
}

// String 返回 LookupResult 的字符串表示
// 用于日志输出和调试
func (lr *LookupResult) String() string {
	return fmt.Sprintf("VolumeOrFileId:%s, Locations:%v, Error:%s", lr.VolumeOrFileId, lr.Locations, lr.Error)
}

var (
	// vc 卷位置缓存
	// 缓存卷 ID 到位置的映射，避免频繁查询 Master
	// 缓存有效期为 10 分钟，过期后重新查询
	vc VidCache
)

// LookupFileId 根据文件 ID 查找文件的完整 URL
//
// 参数:
//   - masterFn: Master 服务器查找函数
//   - grpcDialOption: gRPC 连接选项
//   - fileId: 文件 ID，格式为 "volumeId,needleId"（如 "3,01637037d6"）
//
// 返回值:
//   - fullUrl: 文件的完整 HTTP URL
//   - jwt: JWT 访问令牌（如果启用了安全验证）
//   - err: 错误信息
//
// 工作流程:
//  1. 解析文件 ID，提取卷 ID（逗号前的部分）
//  2. 查找卷 ID 对应的所有位置
//  3. 随机选择一个位置（负载均衡）
//  4. 构建完整的文件访问 URL
//
// 示例:
//   fileId: "3,01637037d6"
//   返回: "http://192.168.1.100:8080/3,01637037d6"
func LookupFileId(masterFn GetMasterFn, grpcDialOption grpc.DialOption, fileId string) (fullUrl string, jwt string, err error) {
	// 步骤 1: 解析文件 ID
	// 文件 ID 格式: "volumeId,needleId"
	parts := strings.Split(fileId, ",")
	if len(parts) != 2 {
		return "", jwt, errors.New("Invalid fileId " + fileId)
	}

	// 步骤 2: 查找卷位置
	lookup, lookupError := LookupVolumeId(masterFn, grpcDialOption, parts[0])
	if lookupError != nil {
		return "", jwt, lookupError
	}

	// 检查是否找到位置
	if len(lookup.Locations) == 0 {
		return "", jwt, errors.New("File Not Found")
	}

	// 步骤 3 & 4: 随机选择一个位置并构建 URL
	// 使用随机选择实现简单的负载均衡
	return "http://" + lookup.Locations[rand.IntN(len(lookup.Locations))].Url + "/" + fileId, lookup.Jwt, nil
}

// LookupVolumeId 查找单个卷 ID 的位置信息
//
// 这是 LookupVolumeIds 的便捷包装方法，用于查找单个卷
//
// 参数:
//   - masterFn: Master 服务器查找函数
//   - grpcDialOption: gRPC 连接选项
//   - vid: 卷 ID（如 "3"）
//
// 返回值:
//   - *LookupResult: 卷的位置信息
//   - error: 错误信息
func LookupVolumeId(masterFn GetMasterFn, grpcDialOption grpc.DialOption, vid string) (*LookupResult, error) {
	results, err := LookupVolumeIds(masterFn, grpcDialOption, []string{vid})
	return results[vid], err
}

// LookupVolumeIds 批量查找多个卷 ID 的位置信息
//
// 这是核心查找方法，支持缓存和批量查询以提高性能
//
// 参数:
//   - masterFn: Master 服务器查找函数
//   - grpcDialOption: gRPC 连接选项
//   - vids: 卷 ID 列表
//
// 返回值:
//   - map[string]*LookupResult: 卷 ID 到查找结果的映射
//   - error: 错误信息
//
// 工作流程:
//  1. 从缓存中查找已知的卷位置
//  2. 收集缓存中不存在的卷 ID
//  3. 如果所有卷都在缓存中，直接返回
//  4. 通过 gRPC 向 Master 查询未知卷的位置
//  5. 将新查询的结果写入缓存（缓存 10 分钟）
//  6. 返回所有卷的查找结果
//
// 性能优化:
//   - 使用缓存减少对 Master 的查询压力
//   - 支持批量查询，一次请求查找多个卷
//   - 只查询缓存中不存在的卷，避免重复查询
func LookupVolumeIds(masterFn GetMasterFn, grpcDialOption grpc.DialOption, vids []string) (map[string]*LookupResult, error) {
	ret := make(map[string]*LookupResult)
	var unknown_vids []string

	// 步骤 1 & 2: 先检查缓存
	for _, vid := range vids {
		locations, cacheErr := vc.Get(vid)
		if cacheErr == nil {
			// 缓存命中
			ret[vid] = &LookupResult{VolumeOrFileId: vid, Locations: locations}
		} else {
			// 缓存未命中，需要查询
			unknown_vids = append(unknown_vids, vid)
		}
	}

	// 步骤 3: 如果所有卷 ID 都在缓存中，直接返回
	if len(unknown_vids) == 0 {
		return ret, nil
	}

	// 步骤 4-6: 查询未知卷的位置
	err := WithMasterServerClient(false, masterFn(context.Background()), grpcDialOption, func(masterClient master_pb.SeaweedClient) error {

		// 构建查询请求
		req := &master_pb.LookupVolumeRequest{
			VolumeOrFileIds: unknown_vids,
		}

		// 向 Master 发起 gRPC 查询
		resp, grpcErr := masterClient.LookupVolume(context.Background(), req)
		if grpcErr != nil {
			return grpcErr
		}

		// 处理查询结果并更新缓存
		for _, vidLocations := range resp.VolumeIdLocations {
			var locations []Location
			// 转换位置信息格式
			for _, loc := range vidLocations.Locations {
				locations = append(locations, Location{
					Url:        loc.Url,
					PublicUrl:  loc.PublicUrl,
					DataCenter: loc.DataCenter,
					GrpcPort:   int(loc.GrpcPort),
				})
			}

			// 将查询结果写入缓存（有效期 10 分钟）
			// 注意: 即使查询出错也会缓存，避免反复查询不存在的卷
			if vidLocations.Error != "" {
				vc.Set(vidLocations.VolumeOrFileId, locations, 10*time.Minute)
			}

			// 保存到返回结果
			ret[vidLocations.VolumeOrFileId] = &LookupResult{
				VolumeOrFileId: vidLocations.VolumeOrFileId,
				Locations:      locations,
				Jwt:            vidLocations.Auth,
				Error:          vidLocations.Error,
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return ret, nil
}
