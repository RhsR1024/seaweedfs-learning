// Package weed_server 实现 Volume Server 的批量删除 gRPC 接口
// 本文件提供高效的批量文件删除功能
//
// 核心功能:
//   - 批量删除多个文件，减少网络往返次数
//   - 支持 Cookie 验证，防止未授权删除
//   - 支持普通 Volume 和 EC Volume
//   - 返回每个文件的删除结果
//
// 删除机制:
//   - 软删除：不立即删除物理文件，只标记为已删除
//   - 更新 Needle 的 LastModified 时间戳
//   - 在索引中标记 Needle 为已删除
//   - 物理空间通过 Vacuum 操作回收
//
// 安全性:
//   - Cookie 验证：防止未授权删除
//   - 跳过 ChunkManifest：避免误删大文件的元数据
//
// 性能优化:
//   - 批量操作减少网络开销
//   - 一次 gRPC 调用删除多个文件
//   - 适合大规模清理场景
package weed_server

import (
	"context"
	"net/http"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// BatchDelete 批量删除多个文件
// gRPC API: BatchDelete
//
// 功能:
//   - 一次调用删除多个文件
//   - 验证 Cookie 防止未授权删除
//   - 支持普通 Volume 和 EC Volume
//   - 返回每个文件的删除结果
//
// 参数:
//   - FileIds: 文件 ID 列表，格式: "volumeId,fileKey[_cookie]"
//     例如: ["3,01e3b0756f", "3,01e3b0757a_a1b2c3d4"]
//   - SkipCookieCheck: 是否跳过 Cookie 验证（不推荐）
//
// 删除流程:
//   【步骤 1：解析文件 ID】
//   - 解析 volumeId、fileKey、cookie
//   - 格式错误时返回 400 Bad Request
//
//   【步骤 2：Cookie 验证（可选）】
//   - 如果 SkipCookieCheck=false:
//     a. 读取 Needle 获取真实 Cookie
//     b. 对比请求中的 Cookie 是否匹配
//     c. 不匹配时返回 400 Bad Request
//   - 如果 SkipCookieCheck=true:
//     a. 直接解析 NeedleId，跳过验证
//
//   【步骤 3：检查 ChunkManifest】
//   - 如果是 ChunkManifest（大文件元数据）:
//     a. 返回 406 Not Acceptable
//     b. 防止误删大文件的索引
//
//   【步骤 4：执行删除】
//   - 设置 LastModified 为当前时间
//   - 调用 DeleteVolumeNeedle 或 DeleteEcShardNeedle
//   - 在索引中标记为已删除
//   - 物理空间在 Vacuum 时回收
//
// 返回结果:
//   - 每个文件返回一个 DeleteResult:
//     - FileId: 文件 ID
//     - Status: HTTP 状态码
//       - 202 Accepted: 删除成功
//       - 304 Not Modified: 文件已删除（重复删除）
//       - 400 Bad Request: 格式错误或 Cookie 不匹配
//       - 404 Not Found: 文件不存在
//       - 406 Not Acceptable: ChunkManifest 不允许批量删除
//       - 500 Internal Server Error: 删除失败
//     - Size: 释放的空间大小（字节）
//     - Error: 错误信息（如果有）
//
// 使用场景:
//   - 清理过期文件
//   - 删除用户数据
//   - 批量数据迁移后的清理
//   - 降低删除操作的网络开销
//
// 注意事项:
//   - SkipCookieCheck=true 存在安全风险，仅在内部调用时使用
//   - ChunkManifest 不支持批量删除，需单独处理
//   - 删除是软删除，空间需要 Vacuum 回收
//   - Cookie 验证会增加磁盘 I/O（需读取 Needle）
//
// 性能考虑:
//   - 批量大小建议 100-1000 个文件
//   - Cookie 验证会增加延迟
//   - EC Volume 删除比普通 Volume 慢
func (vs *VolumeServer) BatchDelete(ctx context.Context, req *volume_server_pb.BatchDeleteRequest) (*volume_server_pb.BatchDeleteResponse, error) {

	resp := &volume_server_pb.BatchDeleteResponse{}

	// 获取当前时间，用于设置 LastModified
	now := uint64(time.Now().Unix())

	// 遍历所有要删除的文件 ID
	for _, fid := range req.FileIds {
		// 【步骤 1：解析文件 ID】
		// 格式: "volumeId,fileKey[_cookie]"
		// 例如: "3,01e3b0756f" 或 "3,01e3b0756f_a1b2c3d4"
		vid, id_cookie, err := operation.ParseFileId(fid)
		if err != nil {
			// 解析失败，返回 400 Bad Request
			resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
				FileId: fid,
				Status: http.StatusBadRequest,
				Error:  err.Error()})
			continue
		}

		// 创建 Needle 对象
		n := new(needle.Needle)
		volumeId, _ := needle.NewVolumeId(vid)

		// 检查是否为 EC Volume
		ecVolume, isEcVolume := vs.store.FindEcVolume(volumeId)

		// 【步骤 2：Cookie 验证（可选）】
		if req.SkipCookieCheck {
			// 跳过 Cookie 验证，直接解析 NeedleId
			// 警告：存在安全风险，仅内部调用时使用
			n.Id, _, err = needle.ParseNeedleIdCookie(id_cookie)
			if err != nil {
				resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
					FileId: fid,
					Status: http.StatusBadRequest,
					Error:  err.Error()})
				continue
			}
		} else {
			// 执行 Cookie 验证
			// 步骤 2.1：解析请求中的 Cookie
			n.ParsePath(id_cookie)
			cookie := n.Cookie

			// 步骤 2.2：读取 Needle 获取真实 Cookie
			if !isEcVolume {
				// 从普通 Volume 读取
				if _, err := vs.store.ReadVolumeNeedle(volumeId, n, nil, nil); err != nil {
					// 文件不存在，返回 404 Not Found
					resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
						FileId: fid,
						Status: http.StatusNotFound,
						Error:  err.Error(),
					})
					continue
				}
			} else {
				// 从 EC Volume 读取
				if _, err := vs.store.ReadEcShardNeedle(volumeId, n, nil); err != nil {
					// 文件不存在，返回 404 Not Found
					resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
						FileId: fid,
						Status: http.StatusNotFound,
						Error:  err.Error(),
					})
					continue
				}
			}

			// 步骤 2.3：验证 Cookie 是否匹配
			if n.Cookie != cookie {
				// Cookie 不匹配，返回 400 Bad Request
				// 这可以防止未授权删除
				resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
					FileId: fid,
					Status: http.StatusBadRequest,
					Error:  "File Random Cookie does not match.",
				})
				break
			}
		}

		// 【步骤 3：检查 ChunkManifest】
		// ChunkManifest 是大文件的元数据，包含所有分块信息
		// 批量删除不支持 ChunkManifest，因为需要特殊处理
		if n.IsChunkedManifest() {
			resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
				FileId: fid,
				Status: http.StatusNotAcceptable,
				Error:  "ChunkManifest: not allowed in batch delete mode.",
			})
			continue
		}

		// 【步骤 4：执行删除】
		// 设置 LastModified 时间戳
		// 这标记了文件被删除的时间
		n.LastModified = now

		if !isEcVolume {
			// 删除普通 Volume 中的 Needle
			if size, err := vs.store.DeleteVolumeNeedle(volumeId, n); err != nil {
				// 删除失败，返回 500 Internal Server Error
				resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
					FileId: fid,
					Status: http.StatusInternalServerError,
					Error:  err.Error()},
				)
			} else if size == 0 {
				// size=0 表示文件已被删除（重复删除）
				// 返回 304 Not Modified
				resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
					FileId: fid,
					Status: http.StatusNotModified},
				)
			} else {
				// 删除成功，返回 202 Accepted
				// size 是释放的空间大小
				resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
					FileId: fid,
					Status: http.StatusAccepted,
					Size:   uint32(size)},
				)
			}
		} else {
			// 删除 EC Volume 中的 Needle
			if size, err := vs.store.DeleteEcShardNeedle(ecVolume, n, n.Cookie); err != nil {
				// 删除失败，返回 500 Internal Server Error
				resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
					FileId: fid,
					Status: http.StatusInternalServerError,
					Error:  err.Error()},
				)
			} else {
				// 删除成功，返回 202 Accepted
				resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
					FileId: fid,
					Status: http.StatusAccepted,
					Size:   uint32(size)},
				)
			}
		}
	}

	return resp, nil

}
