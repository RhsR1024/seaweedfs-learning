// Package weed_server 实现 Volume Server 的 Query 查询功能
// 本文件提供对存储在 Volume 中的结构化数据（JSON/CSV）的查询能力
//
// 核心功能:
//   - JSON 数据查询：对 Volume 中的 JSON 数据进行过滤和字段选择
//   - CSV 数据查询：对 Volume 中的 CSV 数据进行查询（待实现）
//   - 流式返回：查询结果通过 gRPC 流式返回，支持大数据量
//   - 字段过滤：根据条件过滤数据行
//   - 字段投影：只返回指定的字段，减少数据传输
//
// 使用场景:
//   - 日志查询：直接查询存储在 SeaweedFS 中的 JSON 日志文件
//   - 数据分析：对存储的结构化数据进行简单过滤和筛选
//   - 减少传输：只返回需要的字段，节省带宽
//
// 查询流程:
//   1. 根据 fid 读取 Needle 数据
//   2. 根据 InputSerialization 确定数据格式（JSON/CSV）
//   3. 根据 Filter 过滤数据行
//   4. 根据 Selections 选择需要的字段
//   5. 通过 gRPC 流式返回查询结果
//
// 限制:
//   - 仅支持简单的过滤条件（字段、操作符、值）
//   - 不支持复杂的 SQL 查询
//   - 查询性能取决于 Needle 大小和数据格式
//
// 示例查询:
//   - Filter: {Field: "level", Op: "=", Value: "ERROR"}
//   - Selections: ["timestamp", "message"]
//   - 结果：返回所有 level=ERROR 的日志条目的 timestamp 和 message 字段
package weed_server

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/query/json"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/tidwall/gjson"
)

// Query 处理结构化数据查询请求
// 支持对 Volume 中的 JSON/CSV 数据进行过滤和字段选择
//
// 参数:
//   - req: 查询请求，包含：
//     - FromFileIds: 要查询的文件 ID 列表（fid）
//     - InputSerialization: 数据格式（JSON/CSV）
//     - Filter: 过滤条件（字段、操作符、值）
//     - Selections: 需要返回的字段列表
//   - stream: gRPC 流，用于流式返回查询结果
//
// 返回:
//   - error: 查询错误（文件不存在、格式错误等）
//
// 工作流程:
//   1. 遍历所有 FromFileIds
//   2. 解析每个 fid，读取对应的 Needle
//   3. 验证 Cookie 确保文件完整性
//   4. 根据数据格式（JSON/CSV）进行查询
//   5. 将查询结果通过 stream 返回
//
// 示例:
//   req := &QueryRequest{
//       FromFileIds: []string{"3,01abc", "3,01def"},
//       InputSerialization: &InputSerialization{
//           JsonInput: &JsonInput{Type: "LINES"},
//       },
//       Filter: &Filter{
//           Field: "level",
//           Operand: "=",
//           Value: "ERROR",
//       },
//       Selections: []string{"timestamp", "message"},
//   }
func (vs *VolumeServer) Query(req *volume_server_pb.QueryRequest, stream volume_server_pb.VolumeServer_QueryServer) error {

	// 遍历所有要查询的文件 ID
	for _, fid := range req.FromFileIds {

		// 【解析文件 ID】
		// fid 格式：volumeId,needleId_cookie
		// 例如：3,01e3b0756f_a1b2c3d4
		vid, id_cookie, err := operation.ParseFileId(fid)
		if err != nil {
			glog.V(0).Infof("volume query 解析 fid %s 失败: %v", fid, err)
			return err
		}

		// 【创建 Needle 对象】
		n := new(needle.Needle)
		volumeId, _ := needle.NewVolumeId(vid)
		// 解析 needleId 和 cookie
		n.ParsePath(id_cookie)

		// 【验证 Cookie】
		// 保存 cookie 用于后续验证
		// Cookie 用于防止 URL 猜测攻击
		cookie := n.Cookie

		// 【读取 Needle 数据】
		// 从 Volume 中读取 Needle 的完整数据（元数据 + 内容）
		if _, err := vs.store.ReadVolumeNeedle(volumeId, n, nil, nil); err != nil {
			glog.V(0).Infof("volume query 读取 fid %s 失败: %v", fid, err)
			return err
		}

		// 【Cookie 验证】
		// 确保读取的 Needle 的 Cookie 与 URL 中的 Cookie 一致
		// 不一致说明文件已损坏或 URL 被篡改
		if n.Cookie != cookie {
			glog.V(0).Infof("volume query 读取 fid cookie %s 失败: %v", fid, err)
			return err
		}

		// 【CSV 数据查询】
		// TODO: 实现 CSV 数据的查询功能
		if req.InputSerialization.CsvInput != nil {
			// CSV 查询逻辑待实现
			// 可能需要解析 CSV 格式，应用过滤器，选择字段
		}

		// 【JSON 数据查询】
		// 对 Needle 中的 JSON 数据进行查询
		if req.InputSerialization.JsonInput != nil {

			// 创建查询结果条带（Stripe）
			// 条带是查询结果的批次，包含多条记录
			stripe := &volume_server_pb.QueriedStripe{
				Records: nil,
			}

			// 【构造过滤器】
			// Filter 定义了数据行的过滤条件
			// 例如：{"Field": "level", "Op": "=", "Value": "ERROR"}
			filter := json.Query{
				Field: req.Filter.Field,    // 字段名
				Op:    req.Filter.Operand,  // 操作符（=、!=、>、<、>=、<=）
				Value: req.Filter.Value,    // 比较值
			}

			// 【逐行处理 JSON 数据】
			// gjson.ForEachLine 将 JSON 数据按行解析
			// 适用于 JSON Lines 格式（每行一个 JSON 对象）
			gjson.ForEachLine(string(n.Data), func(line gjson.Result) bool {
				// 【查询 JSON 行】
				// QueryJson 应用过滤器和字段选择
				// 返回：
				//   - passedFilter: 是否通过过滤条件
				//   - values: 选择的字段值
				passedFilter, values := json.QueryJson(line.Raw, req.Selections, filter)

				// 如果不通过过滤器，跳过此行
				if !passedFilter {
					return true  // 继续下一行
				}

				// 【添加到结果条带】
				// ToJson 将字段值转换为 JSON 格式
				// 并追加到 stripe.Records 中
				stripe.Records = json.ToJson(stripe.Records, req.Selections, values)

				return true  // 继续下一行
			})

			// 【发送查询结果】
			// 通过 gRPC 流将此文件的查询结果发送给客户端
			err = stream.Send(stripe)
			if err != nil {
				return err
			}
		}

	}

	return nil
}
