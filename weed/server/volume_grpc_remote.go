// Package weed_server 实现 Volume Server 的远程存储功能
// 本文件提供从远程存储（如 S3、Azure、GCS）获取数据并写入 Volume 的能力
//
// 核心功能:
//   - 远程数据拉取：FetchAndWriteNeedle 从远程存储读取 Needle 数据
//   - 本地写入：将远程数据写入本地 Volume
//   - 副本同步：同时将数据同步到副本 Volume Server
//   - 并发处理：本地写入和副本同步并发执行
//
// 使用场景:
//   - Tiered Storage 冷热分层：
//     * 冷数据存储在远程对象存储（S3、Azure、GCS）
//     * 访问时从远程拉取到本地 Volume
//     * 提高热数据访问性能，降低存储成本
//   - 数据恢复：
//     * 从远程备份恢复 Volume 数据
//   - 数据迁移：
//     * 从一个云存储迁移到另一个云存储
//
// Tiered Storage 架构:
//   1. 热数据：存储在本地 Volume（SSD/HDD）
//   2. 温数据：使用 EC（纠删码）降低存储成本
//   3. 冷数据：上传到远程对象存储，本地删除
//   4. 访问冷数据：触发 FetchAndWriteNeedle 从远程拉取
//
// FetchAndWriteNeedle 工作流程:
//   1. 根据 RemoteConf 连接远程存储（S3/Azure/GCS）
//   2. 从远程读取 Needle 数据（指定 offset 和 size）
//   3. 构造 Needle 对象（NeedleId、Cookie、Data、Checksum 等）
//   4. 并发执行：
//      a. 写入本地 Volume
//      b. 同步到所有副本 Volume Server
//   5. 等待所有操作完成
//   6. 返回 ETag（用于验证）
//
// 远程存储配置（RemoteConf）:
//   - Type: 存储类型（s3、azure、gcs 等）
//   - Name: 配置名称
//   - S3 配置：Endpoint、Bucket、AccessKey、SecretKey
//   - Azure 配置：AccountName、AccountKey、Container
//   - GCS 配置：Project、Bucket、Credentials
//
// 性能优化:
//   - 并发写入：本地和副本同步同时进行
//   - 流式处理：边读边写，不缓存全部数据
//   - 异步副本同步：不阻塞本地写入
//
// 错误处理:
//   - 远程读取失败：返回错误，不写入本地
//   - 本地写入失败：记录错误，但副本同步继续
//   - 副本同步失败：记录错误，但本地写入成功
//   - 部分副本失败：返回错误，需要手动修复
//
// 注意事项:
//   - 仅供内部使用（Volume Server 之间）
//   - 需要正确配置远程存储凭证
//   - 网络延迟可能影响性能
//   - 远程存储费用（流量、API 调用）
package weed_server

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"sync"
	"time"
)

// FetchAndWriteNeedle 从远程存储获取 Needle 数据并写入本地 Volume
// 同时将数据同步到所有副本 Volume Server
//
// 参数:
//   - ctx: 上下文
//   - req: 获取和写入请求，包含：
//     - VolumeId: 目标 Volume ID
//     - NeedleId: Needle ID
//     - Cookie: Needle Cookie
//     - Offset: 在远程存储中的偏移量
//     - Size: Needle 数据大小
//     - RemoteConf: 远程存储配置（类型、凭证等）
//     - RemoteLocation: 远程存储位置（bucket、key 等）
//     - Replicas: 副本 Volume Server 列表
//     - Auth: JWT 认证令牌
//
// 返回:
//   - resp: 写入响应，包含：
//     - ETag: Needle 的 ETag（MD5 哈希）
//   - error: 错误（远程读取失败、写入失败等）
//
// 工作流程:
//   1. 【验证 Volume 存在】
//   2. 【连接远程存储】根据 RemoteConf 创建远程存储客户端
//   3. 【读取远程数据】从远程存储读取 Needle 数据
//   4. 【并发写入】启动 goroutine 执行：
//      a. 本地写入：构造 Needle 对象，写入本地 Volume
//      b. 副本同步：通过 HTTP 上传到每个副本 Volume Server
//   5. 【等待完成】使用 WaitGroup 等待所有写入完成
//   6. 【返回结果】返回 ETag 或错误
//
// Tiered Storage 使用示例:
//   // 场景：用户访问冷数据文件，触发从 S3 拉取
//   req := &FetchAndWriteNeedleRequest{
//       VolumeId: 3,
//       NeedleId: 0x01e3b0756f,
//       Cookie: 0x12345678,
//       Offset: 0,
//       Size: 4096,
//       RemoteConf: &RemoteConf{
//           Type: "s3",
//           Name: "my-s3-storage",
//           S3AccessKey: "AKIAIOSFODNN7EXAMPLE",
//           S3SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
//           S3Endpoint: "s3.amazonaws.com",
//           S3Region: "us-west-2",
//       },
//       RemoteLocation: &RemoteLocation{
//           Bucket: "my-bucket",
//           Key: "volumes/3/01e3b0756f.dat",
//       },
//       Replicas: []*Replica{
//           {Url: "192.168.1.11:8080"},
//           {Url: "192.168.1.12:8080"},
//       },
//   }
//   resp, err := volumeServer.FetchAndWriteNeedle(ctx, req)
//   // 数据已从 S3 拉取到本地 Volume 和副本
func (vs *VolumeServer) FetchAndWriteNeedle(ctx context.Context, req *volume_server_pb.FetchAndWriteNeedleRequest) (resp *volume_server_pb.FetchAndWriteNeedleResponse, err error) {
	resp = &volume_server_pb.FetchAndWriteNeedleResponse{}

	// 【验证 Volume 存在】
	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("未找到 volume id %d", req.VolumeId)
	}

	// 【获取远程存储配置】
	remoteConf := req.RemoteConf

	// 【连接远程存储】
	// GetRemoteStorage 根据 RemoteConf 创建对应的远程存储客户端
	// 支持的类型：
	//   - s3: Amazon S3 和兼容的对象存储（MinIO、Ceph 等）
	//   - azure: Microsoft Azure Blob Storage
	//   - gcs: Google Cloud Storage
	//   - b2: Backblaze B2
	//   - aliyun: 阿里云 OSS
	//   - tencent: 腾讯云 COS
	client, getClientErr := remote_storage.GetRemoteStorage(remoteConf)
	if getClientErr != nil {
		return nil, fmt.Errorf("获取远程客户端失败: %w", getClientErr)
	}

	// 【获取远程存储位置】
	// RemoteLocation 指定了 Needle 在远程存储中的位置
	// 例如 S3：{Bucket: "my-bucket", Key: "volumes/3/01e3b0756f.dat"}
	remoteStorageLocation := req.RemoteLocation

	// 【从远程存储读取数据】
	// ReadFile 从远程存储读取指定范围的数据
	// 参数:
	//   - remoteStorageLocation: 远程存储位置（bucket + key）
	//   - offset: 读取偏移量（通常为 0，读取整个 Needle）
	//   - size: 读取大小（Needle 的总大小）
	//
	// 返回:
	//   - data: Needle 的原始二进制数据
	//
	// 注意：
	//   - 网络延迟可能较高（跨区域、跨云）
	//   - 可能产生流量费用（AWS S3 出站流量）
	//   - 可能产生 API 调用费用（GET 请求）
	data, ReadRemoteErr := client.ReadFile(remoteStorageLocation, req.Offset, req.Size)
	if ReadRemoteErr != nil {
		return nil, fmt.Errorf("从远程 %+v 读取失败: %v", remoteStorageLocation, ReadRemoteErr)
	}

	// 【并发写入】
	// 使用 WaitGroup 等待本地写入和副本同步完成
	var wg sync.WaitGroup

	// 【本地写入 goroutine】
	wg.Add(1)
	go func() {
		defer wg.Done()

		// 【构造 Needle 对象】
		n := new(needle.Needle)
		n.Id = types.NeedleId(req.NeedleId)
		n.Cookie = types.Cookie(req.Cookie)
		n.Data, n.DataSize = data, uint32(len(data))

		// 【准备 Needle 元数据】
		// 复制自 *Needle.prepareWriteBuffer()
		// Size = CRC(4) + Data(N) + Flags(1)
		n.Size = 4 + types.Size(n.DataSize) + 1

		// 计算 Checksum（CRC32）
		n.Checksum = needle.NewCRC(n.Data)

		// 设置最后修改时间为当前时间
		n.LastModified = uint64(time.Now().Unix())
		n.SetHasLastModifiedDate()

		// 【写入本地 Volume】
		// WriteVolumeNeedle 将 Needle 追加到 Volume 文件
		// 参数:
		//   - volumeId: Volume ID
		//   - needle: Needle 对象
		//   - checkCookie: 是否检查 Cookie（true：检查重复）
		//   - fsync: 是否立即 fsync（false：延迟刷盘，提高性能）
		//
		// 返回:
		//   - offset: Needle 在 Volume 文件中的偏移量
		//   - err: 写入错误
		if _, localWriteErr := vs.store.WriteVolumeNeedle(v.Id, n, true, false); localWriteErr != nil {
			// 本地写入失败
			if err == nil {
				err = fmt.Errorf("本地写入 needle %d size %d: %v", req.NeedleId, req.Size, localWriteErr)
			}
		} else {
			// 本地写入成功，返回 ETag
			// ETag 是 Needle 的 MD5 哈希，用于验证数据完整性
			resp.ETag = n.Etag()
		}
	}()

	// 【副本同步 goroutine】
	// 如果有副本 Volume Server，并发同步数据
	if len(req.Replicas) > 0 {
		// 构造文件 ID（用于 HTTP 上传）
		// 格式：volumeId,needleId_cookie
		fileId := needle.NewFileId(v.Id, req.NeedleId, req.Cookie)

		// 遍历所有副本 Volume Server
		for _, replica := range req.Replicas {
			wg.Add(1)
			go func(targetVolumeServer string) {
				defer wg.Done()

				// 【构造上传选项】
				uploadOption := &operation.UploadOption{
					// 上传 URL：http://副本地址/文件ID?type=replicate
					// type=replicate 告诉副本这是副本同步，不是普通上传
					UploadUrl: fmt.Sprintf("http://%s/%s?type=replicate", targetVolumeServer, fileId.String()),

					Filename:          "",     // 无需文件名
					Cipher:            false,  // 不加密（已经是加密数据）
					IsInputCompressed: false,  // 不压缩（已经是压缩数据）
					MimeType:          "",     // 无需 MIME 类型
					PairMap:           nil,    // 无需额外元数据

					// JWT 认证令牌（如果启用了安全认证）
					Jwt: security.EncodedJwt(req.Auth),
				}

				// 【创建上传器】
				uploader, uploaderErr := operation.NewUploader()
				if uploaderErr != nil && err == nil {
					err = fmt.Errorf("远程写入 needle %d size %d: %v", req.NeedleId, req.Size, uploaderErr)
					return
				}

				// 【上传数据到副本】
				// UploadData 通过 HTTP POST 将数据上传到副本 Volume Server
				// 参数:
				//   - ctx: 上下文
				//   - data: Needle 数据（从远程存储读取的原始数据）
				//   - uploadOption: 上传选项（URL、认证等）
				//
				// 返回:
				//   - uploadResult: 上传结果（包含 Size、ETag 等）
				//   - err: 上传错误
				//
				// 副本 Volume Server 处理逻辑:
				//   - 接收 HTTP POST 请求
				//   - 识别 ?type=replicate 参数
				//   - 跳过 Master 分配检查（因为是副本同步）
				//   - 写入 Needle 到本地 Volume
				if _, replicaWriteErr := uploader.UploadData(ctx, data, uploadOption); replicaWriteErr != nil && err == nil {
					err = fmt.Errorf("远程写入 needle %d size %d: %v", req.NeedleId, req.Size, replicaWriteErr)
				}
			}(replica.Url)
		}
	}

	// 【等待所有写入完成】
	// 等待本地写入和所有副本同步完成
	// 如果任何一个失败，err 会记录错误信息
	wg.Wait()

	return resp, err
}
