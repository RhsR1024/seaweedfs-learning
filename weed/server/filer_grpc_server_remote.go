// Package weed_server 中的 filer_grpc_server_remote.go 实现远程对象缓存功能
// 将存储在远程对象存储(如 S3、GCS)的文件数据缓存到本地 SeaweedFS 集群
package weed_server

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

// CacheRemoteObjectToLocalCluster 将远程存储的对象缓存到本地 SeaweedFS 集群
// 这通常用于"冷热分层"场景:
//   - 热数据存储在本地 SeaweedFS (快速访问)
//   - 冷数据存储在远程对象存储 (成本低)
//   - 需要访问冷数据时,通过此接口将其缓存到本地
//
// 参数:
//   - req.Directory: 文件所在目录
//   - req.Name: 文件名
// 返回:
//   - 包含更新后的文件 Entry (chunks 指向本地 volume)
func (fs *FilerServer) CacheRemoteObjectToLocalCluster(ctx context.Context, req *filer_pb.CacheRemoteObjectToLocalClusterRequest) (*filer_pb.CacheRemoteObjectToLocalClusterResponse, error) {

	// 【阶段 1: 加载远程存储映射配置】
	// 从 .etc/remote/mounts.json 读取所有远程存储挂载点配置
	mappingEntry, err := fs.filer.FindEntry(ctx, util.JoinPath(filer.DirectoryEtcRemote, filer.REMOTE_STORAGE_MOUNT_FILE))
	if err != nil {
		return nil, err
	}

	// 反序列化映射配置
	// 格式: {"/local/path1": {name: "s3", bucket: "my-bucket", path: "/remote/path1"}, ...}
	mappings, err := filer.UnmarshalRemoteStorageMappings(mappingEntry.Content)
	if err != nil {
		return nil, err
	}

	// 【阶段 2: 查找匹配的远程存储挂载点】
	// 根据请求的目录找到对应的远程存储配置
	var remoteStorageMountedLocation *remote_pb.RemoteStorageLocation
	var localMountedDir string
	for k, loc := range mappings.Mappings {
		// 检查请求目录是否以某个挂载点为前缀
		// 例如: req.Directory="/data/files/a.txt" 匹配 k="/data"
		if strings.HasPrefix(req.Directory, k) {
			localMountedDir, remoteStorageMountedLocation = k, loc
		}
	}

	// 如果没有找到匹配的挂载点,返回错误
	if localMountedDir == "" {
		return nil, fmt.Errorf("%s is not mounted", req.Directory)
	}

	// 【阶段 3: 加载远程存储的访问凭证配置】
	// 从 .etc/remote/<name>.conf 读取 S3/GCS 等的访问密钥配置
	storageConfEntry, err := fs.filer.FindEntry(ctx, util.JoinPath(filer.DirectoryEtcRemote, remoteStorageMountedLocation.Name+filer.REMOTE_STORAGE_CONF_SUFFIX))
	if err != nil {
		return nil, err
	}

	// 反序列化存储凭证配置
	// 包含: access_key、secret_key、endpoint 等
	storageConf := &remote_pb.RemoteConf{}
	if unMarshalErr := proto.Unmarshal(storageConfEntry.Content, storageConf); unMarshalErr != nil {
		return nil, fmt.Errorf("unmarshal remote storage conf %s/%s: %v", filer.DirectoryEtcRemote, remoteStorageMountedLocation.Name+filer.REMOTE_STORAGE_CONF_SUFFIX, unMarshalErr)
	}

	// 【阶段 4: 查找要缓存的文件 Entry】
	entry, err := fs.filer.FindEntry(ctx, util.JoinPath(req.Directory, req.Name))
	if err == filer_pb.ErrNotFound {
		return nil, err
	}

	// 初始化响应对象
	resp := &filer_pb.CacheRemoteObjectToLocalClusterResponse{}

	// 检查文件是否有远程存储信息
	// 如果 entry.Remote 为空或 RemoteSize 为 0,说明文件没有远程数据,无需缓存
	if entry.Remote == nil || entry.Remote.RemoteSize == 0 {
		return resp, nil
	}

	// 【阶段 5: 检测存储选项】
	// 根据目录配置确定副本策略、集合、磁盘类型等
	so, err := fs.detectStorageOption(ctx, req.Directory, "", "", 0, "", "", "", "")
	if err != nil {
		return resp, err
	}
	// 将存储选项转换为 volume 分配请求
	assignRequest, altRequest := so.ToAssignRequests(1)

	// 【阶段 6: 计算合适的分块大小】
	// 目标: 既要减少网络请求次数,又要避免单个请求过大
	chunkSize := int64(5 * 1024 * 1024) // 初始分块大小: 5MB
	chunkCount := entry.Remote.RemoteSize/chunkSize + 1

	// 动态调整分块大小:
	// 如果分块数量超过 1000,则增加分块大小,直到:
	//   1. 分块数量 <= 1000
	//   2. 分块大小达到 MaxMB/2
	for chunkCount > 1000 && chunkSize < int64(fs.option.MaxMB)*1024*1024/2 {
		chunkSize *= 2 // 每次翻倍
		chunkCount = entry.Remote.RemoteSize/chunkSize + 1
	}

	// 【阶段 7: 计算远程对象的完整路径】
	// 将本地路径转换为远程路径
	// 例如: 本地 "/data/files/a.txt", 挂载点 "/data" -> 远程 "/remote/files/a.txt"
	dest := util.FullPath(remoteStorageMountedLocation.Path).Child(string(util.FullPath(req.Directory).Child(req.Name))[len(localMountedDir):])

	// 用于保存新生成的本地 chunks
	var chunks []*filer_pb.FileChunk
	var fetchAndWriteErr error
	var wg sync.WaitGroup

	// 限制并发度为 8,避免过多并发请求导致资源耗尽
	limitedConcurrentExecutor := util.NewLimitedConcurrentExecutor(8)
	// 【阶段 8: 并发缓存所有分块】
	// 遍历文件的所有分块,每个分块独立缓存
	for offset := int64(0); offset < entry.Remote.RemoteSize; offset += chunkSize {
		localOffset := offset // 闭包变量,保存当前分块的起始偏移

		wg.Add(1)
		// 在并发执行器中处理每个分块
		limitedConcurrentExecutor.Execute(func() {
			defer wg.Done()

			// 计算当前分块的实际大小
			size := chunkSize
			if localOffset+chunkSize > entry.Remote.RemoteSize {
				// 最后一个分块可能小于 chunkSize
				size = entry.Remote.RemoteSize - localOffset
			}

			// 【步骤 8.1: 从 Master 分配一个 volume】
			// 每个分块需要一个独立的 fid
			assignResult, err := operation.Assign(ctx, fs.filer.GetMaster, fs.grpcDialOption, assignRequest, altRequest)
			if err != nil {
				fetchAndWriteErr = err
				return
			}
			if assignResult.Error != "" {
				fetchAndWriteErr = fmt.Errorf("assign: %v", assignResult.Error)
				return
			}

			// 【步骤 8.2: 解析分配的 file ID】
			fileId, parseErr := needle.ParseFileIdFromString(assignResult.Fid)
			if assignResult.Error != "" {
				fetchAndWriteErr = fmt.Errorf("unrecognized file id %s: %v", assignResult.Fid, parseErr)
				return
			}

			// 【步骤 8.3: 构建副本列表】
			// 如果有副本,volume server 会同步写入所有副本
			var replicas []*volume_server_pb.FetchAndWriteNeedleRequest_Replica
			for _, r := range assignResult.Replicas {
				replicas = append(replicas, &volume_server_pb.FetchAndWriteNeedleRequest_Replica{
					Url:       r.Url,       // 副本 volume server 地址
					PublicUrl: r.PublicUrl, // 公网地址
					GrpcPort:  int32(r.GrpcPort), // gRPC 端口
				})
			}

			// 【步骤 8.4: 调用 Volume Server 的 FetchAndWriteNeedle 接口】
			// Volume Server 会:
			//   1. 从远程对象存储下载指定范围的数据
			//   2. 将数据写入 Needle
			//   3. 同步到所有副本
			assignedServerAddress := pb.NewServerAddressWithGrpcPort(assignResult.Url, assignResult.GrpcPort)
			var etag string
			err = operation.WithVolumeServerClient(false, assignedServerAddress, fs.grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
				resp, fetchAndWriteErr := volumeServerClient.FetchAndWriteNeedle(context.Background(), &volume_server_pb.FetchAndWriteNeedleRequest{
					VolumeId:   uint32(fileId.VolumeId), // 目标 volume ID
					NeedleId:   uint64(fileId.Key),      // 目标 needle ID
					Cookie:     uint32(fileId.Cookie),   // cookie
					Offset:     localOffset,             // 远程对象的起始偏移
					Size:       size,                    // 要下载的数据大小
					Replicas:   replicas,                // 副本列表
					Auth:       string(assignResult.Auth), // 鉴权 token
					RemoteConf: storageConf,             // 远程存储访问凭证
					RemoteLocation: &remote_pb.RemoteStorageLocation{
						Name:   remoteStorageMountedLocation.Name,   // 存储名称(如 "s3")
						Bucket: remoteStorageMountedLocation.Bucket, // 存储桶名称
						Path:   string(dest),                        // 远程对象路径
					},
				})
				if fetchAndWriteErr != nil {
					return fmt.Errorf("volume server %s fetchAndWrite %s: %v", assignResult.Url, dest, fetchAndWriteErr)
				} else {
					// 保存 ETag 用于数据完整性校验
					etag = resp.ETag
				}
				return nil
			})

			if err != nil && fetchAndWriteErr == nil {
				fetchAndWriteErr = err
				return
			}

			// 【步骤 8.5: 记录新生成的 chunk 信息】
			chunks = append(chunks, &filer_pb.FileChunk{
				FileId:       assignResult.Fid,      // 完整的 file ID 字符串
				Offset:       localOffset,           // 在原文件中的偏移
				Size:         uint64(size),          // chunk 大小
				ModifiedTsNs: time.Now().UnixNano(), // 修改时间戳
				ETag:         etag,                  // 数据完整性标识
				Fid: &filer_pb.FileId{
					VolumeId: uint32(fileId.VolumeId), // volume ID
					FileKey:  uint64(fileId.Key),      // file key
					Cookie:   uint32(fileId.Cookie),   // cookie
				},
			})
		})
	}

	// 【阶段 9: 等待所有分块缓存完成】
	wg.Wait()
	if fetchAndWriteErr != nil {
		return nil, fetchAndWriteErr
	}

	// 【阶段 10: 更新文件 Entry】
	// 保存旧的 chunks (指向远程存储),稍后删除
	garbage := entry.GetChunks()

	// 浅拷贝 Entry,更新 chunks 和远程同步时间戳
	newEntry := entry.ShallowClone()
	newEntry.Chunks = chunks // 使用新的本地 chunks
	newEntry.Remote = proto.Clone(entry.Remote).(*filer_pb.RemoteEntry)
	newEntry.Remote.LastLocalSyncTsNs = time.Now().UnixNano() // 记录同步时间

	// 【阶段 11: 更新 Filer Store 中的 Entry】
	// 注意: 此处跳过 meta data log events,避免触发事件通知
	if err := fs.filer.Store.UpdateEntry(context.Background(), newEntry); err != nil {
		// 更新失败,删除已经写入的 chunks
		fs.filer.DeleteUncommittedChunks(ctx, chunks)
		return nil, err
	}

	// 【阶段 12: 删除旧的 chunks】
	// 旧 chunks 可能是空的(如果之前全在远程存储)
	fs.filer.DeleteChunks(ctx, entry.FullPath, garbage)

	// 【阶段 13: 通知订阅者文件已更新】
	// 通知其他 Filer 节点、mount clients 等
	fs.filer.NotifyUpdateEvent(ctx, entry, newEntry, true, false, nil)

	// 返回更新后的 Entry
	resp.Entry = newEntry.ToProtoEntry()

	return resp, nil

}
