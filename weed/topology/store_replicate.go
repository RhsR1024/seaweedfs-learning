// Package topology 提供 SeaweedFS 的拓扑管理功能
// 本文件实现了数据副本的复制逻辑，确保数据的高可用性和可靠性
//
// 主要功能:
//  1. ReplicatedWrite: 将数据写入主副本和备副本
//  2. ReplicatedDelete: 删除主副本和备副本的数据
//  3. DistributedOperation: 并发执行分布式操作
//  4. GetWritableRemoteReplications: 获取可写的远程副本位置
//
// 副本策略:
//   SeaweedFS 支持多种副本策略（如 000, 001, 010, 100, 110 等）
//   - 第一位: 同数据中心的副本数
//   - 第二位: 不同机架的副本数
//   - 第三位: 不同数据中心的副本数
//   例如: "001" 表示 1 个主副本 + 1 个不同数据中心的副本 = 总共 2 个副本
package topology

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/buffer_pool"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

// ReplicatedWrite 复制写入数据到本地和所有副本
// 这是 SeaweedFS 保证数据可靠性的核心方法
//
// 工作流程:
//  1. 验证 JWT 令牌（如果启用安全认证）
//  2. 判断是原始请求还是副本复制请求
//  3. 如果是原始请求，查询需要复制到的远程 Volume Server 列表
//  4. 写入本地 Volume（主副本）
//  5. 并发写入所有远程副本
//  6. 等待所有副本写入完成
//
// 参数:
//   - ctx: 上下文，用于超时控制和请求追踪
//   - masterFn: 获取 Master Server 地址的函数
//   - grpcDialOption: gRPC 连接选项
//   - s: Store 对象，管理本地 Volume
//   - volumeId: 要写入的 Volume ID
//   - n: Needle 对象，包含要写入的数据和元数据
//   - r: 原始 HTTP 请求（包含查询参数，如 type=replicate）
//   - contentMd5: 内容的 MD5 校验值
//
// 返回值:
//   - isUnchanged: 如果文件内容未改变（幂等写入），返回 true
//   - err: 错误信息（如果有）
//
// 副本复制机制:
//   - 当 type=replicate 时，表示这是副本复制请求，不再进行二次复制
//   - 主副本写入成功后，才会写入备副本
//   - 所有副本写入操作并发执行，提高性能
//
// 错误处理:
//   - 如果本地写入失败，直接返回错误
//   - 如果远程副本写入失败，也会返回错误（保证一致性）
func ReplicatedWrite(ctx context.Context, masterFn operation.GetMasterFn, grpcDialOption grpc.DialOption, s *storage.Store, volumeId needle.VolumeId, n *needle.Needle, r *http.Request, contentMd5 string) (isUnchanged bool, err error) {

	// 步骤 1: 从请求中提取 JWT 令牌（用于副本服务器的身份验证）
	//check JWT
	jwt := security.GetJwt(r)

	// 步骤 2: 检查是否是副本复制请求
	// check whether this is a replicated write request
	var remoteLocations []operation.Location
	if r.FormValue("type") != "replicate" {
		// 这是原始上传请求（不是副本复制）
		// this is the initial request
		// 获取需要复制到的远程副本位置列表
		remoteLocations, err = GetWritableRemoteReplications(s, grpcDialOption, volumeId, masterFn)
		if err != nil {
			glog.V(0).Infoln(err)
			return
		}
	}

	// 步骤 3: 读取 fsync 参数
	// fsync=true 表示需要强制刷新到磁盘（确保数据持久化）
	// read fsync value
	fsync := false
	if r.FormValue("fsync") == "true" {
		fsync = true
	}

	// 步骤 4: 写入本地 Volume（主副本）
	if s.GetVolume(volumeId) != nil {
		start := time.Now()

		// 记录并发写入请求数（监控指标）
		inFlightGauge := stats.VolumeServerInFlightRequestsGauge.WithLabelValues(stats.WriteToLocalDisk)
		inFlightGauge.Inc()
		defer inFlightGauge.Dec()

		// 执行实际的写入操作
		// checkCookie=true: 验证 Needle 的 Cookie 值，防止覆盖错误的数据
		isUnchanged, err = s.WriteVolumeNeedle(volumeId, n, true, fsync)
		// 记录写入耗时（监控指标）
		stats.VolumeServerRequestHistogram.WithLabelValues(stats.WriteToLocalDisk).Observe(time.Since(start).Seconds())
		if err != nil {
			// 本地写入失败，记录错误并返回
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorWriteToLocalDisk).Inc()
			err = fmt.Errorf("failed to write to local disk: %w", err)
			glog.V(0).Infoln(err)
			return
		}
	}

	// 步骤 5: 写入远程副本（如果有）
	if len(remoteLocations) > 0 { //send to other replica locations
		start := time.Now()

		// 记录副本写入并发请求数
		inFlightGauge := stats.VolumeServerInFlightRequestsGauge.WithLabelValues(stats.WriteToReplicas)
		inFlightGauge.Inc()
		defer inFlightGauge.Dec()

		// 并发写入所有远程副本
		err = DistributedOperation(remoteLocations, func(location operation.Location) error {
			// 构造副本服务器的 URL
			u := url.URL{
				Scheme: "http",
				Host:   location.Url,
				Path:   r.URL.Path, // 保持相同的 volumeId 和 fileId
			}
			// 设置查询参数
			q := url.Values{
				"type": {"replicate"}, // 标记为副本复制请求，避免二次复制
				"ttl":  {n.Ttl.String()}, // Time To Live
			}
			if n.LastModified > 0 {
				q.Set("ts", strconv.FormatUint(n.LastModified, 10)) // 时间戳
			}
			if n.IsChunkedManifest() {
				q.Set("cm", "true") // Chunk Manifest 标志
			}
			u.RawQuery = q.Encode()

			// 构造自定义 HTTP 头（Pairs）
			pairMap := make(map[string]string)
			if n.HasPairs() {
				tmpMap := make(map[string]string)
				err := json.Unmarshal(n.Pairs, &tmpMap)
				if err != nil {
					stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorUnmarshalPairs).Inc()
					glog.V(0).Infoln("Unmarshal pairs error:", err)
				}
				// 添加 Pair 名称前缀
				for k, v := range tmpMap {
					pairMap[needle.PairNamePrefix+k] = v
				}
			}

			// 从缓冲池获取临时缓冲区
			bytesBuffer := buffer_pool.SyncPoolGetBuffer()
			defer buffer_pool.SyncPoolPutBuffer(bytesBuffer)

			// volume server do not know about encryption
			// TODO optimize here to compress data only once
			// Volume Server 不处理加密
			// TODO: 优化这里，避免重复压缩数据
			uploadOption := &operation.UploadOption{
				UploadUrl:         u.String(),
				Filename:          string(n.Name),
				Cipher:            false,           // Volume Server 不加密
				IsInputCompressed: n.IsCompressed(), // 传递压缩状态
				MimeType:          string(n.Mime),
				PairMap:           pairMap,
				Jwt:               jwt, // 使用原始请求的 JWT
				Md5:               contentMd5,
				BytesBuffer:       bytesBuffer, // 复用缓冲区
			}

			// 创建上传器并上传数据到副本服务器
			uploader, err := operation.NewUploader()
			if err != nil {
				glog.Errorf("replication-UploadData, err:%v, url:%s", err, u.String())
				return err
			}
			_, err = uploader.UploadData(ctx, n.Data, uploadOption)
			if err != nil {
				glog.Errorf("replication-UploadData, err:%v, url:%s", err, u.String())
			}
			return err
		})
		// 记录副本写入总耗时
		stats.VolumeServerRequestHistogram.WithLabelValues(stats.WriteToReplicas).Observe(time.Since(start).Seconds())
		if err != nil {
			// 副本写入失败，返回错误
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorWriteToReplicas).Inc()
			err = fmt.Errorf("failed to write to replicas for volume %d: %v", volumeId, err)
			glog.V(0).Infoln(err)
			return false, err
		}
	}
	return
}

// ReplicatedDelete 复制删除数据，删除本地和所有副本
// 与 ReplicatedWrite 类似，确保数据在所有副本上一致删除
//
// 工作流程:
//  1. 验证 JWT 令牌
//  2. 判断是原始删除请求还是副本复制删除请求
//  3. 如果是原始请求，查询所有副本位置
//  4. 删除本地 Volume 中的数据
//  5. 并发删除所有远程副本的数据
//
// 参数:
//   - masterFn: 获取 Master Server 地址的函数
//   - grpcDialOption: gRPC 连接选项
//   - store: Store 对象，管理本地 Volume
//   - volumeId: Volume ID
//   - n: 要删除的 Needle 对象（包含 fileId 和 cookie）
//   - r: 原始 HTTP 请求
//
// 返回值:
//   - size: 删除的数据大小
//   - err: 错误信息（如果有）
//
// 注意:
//   - 删除是逻辑删除，实际数据可能仍存在于磁盘上
//   - 通过 Compaction 可以回收被删除数据占用的空间
func ReplicatedDelete(masterFn operation.GetMasterFn, grpcDialOption grpc.DialOption, store *storage.Store, volumeId needle.VolumeId, n *needle.Needle, r *http.Request) (size types.Size, err error) {

	// 步骤 1: 验证 JWT 令牌
	//check JWT
	jwt := security.GetJwt(r)

	// 步骤 2: 检查是否是副本复制删除请求
	var remoteLocations []operation.Location
	if r.FormValue("type") != "replicate" {
		// 原始删除请求，获取所有副本位置
		remoteLocations, err = GetWritableRemoteReplications(store, grpcDialOption, volumeId, masterFn)
		if err != nil {
			glog.V(0).Infoln(err)
			return
		}
	}

	// 步骤 3: 删除本地 Volume 中的数据
	size, err = store.DeleteVolumeNeedle(volumeId, n)
	if err != nil {
		glog.V(0).Infoln("delete error:", err)
		return
	}

	// 步骤 4: 并发删除所有远程副本
	if len(remoteLocations) > 0 { //send to other replica locations
		// 使用 HTTP DELETE 请求删除远程副本
		// 添加 type=replicate 参数，避免远程服务器再次触发副本删除
		if err = DistributedOperation(remoteLocations, func(location operation.Location) error {
			return util_http.Delete("http://"+location.Url+r.URL.Path+"?type=replicate", string(jwt))
		}); err != nil {
			// 如果任何副本删除失败，将 size 设置为 0（表示删除不完整）
			size = 0
		}
	}
	return
}

// DistributedOperationResult 分布式操作的结果集合
// 记录每个远程位置的操作结果（成功或错误）
type DistributedOperationResult map[string]error

// Error 将所有错误合并为一个错误消息
// 如果所有操作都成功，返回 nil
//
// 返回值:
//   - error: 合并后的错误消息，格式为 "[host1]: error1\n[host2]: error2"
func (dr DistributedOperationResult) Error() error {
	var errs []string
	// 遍历所有结果，收集错误信息
	for k, v := range dr {
		if v != nil {
			errs = append(errs, fmt.Sprintf("[%s]: %v", k, v))
		}
	}
	// 如果没有错误，返回 nil
	if len(errs) == 0 {
		return nil
	}
	// 合并所有错误消息，用换行符分隔
	return errors.New(strings.Join(errs, "\n"))
}

// RemoteResult 单个远程操作的结果
type RemoteResult struct {
	Host  string // 远程主机地址
	Error error  // 操作错误（nil 表示成功）
}

// DistributedOperation 并发执行分布式操作
// 对每个远程位置并发执行指定的操作函数，等待所有操作完成
//
// 参数:
//   - locations: 远程位置列表（Volume Server 地址）
//   - op: 要执行的操作函数，接收一个位置，返回错误
//
// 返回值:
//   - error: 如果任何操作失败，返回合并后的错误；全部成功返回 nil
//
// 工作原理:
//  1. 为每个位置启动一个 goroutine
//  2. 每个 goroutine 执行 op 函数，并将结果发送到 channel
//  3. 主 goroutine 等待所有结果
//  4. 合并所有错误并返回
//
// 优点:
//   - 并发执行，提高性能
//   - 即使部分操作失败，也会等待所有操作完成
//   - 返回详细的错误信息，包含每个失败的主机
func DistributedOperation(locations []operation.Location, op func(location operation.Location) error) error {
	length := len(locations)
	results := make(chan RemoteResult)

	// 为每个位置启动一个 goroutine 并发执行操作
	for _, location := range locations {
		go func(location operation.Location, results chan RemoteResult) {
			// 执行操作并发送结果
			results <- RemoteResult{location.Url, op(location)}
		}(location, results)
	}

	// 收集所有结果
	ret := DistributedOperationResult(make(map[string]error))
	for i := 0; i < length; i++ {
		result := <-results
		ret[result.Host] = result.Error
	}

	// 检查是否有错误
	return ret.Error()
}

// GetWritableRemoteReplications 获取可写的远程副本位置列表
// 根据 Volume 的副本策略，从 Master 查询其他副本的位置
//
// 参数:
//   - s: Store 对象
//   - grpcDialOption: gRPC 连接选项
//   - volumeId: Volume ID
//   - masterFn: 获取 Master Server 地址的函数
//
// 返回值:
//   - remoteLocations: 远程副本位置列表（不包含本地位置）
//   - err: 错误信息
//
// 工作原理:
//  1. 检查本地 Volume 是否存在
//  2. 如果副本数为 1（无副本），直接返回空列表
//  3. 通过 Master 查询该 Volume 的所有位置
//  4. 过滤掉本地位置，返回远程位置列表
//  5. 验证副本数是否符合预期
//
// 特殊情况:
//   - 如果本地没有该 Volume，也会查询远程位置（用于纯副本场景）
//   - 如果副本数不足，会记录错误但仍返回现有副本列表
func GetWritableRemoteReplications(s *storage.Store, grpcDialOption grpc.DialOption, volumeId needle.VolumeId, masterFn operation.GetMasterFn) (remoteLocations []operation.Location, err error) {

	// 获取本地 Volume
	v := s.GetVolume(volumeId)
	if v != nil && v.ReplicaPlacement.GetCopyCount() == 1 {
		// 副本数为 1（没有副本），无需复制
		return
	}

	// not on local store, or has replications
	// 不在本地存储，或者有副本配置
	// 通过 Master 查询该 Volume 的所有位置
	lookupResult, lookupErr := operation.LookupVolumeId(masterFn, grpcDialOption, volumeId.String())
	if lookupErr == nil {
		// 构造本地地址（IP:Port）
		selfUrl := util.JoinHostPort(s.Ip, s.Port)
		// 过滤出远程位置（排除本地）
		for _, location := range lookupResult.Locations {
			if location.Url != selfUrl {
				remoteLocations = append(remoteLocations, location)
			}
		}
	} else {
		// 查询失败
		err = fmt.Errorf("replicating lookup failed for %d: %v", volumeId, lookupErr)
		return
	}

	// 验证副本数是否正确
	if v != nil {
		// has one local and has remote replications
		// 有本地副本和远程副本
		copyCount := v.ReplicaPlacement.GetCopyCount()
		if len(lookupResult.Locations) < copyCount {
			// 实际副本数少于配置的副本数，记录错误
			err = fmt.Errorf("replicating operations [%d] is less than volume %d replication copy count [%d]",
				len(lookupResult.Locations), volumeId, copyCount)
		}
	}

	return
}
