// Package weed_server 中的 filer_grpc_server_sub_meta.go 实现元数据订阅功能
// 允许客户端实时订阅 Filer 元数据变更事件,支持本地和聚合两种订阅模式
package weed_server

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/stats"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

const (
	// MaxUnsyncedEvents 定义过滤事件数量阈值
	// 当过滤掉的事件超过此数量时,发送一个空通知包含时间戳
	// 这样订阅客户端可以知道当前的同步进度,即使没有实际数据更新
	MaxUnsyncedEvents = 1e3
)

// SubscribeMetadata 聚合订阅元数据变更
// 此函数订阅跨集群聚合的元数据事件,适用于多 Filer 集群同步场景
//
// 参数:
//   - req: 订阅请求,包含客户端信息、起始时间戳、路径前缀等
//   - stream: 用于推送事件的流式响应
// 返回:
//   - error: 订阅过程中的错误
//
// 工作流程:
//   1. 注册客户端并检测重复订阅
//   2. 从持久化日志读取历史事件
//   3. 从内存日志读取最新事件
//   4. 循环重复步骤 2-3 直到客户端断开或达到 UntilNs
func (fs *FilerServer) SubscribeMetadata(req *filer_pb.SubscribeMetadataRequest, stream filer_pb.SeaweedFiler_SubscribeMetadataServer) error {

	// 获取流的上下文,用于检测客户端断开
	ctx := stream.Context()
	// 解析客户端地址,用于日志和监控
	peerAddress := findClientAddress(ctx, 0)

	// 【步骤 1: 注册客户端】
	// 将客户端添加到已知订阅者列表
	// 返回值:
	//   - isReplacing: 是否替换了旧的订阅(同 ClientId 但 Epoch 更小)
	//   - alreadyKnown: 是否已存在相同的订阅(ClientId 和 Epoch 都相同)
	//   - clientName: 客户端标识字符串(ClientName@PeerAddress)
	isReplacing, alreadyKnown, clientName := fs.addClient("", req.ClientName, peerAddress, req.ClientId, req.ClientEpoch)

	if isReplacing {
		// 通知其他等待中的订阅者,有客户端被替换了
		fs.filer.MetaAggregator.ListenersCond.Broadcast()
	} else if alreadyKnown {
		// 检测到重复订阅,通知其他订阅者并返回错误
		fs.filer.MetaAggregator.ListenersCond.Broadcast()
		return fmt.Errorf("duplicated subscription detected for client %s id %d", clientName, req.ClientId)
	}

	// 【清理逻辑】当函数返回时(正常或异常),删除客户端注册
	defer func() {
		glog.V(0).Infof("disconnect %v subscriber %s clientId:%d", clientName, req.PathPrefix, req.ClientId)
		fs.deleteClient("", clientName, req.ClientId, req.ClientEpoch)
		// 通知其他等待中的订阅者,有客户端断开了
		fs.filer.MetaAggregator.ListenersCond.Broadcast()
	}()

	// 【步骤 2: 初始化读取位置】
	// 从请求中的起始时间戳创建日志读取位置
	// -2 表示从该时间戳之后的第一条记录开始读取
	lastReadTime := log_buffer.NewMessagePosition(req.SinceNs, -2)
	glog.V(0).Infof(" %v starts to subscribe %s from %+v", clientName, req.PathPrefix, lastReadTime)

	// 【步骤 3: 构建事件处理函数链】
	// eachEventNotificationFn: 处理单个事件通知的函数,包含路径过滤和流发送逻辑
	eachEventNotificationFn := fs.eachEventNotificationFn(req, stream, clientName)

	// eachLogEntryFn: 包装事件处理函数,用于处理日志条目(反序列化后调用 eachEventNotificationFn)
	eachLogEntryFn := eachLogEntryFn(eachEventNotificationFn)

	// 状态变量
	var processedTsNs int64        // 已处理的最新时间戳
	var readPersistedLogErr error  // 持久化日志读取错误
	var readInMemoryLogErr error   // 内存日志读取错误
	var isDone bool                // 是否已完成订阅(达到 UntilNs)

	// 【主循环】持续读取和推送事件
	for {

		// 【步骤 4: 读取持久化日志】
		// 从磁盘上的日志文件读取历史事件
		glog.V(4).Infof("read on disk %v aggregated subscribe %s from %+v", clientName, req.PathPrefix, lastReadTime)

		processedTsNs, isDone, readPersistedLogErr = fs.filer.ReadPersistedLogBuffer(lastReadTime, req.UntilNs, eachLogEntryFn)
		if readPersistedLogErr != nil {
			return fmt.Errorf("reading from persisted logs: %w", readPersistedLogErr)
		}
		// 如果已达到 UntilNs,订阅完成
		if isDone {
			return nil
		}

		// 【步骤 5: 更新读取位置】
		glog.V(4).Infof("processed to %v: %v", clientName, processedTsNs)
		if processedTsNs != 0 {
			// 如果处理了事件,更新读取位置到已处理的时间戳
			lastReadTime = log_buffer.NewMessagePosition(processedTsNs, -2)
		} else {
			// 【情况 1】磁盘上没有找到数据
			// 检查之前是否从内存日志得到 ResumeFromDiskError,这表示存在时间间隙
			if errors.Is(readInMemoryLogErr, log_buffer.ResumeFromDiskError) {
				// 存在间隙: 请求时间 < 最早内存时间,但磁盘上没有数据
				// 跳过间隙,前进到最早内存时间,避免无限循环
				earliestTime := fs.filer.MetaAggregator.MetaLogBuffer.GetEarliestTime()
				if !earliestTime.IsZero() && earliestTime.After(lastReadTime.Time) {
					glog.V(3).Infof("gap detected: skipping from %v to earliest memory time %v for %v",
						lastReadTime.Time, earliestTime, clientName)
					// 定位到最早时间;基于时间的读取器会包含它
					lastReadTime = log_buffer.NewMessagePosition(earliestTime.UnixNano(), -2)
					readInMemoryLogErr = nil // 清除错误,因为我们跳过了间隙
				}
			} else {
				// 【情况 2】首次读取或尚未遇到 ResumeFromDiskError
				// 检查下一天是否有日志文件
				// SeaweedFS 按天组织日志文件,所以需要跳到下一天检查
				nextDayTs := util.GetNextDayTsNano(lastReadTime.Time.UnixNano())
				position := log_buffer.NewMessagePosition(nextDayTs, -2)
				found, err := fs.filer.HasPersistedLogFiles(position)
				if err != nil {
					return fmt.Errorf("checking persisted log files: %w", err)
				}
				if found {
					// 找到下一天的日志文件,更新读取位置
					lastReadTime = position
				}
			}
		}

		// 【步骤 6: 读取内存日志】
		// 从内存缓冲区读取最新事件(尚未持久化到磁盘的)
		glog.V(4).Infof("read in memory %v aggregated subscribe %s from %+v", clientName, req.PathPrefix, lastReadTime)

		lastReadTime, isDone, readInMemoryLogErr = fs.filer.MetaAggregator.MetaLogBuffer.LoopProcessLogData("aggMeta:"+clientName, lastReadTime, req.UntilNs, func() bool {
			// 【等待回调】当内存日志暂时没有新数据时,此函数被调用
			// 返回 true 继续等待,返回 false 退出循环

			// 检查客户端是否已断开连接
			select {
			case <-ctx.Done():
				// 上下文取消,客户端已断开
				return false
			default:
			}

			// 等待新的事件到达
			// 使用条件变量实现高效的事件驱动等待,避免轮询
			fs.filer.MetaAggregator.ListenersLock.Lock()
			atomic.AddInt64(&fs.filer.MetaAggregator.ListenersWaits, 1) // 增加等待计数
			fs.filer.MetaAggregator.ListenersCond.Wait()                // 阻塞等待通知
			atomic.AddInt64(&fs.filer.MetaAggregator.ListenersWaits, -1) // 减少等待计数
			fs.filer.MetaAggregator.ListenersLock.Unlock()

			// 检查客户端是否仍然有效
			return fs.hasClient(req.ClientId, req.ClientEpoch)
		}, eachLogEntryFn)

		// 【步骤 7: 处理内存日志读取结果】
		if readInMemoryLogErr != nil {
			if errors.Is(readInMemoryLogErr, log_buffer.ResumeFromDiskError) {
				// 内存日志说数据太旧 - 下次迭代将从磁盘读取
				// 但如果磁盘也没有数据(历史间隙),我们会向前跳过
				continue
			}
			glog.Errorf("processed to %v: %v", lastReadTime, readInMemoryLogErr)
			if !errors.Is(readInMemoryLogErr, log_buffer.ResumeError) {
				// 非 ResumeError 的错误,退出循环
				break
			}
		}

		// 检查是否已完成订阅
		if isDone {
			return nil
		}

		// 检查客户端是否仍然连接
		if !fs.hasClient(req.ClientId, req.ClientEpoch) {
			glog.V(0).Infof("client %v is closed", clientName)
			return nil
		}

		// 【睡眠】避免在没有新数据时过于频繁循环
		// 使用质数(1127ms)避免与其他定时任务产生共振
		time.Sleep(1127 * time.Millisecond)
	}

	return readInMemoryLogErr

}

// SubscribeLocalMetadata 订阅本地 Filer 的元数据变更
// 与 SubscribeMetadata 的区别:
//   - SubscribeMetadata: 订阅聚合的元数据（来自所有 Filer 的变更）
//   - SubscribeLocalMetadata: 只订阅本地 Filer 的元数据变更
//
// 使用场景:
//   - 本地缓存同步: 只需要监听本地 Filer 的变更
//   - 本地备份: 只备份本地 Filer 管理的文件
//   - 本地索引: 只索引本地 Filer 的元数据
//
// 工作流程:
//   1. 注册客户端（使用负数 clientId 与聚合订阅区分）
//   2. 从持久化日志读取历史事件
//   3. 从内存日志读取最新事件
//   4. 循环重复步骤 2-3 直到客户端断开或达到 UntilNs
func (fs *FilerServer) SubscribeLocalMetadata(req *filer_pb.SubscribeMetadataRequest, stream filer_pb.SeaweedFiler_SubscribeLocalMetadataServer) error {

	// 【步骤 1: 初始化客户端信息】
	ctx := stream.Context()
	peerAddress := findClientAddress(ctx, 0)

	// 【重要】使用负数 client ID 来区分本地订阅和聚合订阅
	// 这样在 addClient/deleteClient/hasClient 中可以区分两种订阅类型
	// 避免本地订阅和聚合订阅的 client ID 冲突
	req.ClientId = -req.ClientId

	// 【步骤 2: 注册客户端】
	isReplacing, alreadyKnown, clientName := fs.addClient("local", req.ClientName, peerAddress, req.ClientId, req.ClientEpoch)
	if isReplacing {
		// 替换了旧的订阅，通知所有等待中的订阅者
		fs.listenersCond.Broadcast()
	} else if alreadyKnown {
		// 检测到重复订阅，返回错误
		return fmt.Errorf("duplicated local subscription detected for client %s clientId:%d", clientName, req.ClientId)
	}

	// 【步骤 3: 设置清理函数】
	// 当函数返回时（无论正常返回还是错误返回），执行清理
	defer func() {
		glog.V(0).Infof("disconnect %v local subscriber %s clientId:%d", clientName, req.PathPrefix, req.ClientId)
		fs.deleteClient("local", clientName, req.ClientId, req.ClientEpoch)
		fs.listenersCond.Broadcast() // 通知其他等待的订阅者
	}()

	// 【步骤 4: 初始化读取位置】
	lastReadTime := log_buffer.NewMessagePosition(req.SinceNs, -2)
	glog.V(0).Infof(" + %v local subscribe %s from %+v clientId:%d", clientName, req.PathPrefix, lastReadTime, req.ClientId)

	// 【步骤 5: 创建事件处理函数】
	// eachEventNotificationFn 负责过滤和发送事件到客户端
	eachEventNotificationFn := fs.eachEventNotificationFn(req, stream, clientName)

	// eachLogEntryFn 将 LogEntry 转换为 EventNotification
	eachLogEntryFn := eachLogEntryFn(eachEventNotificationFn)

	// 【步骤 6: 初始化循环变量】
	var processedTsNs int64           // 持久化日志处理到的时间戳
	var readPersistedLogErr error     // 持久化日志读取错误
	var readInMemoryLogErr error      // 内存日志读取错误
	var isDone bool                   // 是否已完成订阅
	var lastCheckedFlushTsNs int64 = -1 // 上次检查的刷盘时间戳
	var lastDiskReadTsNs int64 = -1     // 上次磁盘读取的位置

	// 【主循环】持续读取和推送事件
	for {
		// 【步骤 7: 判断是否需要从磁盘读取】
		// 需要从磁盘读取的情况:
		//   1. 首次进入循环（lastCheckedFlushTsNs == -1）
		//   2. 有新数据刷盘（currentFlushTsNs > lastCheckedFlushTsNs）
		//   3. 读取位置前进（currentReadTsNs > lastDiskReadTsNs）
		//      这种情况表示正在追赶积压的数据
		currentFlushTsNs := fs.filer.LocalMetaLogBuffer.GetLastFlushTsNs()
		currentReadTsNs := lastReadTime.Time.UnixNano()
		shouldReadFromDisk := lastCheckedFlushTsNs == -1 ||
			currentFlushTsNs > lastCheckedFlushTsNs ||
			currentReadTsNs > lastDiskReadTsNs

		if shouldReadFromDisk {
			// 【步骤 8: 从持久化日志读取】
			// 记录本次磁盘读取的位置
			lastDiskReadTsNs = currentReadTsNs
			glog.V(4).Infof("read on disk %v local subscribe %s from %+v (lastFlushed: %v)", clientName, req.PathPrefix, lastReadTime, time.Unix(0, currentFlushTsNs))

			// 调用 Filer 的持久化日志读取接口
			processedTsNs, isDone, readPersistedLogErr = fs.filer.ReadPersistedLogBuffer(lastReadTime, req.UntilNs, eachLogEntryFn)
			if readPersistedLogErr != nil {
				glog.V(0).Infof("read on disk %v local subscribe %s from %+v: %v", clientName, req.PathPrefix, lastReadTime, readPersistedLogErr)
				return fmt.Errorf("reading from persisted logs: %w", readPersistedLogErr)
			}

			// 如果已经读到 UntilNs，订阅完成
			if isDone {
				return nil
			}

			// 【步骤 9: 更新状态】
			// 更新上次检查的刷盘时间
			lastCheckedFlushTsNs = currentFlushTsNs

			if processedTsNs != 0 {
				// 持久化日志有数据，更新读取位置
				lastReadTime = log_buffer.NewMessagePosition(processedTsNs, -2)
			} else {
				// 【步骤 10: 处理磁盘无数据的情况】
				// 磁盘上没有找到数据，可能有两种情况:
				//   1. 间隙 (gap): 请求的时间在内存最早时间之前，但磁盘上没有数据
				//   2. 首次读取或等待新数据

				if readInMemoryLogErr == log_buffer.ResumeFromDiskError {
					// 【情况 1: 检测到间隙】
					// 内存日志说请求的时间太旧，但磁盘上也没有数据
					// 这说明存在数据间隙（可能是日志被清理或轮转）
					earliestTime := fs.filer.LocalMetaLogBuffer.GetEarliestTime()
					if !earliestTime.IsZero() && earliestTime.After(lastReadTime.Time) {
						glog.V(3).Infof("gap detected: skipping from %v to earliest memory time %v for %v",
							lastReadTime.Time, earliestTime, clientName)
						// 跳过间隙，直接定位到内存日志的最早时间
						lastReadTime = log_buffer.NewMessagePosition(earliestTime.UnixNano(), -2)
						readInMemoryLogErr = nil // 清除错误，继续处理
					} else {
						// 内存中还没有数据，等待新数据到达
						time.Sleep(1127 * time.Millisecond)
						continue
					}
				} else {
					// 【情况 2: 首次读取或等待新数据】
					// 检查下一天是否有日志文件
					nextDayTs := util.GetNextDayTsNano(lastReadTime.Time.UnixNano())
					position := log_buffer.NewMessagePosition(nextDayTs, -2)
					found, err := fs.filer.HasPersistedLogFiles(position)
					if err != nil {
						return fmt.Errorf("checking persisted log files: %w", err)
					}
					if found {
						// 找到了下一天的日志，更新读取位置
						lastReadTime = position
					}
				}
			}
		}

		// 【步骤 11: 从内存日志读取】
		glog.V(3).Infof("read in memory %v local subscribe %s from %+v", clientName, req.PathPrefix, lastReadTime)

		// LoopProcessLogData 会从内存日志读取事件
		// 如果内存中没有新数据，会调用等待回调函数
		lastReadTime, isDone, readInMemoryLogErr = fs.filer.LocalMetaLogBuffer.LoopProcessLogData("localMeta:"+clientName, lastReadTime, req.UntilNs, func() bool {
			// 【等待回调】当内存日志暂时没有新数据时，此函数被调用
			// 返回 true 继续等待，返回 false 退出循环

			// 【检查 1: 客户端是否已断开连接】
			select {
			case <-ctx.Done():
				// 上下文取消，客户端已断开
				return false
			default:
			}

			// 【检查 2: 等待新事件到达】
			// 使用条件变量实现高效的事件驱动等待，避免轮询
			fs.listenersLock.Lock()
			atomic.AddInt64(&fs.listenersWaits, 1)  // 增加等待计数
			fs.listenersCond.Wait()                 // 阻塞等待通知
			atomic.AddInt64(&fs.listenersWaits, -1) // 减少等待计数
			fs.listenersLock.Unlock()

			// 【检查 3: 客户端是否仍然有效】
			if !fs.hasClient(req.ClientId, req.ClientEpoch) {
				return false
			}
			return true
		}, eachLogEntryFn)

		// 【步骤 12: 处理内存日志读取结果】
		if readInMemoryLogErr != nil {
			if readInMemoryLogErr == log_buffer.ResumeFromDiskError {
				// 【情况 1: 内存日志说数据太旧】
				// 内存缓冲区说请求的时间太旧，需要从磁盘读取
				// 但只在以下情况下重试磁盘读取:
				//   (a) 刷盘位置前进了（有新数据刷到磁盘）
				//   (b) 读取位置前进了（正在追赶积压数据）
				currentFlushTsNs := fs.filer.LocalMetaLogBuffer.GetLastFlushTsNs()
				currentReadTsNs := lastReadTime.Time.UnixNano()
				if currentFlushTsNs > lastCheckedFlushTsNs || currentReadTsNs > lastDiskReadTsNs {
					glog.V(0).Infof("retry disk read %v local subscribe %s (lastFlushed: %v -> %v, readTs: %v -> %v)",
						clientName, req.PathPrefix,
						time.Unix(0, lastCheckedFlushTsNs), time.Unix(0, currentFlushTsNs),
						time.Unix(0, lastDiskReadTsNs), time.Unix(0, currentReadTsNs))
					continue // 回到循环开始，重新从磁盘读取
				}

				// 【情况 2: 无法继续前进】
				// 刷盘位置和读取位置都没有前进，无法继续
				// 等待新数据到达（使用事件驱动，而不是轮询）
				fs.listenersLock.Lock()
				atomic.AddInt64(&fs.listenersWaits, 1)
				fs.listenersCond.Wait() // 等待新数据通知
				atomic.AddInt64(&fs.listenersWaits, -1)
				fs.listenersLock.Unlock()
				continue // 被唤醒后重试
			}

			// 【其他错误】
			glog.Errorf("processed to %v: %v", lastReadTime, readInMemoryLogErr)
			if readInMemoryLogErr != log_buffer.ResumeError {
				// 非 ResumeError 的错误，退出循环
				break
			}
		}
		if isDone {
			return nil
		}
		if !fs.hasClient(req.ClientId, req.ClientEpoch) {
			return nil
		}
	}

	return readInMemoryLogErr

}

// eachLogEntryFn 包装事件通知处理函数,用于处理日志条目
// 参数:
//   - eachEventNotificationFn: 实际的事件处理函数
// 返回:
//   - log_buffer.EachLogEntryFuncType: 日志条目处理函数
//
// 工作流程:
//   1. 反序列化日志条目数据为 SubscribeMetadataResponse
//   2. 调用传入的事件处理函数
//   3. 返回处理结果
func eachLogEntryFn(eachEventNotificationFn func(dirPath string, eventNotification *filer_pb.EventNotification, tsNs int64) error) log_buffer.EachLogEntryFuncType {
	return func(logEntry *filer_pb.LogEntry) (bool, error) {
		// 反序列化日志条目为事件响应结构
		event := &filer_pb.SubscribeMetadataResponse{}
		if err := proto.Unmarshal(logEntry.Data, event); err != nil {
			glog.Errorf("unexpected unmarshal filer_pb.SubscribeMetadataResponse: %v", err)
			return false, fmt.Errorf("unexpected unmarshal filer_pb.SubscribeMetadataResponse: %w", err)
		}

		// 调用实际的事件处理函数
		if err := eachEventNotificationFn(event.Directory, event.EventNotification, event.TsNs); err != nil {
			return false, err
		}

		// 返回 false 表示继续处理下一条日志
		return false, nil
	}
}

// eachEventNotificationFn 创建事件通知处理函数
// 返回一个闭包函数，用于过滤和发送元数据变更事件到客户端
//
// 核心功能:
//   1. 事件过滤: 根据路径前缀、目录列表、签名等条件过滤事件
//   2. 防重复: 检查事件签名，避免将客户端自己产生的事件推送回去
//   3. 心跳机制: 当连续过滤大量事件时，发送空事件作为心跳保持连接
//   4. 监控统计: 记录最后发送的时间戳用于监控
//
// 参数:
//   - req: 订阅请求（包含过滤条件）
//   - stream: gRPC 流，用于发送事件
//   - clientName: 客户端名称（用于日志）
//
// 返回:
//   - func: 事件处理函数，每个事件都会调用此函数
func (fs *FilerServer) eachEventNotificationFn(req *filer_pb.SubscribeMetadataRequest, stream filer_pb.SeaweedFiler_SubscribeMetadataServer, clientName string) func(dirPath string, eventNotification *filer_pb.EventNotification, tsNs int64) error {
	// 【闭包变量】记录连续过滤的事件数量
	// 当过滤的事件太多时，需要发送心跳以保持连接活跃
	filtered := 0

	return func(dirPath string, eventNotification *filer_pb.EventNotification, tsNs int64) error {
		// 【延迟执行】检查是否需要发送心跳
		defer func() {
			// 【步骤 1: 心跳机制】
			// 当连续过滤的事件超过 MaxUnsyncedEvents 时，发送空事件作为心跳
			// 这样可以:
			//   1. 让客户端知道连接还活着
			//   2. 更新客户端的时间戳，避免断线重连后重复接收大量事件
			//   3. 及时检测客户端是否已断开连接
			if filtered > MaxUnsyncedEvents {
				if err := stream.Send(&filer_pb.SubscribeMetadataResponse{
					EventNotification: &filer_pb.EventNotification{}, // 空事件
					TsNs:              tsNs,                           // 但包含时间戳
				}); err == nil {
					filtered = 0 // 发送成功，重置计数器
				}
				// 注意: 如果发送失败，不重置计数器，下次还会尝试发送
			}
		}()

		// 【步骤 2: 签名检查 - 防止事件回环】
		// 增加过滤计数
		filtered++

		// 检查事件的签名列表
		// 每个 Filer 在转发事件时会添加自己的签名
		// 客户端订阅时也可以提供自己的签名
		foundSelf := false
		for _, sig := range eventNotification.Signatures {
			// 【检查 2.1: 客户端签名匹配】
			// 如果事件包含客户端自己的签名，说明这个事件是客户端产生的
			// 不应该推送回去，避免无限循环
			if sig == req.Signature && req.Signature != 0 {
				return nil // 跳过此事件
			}
			// 【检查 2.2: 本 Filer 签名】
			// 检查事件是否已经包含本 Filer 的签名
			if sig == fs.filer.Signature {
				foundSelf = true
			}
		}

		// 【步骤 2.3: 添加本 Filer 签名】
		// 如果事件还没有本 Filer 的签名，添加上去
		// 这样如果事件被转发到其他 Filer，可以避免循环回来
		if !foundSelf {
			eventNotification.Signatures = append(eventNotification.Signatures, fs.filer.Signature)
		}

		// 【步骤 3: 构造完整路径】
		// 从事件中提取文件/目录名称
		var entryName string
		if eventNotification.OldEntry != nil {
			// 删除或重命名事件：使用旧 Entry 的名称
			entryName = eventNotification.OldEntry.Name
		} else if eventNotification.NewEntry != nil {
			// 创建或更新事件：使用新 Entry 的名称
			entryName = eventNotification.NewEntry.Name
		}

		// 拼接完整路径：目录路径 + 文件名
		fullpath := util.Join(dirPath, entryName)

		// 【步骤 4: 过滤系统内部日志】
		// 跳过 Filer 内部的元数据日志目录
		// 这些日志是系统内部使用的，不应该暴露给客户端
		if strings.HasPrefix(fullpath, filer.SystemLogDir) {
			return nil
		}

		// 【步骤 5: 路径过滤】
		// 根据订阅请求的过滤条件，判断是否应该推送此事件
		// 支持三种过滤方式，按优先级检查:

		if hasPrefixIn(fullpath, req.PathPrefixes) {
			// 【过滤方式 1: 多路径前缀匹配】
			// 例如: PathPrefixes = ["/photos", "/videos"]
			//       只推送这两个目录下的事件
			// good - 通过过滤
		} else if matchByDirectory(dirPath, req.Directories) {
			// 【过滤方式 2: 精确目录匹配】
			// 例如: Directories = ["/photos/2024", "/videos/2024"]
			//       只推送这些精确目录下的事件（不包括子目录）
			// good - 通过过滤
		} else {
			// 【过滤方式 3: 单路径前缀匹配（默认）】
			// 检查文件的当前路径
			if !strings.HasPrefix(fullpath, req.PathPrefix) {
				// 当前路径不匹配，检查是否是重命名/移动事件
				if eventNotification.NewParentPath != "" {
					// 这是一个移动事件，检查目标路径
					newFullPath := util.Join(eventNotification.NewParentPath, entryName)
					if !strings.HasPrefix(newFullPath, req.PathPrefix) {
						// 目标路径也不匹配，跳过此事件
						return nil
					}
					// 目标路径匹配，继续处理
				} else {
					// 不是移动事件且路径不匹配，跳过
					return nil
				}
			}
			// 路径匹配，继续处理
		}

		// 【步骤 6: 记录监控指标】
		// 记录最后发送的事件时间戳，用于 Prometheus 监控
		// 可以用来:
		//   1. 监控订阅延迟
		//   2. 检测订阅是否正常工作
		//   3. 发现订阅积压问题
		stats.FilerServerLastSendTsOfSubscribeGauge.WithLabelValues(fs.option.Host.String(), req.ClientName, req.PathPrefix).Set(float64(tsNs))

		// 【步骤 7: 构造并发送响应消息】
		message := &filer_pb.SubscribeMetadataResponse{
			Directory:         dirPath,            // 事件发生的目录
			EventNotification: eventNotification,  // 事件详情
			TsNs:              tsNs,               // 事件时间戳（纳秒）
		}

		// 通过 gRPC 流发送消息到客户端
		if err := stream.Send(message); err != nil {
			// 发送失败，可能是客户端断开连接
			glog.V(0).Infof("=> client %v: %+v", clientName, err)
			return err
		}

		// 【步骤 8: 重置过滤计数器】
		// 成功发送了一个事件，重置过滤计数器
		filtered = 0
		return nil
	}
}

// hasPrefixIn 检查文本是否以任一前缀开头
// 用于过滤订阅事件,只推送匹配路径前缀的事件
func hasPrefixIn(text string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(text, p) {
			return true
		}
	}
	return false
}

// matchByDirectory 检查目录路径是否精确匹配列表中的任一项
// 用于精确目录匹配过滤,与前缀匹配不同
func matchByDirectory(dirPath string, directories []string) bool {
	for _, dir := range directories {
		if dirPath == dir {
			return true
		}
	}
	return false
}

// addClient 注册客户端订阅
// 参数:
//   - prefix: 日志前缀(如 "local" 或 "")
//   - clientType: 客户端类型标识
//   - clientAddress: 客户端地址
//   - clientId: 客户端唯一 ID
//   - clientEpoch: 客户端纪元,用于检测客户端重启
// 返回:
//   - isReplacing: 是否替换了旧订阅
//   - alreadyKnown: 是否已存在相同订阅
//   - clientName: 客户端标识字符串
func (fs *FilerServer) addClient(prefix string, clientType string, clientAddress string, clientId int32, clientEpoch int32) (isReplacing, alreadyKnown bool, clientName string) {
	// 构建客户端标识: 类型@地址
	clientName = clientType + "@" + clientAddress
	glog.V(0).Infof("+ %v listener %v clientId %v clientEpoch %v", prefix, clientName, clientId, clientEpoch)

	if clientId != 0 {
		fs.knownListenersLock.Lock()
		defer fs.knownListenersLock.Unlock()

		epoch, found := fs.knownListeners[clientId]
		if !found || epoch < clientEpoch {
			// 新客户端或客户端重启(Epoch 更大)
			fs.knownListeners[clientId] = clientEpoch
			isReplacing = true
		} else {
			// 已存在相同的订阅
			alreadyKnown = true
		}
	}
	return
}

// deleteClient 删除客户端订阅
// 只有当 Epoch 匹配或更大时才删除,防止删除新的订阅
func (fs *FilerServer) deleteClient(prefix string, clientName string, clientId int32, clientEpoch int32) {
	glog.V(0).Infof("- %v listener %v clientId %v clientEpoch %v", prefix, clientName, clientId, clientEpoch)
	if clientId != 0 {
		fs.knownListenersLock.Lock()
		defer fs.knownListenersLock.Unlock()

		epoch, found := fs.knownListeners[clientId]
		if found && epoch <= clientEpoch {
			// 只有当记录的 Epoch <= 请求的 Epoch 时才删除
			// 这防止删除已经重新连接的新订阅
			delete(fs.knownListeners, clientId)
		}
	}
}

// hasClient 检查客户端是否仍然存在
// 用于在长时间运行的订阅循环中检测客户端是否已断开
func (fs *FilerServer) hasClient(clientId int32, clientEpoch int32) bool {
	if clientId != 0 {
		fs.knownListenersLock.Lock()
		defer fs.knownListenersLock.Unlock()

		epoch, found := fs.knownListeners[clientId]
		if found && epoch <= clientEpoch {
			// 客户端仍然存在且 Epoch 匹配
			return true
		}
	}
	return false
}
