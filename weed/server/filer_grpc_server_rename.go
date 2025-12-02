// Package weed_server 中的 filer_grpc_server_rename.go 实现文件/目录的原子重命名和移动
// 支持事务性操作,确保重命名过程的一致性
package weed_server

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// AtomicRenameEntry 原子性地重命名或移动文件/目录
// 使用事务确保操作的原子性:
//   - 要么完全成功(旧 entry 删除,新 entry 创建)
//   - 要么完全失败(回滚所有更改)
//
// 参数:
//   - req.OldDirectory: 原目录路径
//   - req.OldName: 原文件/目录名
//   - req.NewDirectory: 新目录路径
//   - req.NewName: 新文件/目录名
//   - req.Signatures: 签名信息,用于鉴权
// 返回:
//   - AtomicRenameEntryResponse: 空响应表示成功
func (fs *FilerServer) AtomicRenameEntry(ctx context.Context, req *filer_pb.AtomicRenameEntryRequest) (*filer_pb.AtomicRenameEntryResponse, error) {

	// 记录重命名请求的详细信息
	glog.V(1).Infof("AtomicRenameEntry %v", req)

	// 规范化路径,确保使用统一的路径分隔符
	oldParent := util.FullPath(filepath.ToSlash(req.OldDirectory))
	newParent := util.FullPath(filepath.ToSlash(req.NewDirectory))

	// 【步骤 1: 权限检查】
	// 检查是否允许进行重命名操作
	// 可能的限制:
	//   - 不能重命名系统目录
	//   - 不能跨特殊边界移动文件
	if err := fs.filer.CanRename(oldParent, newParent, req.OldName); err != nil {
		return nil, err
	}

	// 【步骤 2: 开始事务】
	// 如果 Filer Store 支持事务,将开启事务
	// 如果不支持(如某些 NoSQL),会使用模拟事务
	ctx, err := fs.filer.BeginTransaction(ctx)
	if err != nil {
		return nil, err
	}

	// 【步骤 3: 查找原 entry】
	oldEntry, err := fs.filer.FindEntry(ctx, oldParent.Child(req.OldName))
	if err != nil {
		// 原 entry 不存在,回滚事务并返回错误
		fs.filer.RollbackTransaction(ctx)
		return nil, fmt.Errorf("%s/%s not found: %v", req.OldDirectory, req.OldName, err)
	}

	// 【步骤 4: 执行移动操作】
	// nil 参数表示不需要流式返回中间状态
	moveErr := fs.moveEntry(ctx, nil, oldParent, oldEntry, newParent, req.NewName, req.Signatures)
	if moveErr != nil {
		// 移动失败,回滚事务
		fs.filer.RollbackTransaction(ctx)
		return nil, fmt.Errorf("%s/%s move error: %v", req.OldDirectory, req.OldName, moveErr)
	} else {
		// 【步骤 5: 提交事务】
		if commitError := fs.filer.CommitTransaction(ctx); commitError != nil {
			// 提交失败,回滚事务
			fs.filer.RollbackTransaction(ctx)
			return nil, fmt.Errorf("%s/%s move commit error: %v", req.OldDirectory, req.OldName, commitError)
		}
	}

	// 返回空响应表示成功
	return &filer_pb.AtomicRenameEntryResponse{}, nil
}

// StreamRenameEntry 流式重命名/移动文件/目录
// 与 AtomicRenameEntry 的区别:
//   - 支持流式返回中间进度(适合大目录移动)
//   - 客户端可以实时看到移动进度
//   - 遵循 POSIX rename 语义
//
// 参数:
//   - req: 重命名请求
//   - stream: 用于返回进度的流式响应
// 返回:
//   - error: 错误信息
func (fs *FilerServer) StreamRenameEntry(req *filer_pb.StreamRenameEntryRequest, stream filer_pb.SeaweedFiler_StreamRenameEntryServer) (err error) {

	// 记录流式重命名请求的详细信息
	glog.V(1).Infof("StreamRenameEntry %v", req)

	// 规范化路径
	oldParent := util.FullPath(filepath.ToSlash(req.OldDirectory))
	newParent := util.FullPath(filepath.ToSlash(req.NewDirectory))

	// 【步骤 1: 权限检查】
	if err := fs.filer.CanRename(oldParent, newParent, req.OldName); err != nil {
		return err
	}

	// 使用新的 context (不使用客户端传递的 context)
	ctx := context.Background()

	// 【步骤 2: 开始事务】
	ctx, err = fs.filer.BeginTransaction(ctx)
	if err != nil {
		return err
	}

	// 【步骤 3: 查找原 entry】
	oldEntry, err := fs.filer.FindEntry(ctx, oldParent.Child(req.OldName))
	if err != nil {
		fs.filer.RollbackTransaction(ctx)
		return fmt.Errorf("%s/%s not found: %v", req.OldDirectory, req.OldName, err)
	}

	// 【步骤 4: 目录重命名特殊处理】
	// 遵循 POSIX 标准: https://pubs.opengroup.org/onlinepubs/000095399/functions/rename.html
	if oldEntry.IsDirectory() {
		targetDir := newParent.Child(req.NewName)
		newEntry, err := fs.filer.FindEntry(ctx, targetDir)

		// 如果目标路径已存在
		if err == nil {
			// 检查 1: 目标必须是目录
			if !newEntry.IsDirectory() {
				fs.filer.RollbackTransaction(ctx)
				return fmt.Errorf("%s is not directory", targetDir)
			}

			// 检查 2: 目标目录必须为空
			// 只查询 1 个 entry 即可判断是否为空
			if entries, _, _ := fs.filer.ListDirectoryEntries(context.Background(), targetDir, "", false, 1, "", "", ""); len(entries) > 0 {
				return fmt.Errorf("%s is not empty", targetDir)
			}
		}
	}

	// 【步骤 5: 执行移动操作】
	// 传入 stream 参数,支持流式返回进度
	moveErr := fs.moveEntry(ctx, stream, oldParent, oldEntry, newParent, req.NewName, req.Signatures)
	if moveErr != nil {
		// 移动失败,回滚事务
		fs.filer.RollbackTransaction(ctx)
		return fmt.Errorf("%s/%s move error: %v", req.OldDirectory, req.OldName, moveErr)
	} else {
		// 【步骤 6: 提交事务】
		if commitError := fs.filer.CommitTransaction(ctx); commitError != nil {
			// 提交失败,回滚事务
			fs.filer.RollbackTransaction(ctx)
			return fmt.Errorf("%s/%s move commit error: %v", req.OldDirectory, req.OldName, commitError)
		}
	}

	return nil
}

// moveEntry 移动单个 entry (文件或目录)
// 如果是目录,会递归移动其所有子 entry
//
// 参数:
//   - ctx: 事务上下文
//   - stream: 流式响应(可选,用于返回进度)
//   - oldParent: 原父目录路径
//   - entry: 要移动的 entry
//   - newParent: 新父目录路径
//   - newName: 新名称
//   - signatures: 签名信息
// 返回:
//   - error: 错误信息
func (fs *FilerServer) moveEntry(ctx context.Context, stream filer_pb.SeaweedFiler_StreamRenameEntryServer, oldParent util.FullPath, entry *filer.Entry, newParent util.FullPath, newName string, signatures []int32) error {

	// 移动当前 entry,如果是目录,在回调中递归移动子 entry
	if err := fs.moveSelfEntry(ctx, stream, oldParent, entry, newParent, newName, func() error {
		// 【回调函数】在移动当前 entry 之后,移动子 entry 之前执行
		if entry.IsDirectory() {
			// 递归移动目录下的所有子 entry
			if err := fs.moveFolderSubEntries(ctx, stream, oldParent, entry, newParent, newName, signatures); err != nil {
				return err
			}
		}
		return nil
	}, signatures); err != nil {
		return fmt.Errorf("fail to move %s => %s: %v", oldParent.Child(entry.Name()), newParent.Child(newName), err)
	}

	return nil
}

// moveFolderSubEntries 递归移动目录下的所有子 entry
// 采用分页方式遍历,避免一次性加载大量 entry 到内存
//
// 参数:
//   - ctx: 事务上下文
//   - stream: 流式响应(可选)
//   - oldParent: 原父目录路径
//   - entry: 要移动的目录 entry
//   - newParent: 新父目录路径
//   - newName: 新目录名
//   - signatures: 签名信息
// 返回:
//   - error: 错误信息
func (fs *FilerServer) moveFolderSubEntries(ctx context.Context, stream filer_pb.SeaweedFiler_StreamRenameEntryServer, oldParent util.FullPath, entry *filer.Entry, newParent util.FullPath, newName string, signatures []int32) error {

	// 计算原目录和新目录的完整路径
	currentDirPath := oldParent.Child(entry.Name())
	newDirPath := newParent.Child(newName)

	glog.V(1).Infof("moving folder %s => %s", currentDirPath, newDirPath)

	// 【分页遍历目录】
	// 使用游标方式遍历,避免一次性加载所有子 entry
	lastFileName := ""      // 上一次遍历的最后一个文件名(游标)
	includeLastFile := false // 是否包含 lastFileName (避免重复)

	for {
		// 每次最多查询 1024 个 entry
		entries, hasMore, err := fs.filer.ListDirectoryEntries(ctx, currentDirPath, lastFileName, includeLastFile, 1024, "", "", "")
		if err != nil {
			return err
		}

		// 调试信息: 找到的 entry 数量
		// println("found", len(entries), "entries under", currentDirPath)

		// 遍历当前批次的所有 entry,逐个移动
		for _, item := range entries {
			lastFileName = item.Name() // 更新游标
			// 调试信息: 当前处理的文件名
			// println("processing", lastFileName)

			// 递归调用 moveEntry 移动子 entry
			// 如果子 entry 也是目录,会继续递归
			err := fs.moveEntry(ctx, stream, currentDirPath, item, newDirPath, item.Name(), signatures)
			if err != nil {
				return err
			}
		}

		// 如果没有更多 entry,退出循环
		if !hasMore {
			break
		}
	}
	return nil
}

// moveSelfEntry 移动单个 entry 自身(不包括子 entry)
// 执行顺序:
//   1. 在新位置创建 entry
//   2. 发送创建事件(如果有 stream)
//   3. 执行回调函数(通常是移动子 entry)
//   4. 删除原位置的 entry
//   5. 发送删除事件(如果有 stream)
//
// 参数:
//   - ctx: 事务上下文
//   - stream: 流式响应(可选)
//   - oldParent: 原父目录路径
//   - entry: 要移动的 entry
//   - newParent: 新父目录路径
//   - newName: 新名称
//   - moveFolderSubEntries: 回调函数,在新 entry 创建后,旧 entry 删除前执行
//   - signatures: 签名信息
// 返回:
//   - error: 错误信息
func (fs *FilerServer) moveSelfEntry(ctx context.Context, stream filer_pb.SeaweedFiler_StreamRenameEntryServer, oldParent util.FullPath, entry *filer.Entry, newParent util.FullPath, newName string, moveFolderSubEntries func() error, signatures []int32) error {

	// 计算原路径和新路径
	oldPath, newPath := oldParent.Child(entry.Name()), newParent.Child(newName)

	glog.V(1).Infof("moving entry %s => %s", oldPath, newPath)

	// 【特殊情况】如果原路径和新路径相同,跳过移动
	// 这可能发生在重命名为相同名称的情况
	if oldPath == newPath {
		glog.V(1).Infof("skip moving entry %s => %s", oldPath, newPath)
		return nil
	}

	// 【步骤 1: 在新位置创建 entry】
	// 创建一个新的 Entry 对象,复制原 entry 的所有属性
	newEntry := &filer.Entry{
		FullPath:        newPath,               // 新路径
		Attr:            entry.Attr,            // 文件属性(大小、权限、时间戳等)
		Chunks:          entry.GetChunks(),     // 数据块列表
		Extended:        entry.Extended,        // 扩展属性
		Content:         entry.Content,         // 小文件的内联内容
		HardLinkCounter: entry.HardLinkCounter, // 硬链接计数
		HardLinkId:      entry.HardLinkId,      // 硬链接 ID
		Remote:          entry.Remote,          // 远程存储信息
		Quota:           entry.Quota,           // 配额信息
	}

	// 在新位置创建 entry
	// 参数说明:
	//   - o_excl=false: 如果已存在同名文件,覆盖它
	//   - isFromOtherCluster=false: 不是来自其他集群的同步
	//   - skipCreateParentDir=false: 自动创建父目录(如果不存在)
	if createErr := fs.filer.CreateEntry(ctx, newEntry, false, false, signatures, false, fs.filer.MaxFilenameLength); createErr != nil {
		return createErr
	}

	// 【步骤 2: 发送"创建"事件通知】
	// 如果有 stream,通知订阅者新 entry 已创建
	if stream != nil {
		if err := stream.Send(&filer_pb.StreamRenameEntryResponse{
			Directory: string(oldParent), // 原目录
			EventNotification: &filer_pb.EventNotification{
				OldEntry: &filer_pb.Entry{
					Name: entry.Name(), // 原名称
				},
				NewEntry:           newEntry.ToProtoEntry(), // 新 entry 的完整信息
				DeleteChunks:       false,                   // 不删除 chunks (移动操作会复用)
				NewParentPath:      string(newParent),       // 新父目录
				IsFromOtherCluster: false,
				Signatures:         nil,
			},
			TsNs: time.Now().UnixNano(), // 事件时间戳
		}); err != nil {
			return err
		}
	}

	// 【步骤 3: 执行回调函数】
	// 对于目录,这里会递归移动所有子 entry
	// 对于文件,回调为 nil,直接跳过
	if moveFolderSubEntries != nil {
		if moveChildrenErr := moveFolderSubEntries(); moveChildrenErr != nil {
			return moveChildrenErr
		}
	}

	// 【步骤 4: 删除原位置的 entry】
	// 在 context 中标记操作类型为 "MV" (移动)
	// 这样在事件日志中可以区分删除和移动
	ctx = context.WithValue(ctx, "OP", "MV")

	// 删除原 entry 的元数据和数据
	// 参数说明:
	//   - isDeleteData=false: 不删除数据块(因为新 entry 会复用)
	//   - isRecursive=false: 不递归删除(子 entry 已经单独移动)
	//   - ignoreRecursiveError=false: 不忽略递归错误
	//   - shouldDeleteChunks=false: 不删除 chunks
	//   - skipParentDirUpdate=0: 不跳过父目录更新
	deleteErr := fs.filer.DeleteEntryMetaAndData(ctx, oldPath, false, false, false, false, signatures, 0)
	if deleteErr != nil {
		return deleteErr
	}

	// 【步骤 5: 发送"删除"事件通知】
	// 通知订阅者原 entry 已删除
	if stream != nil {
		if err := stream.Send(&filer_pb.StreamRenameEntryResponse{
			Directory: string(oldParent), // 原目录
			EventNotification: &filer_pb.EventNotification{
				OldEntry: &filer_pb.Entry{
					Name: entry.Name(), // 原名称
				},
				NewEntry:           nil,   // 删除事件,NewEntry 为 nil
				DeleteChunks:       false, // 不删除 chunks
				NewParentPath:      "",
				IsFromOtherCluster: false,
				Signatures:         nil,
			},
			TsNs: time.Now().UnixNano(), // 事件时间戳
		}); err != nil {
			return err
		}
	}

	return nil

}
