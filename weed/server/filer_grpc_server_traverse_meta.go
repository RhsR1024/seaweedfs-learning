// Package weed_server 中的 filer_grpc_server_traverse_meta.go 实现 BFS 方式遍历目录树
// 使用广度优先搜索 (Breadth-First Search) 遍历元数据，支持路径前缀排除
package weed_server

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/viant/ptrie"
)

// TraverseBfsMetadata 使用 BFS 方式遍历目录树的元数据
// 与深度优先搜索 (DFS) 不同，BFS 按层级顺序遍历:
//   - 先访问根目录
//   - 再访问根目录的所有直接子项
//   - 然后访问所有子项的子项
//   - 以此类推
//
// 这种遍历方式的优点:
//   1. 可以更快地看到目录结构的全貌
//   2. 可以按层级进行处理（例如先处理浅层目录）
//   3. 可以更容易地实现中断和恢复
//
// 参数:
//   - req.Directory: 要遍历的根目录
//   - req.ExcludedPrefixes: 要排除的路径前缀列表
//     例如: ["/data/.tmp", "/data/.hidden"] 会跳过这些路径下的所有内容
//
// 返回:
//   - 通过 stream 逐个发送遍历到的 Entry
//   - 每个响应包含 Directory (父目录) 和 Entry (条目本身)
func (fs *FilerServer) TraverseBfsMetadata(req *filer_pb.TraverseBfsMetadataRequest, stream filer_pb.SeaweedFiler_TraverseBfsMetadataServer) error {

	glog.V(0).Infof("TraverseBfsMetadata %v", req)

	// 【步骤 1: 构建排除路径前缀树】
	// 使用 ptrie (Prefix Trie) 数据结构存储需要排除的路径前缀
	// ptrie 的优点:
	//   - O(m) 时间复杂度进行前缀匹配，m 为路径长度
	//   - 内存占用小，共享公共前缀
	//   - 比逐个字符串比较快得多
	excludedTrie := ptrie.New[bool]()
	for _, excluded := range req.ExcludedPrefixes {
		// 将排除路径插入前缀树
		// 例如: "/data/.tmp" -> true
		excludedTrie.Put([]byte(excluded), true)
	}

	ctx := stream.Context()

	// 【步骤 2: 初始化 BFS 队列】
	// BFS 的核心是使用队列 (FIFO):
	//   - 先入队的先处理（保证按层级顺序）
	//   - 处理每个目录时，将其子项加入队列尾部
	queue := util.NewQueue[*filer.Entry]()

	// 加载根目录 Entry
	dirEntry, err := fs.filer.FindEntry(ctx, util.FullPath(req.Directory))
	if err != nil {
		return fmt.Errorf("find dir %s: %v", req.Directory, err)
	}

	// 将根目录入队，作为 BFS 的起点
	queue.Enqueue(dirEntry)

	// 【步骤 3: BFS 主循环】
	// 持续从队列中取出 Entry 进行处理，直到队列为空
	for item := queue.Dequeue(); item != nil; item = queue.Dequeue() {

		// 【步骤 3.1: 检查当前路径是否被排除】
		// 使用前缀树快速匹配当前路径是否以排除前缀开头
		// 例如: 当前路径 "/data/.tmp/file.txt"
		//       排除前缀 "/data/.tmp"
		//       匹配成功，跳过该路径
		if excludedTrie.MatchPrefix([]byte(item.FullPath), func(key []byte, value bool) bool {
			// 匹配回调函数: 只要找到任何匹配的前缀就返回 true
			return true
		}) {
			// println("excluded", item.FullPath)
			continue
		}

		// 【步骤 3.2: 发送当前 Entry 到客户端】
		// 获取父目录路径
		// 例如: "/data/files/a.txt" -> parent="/data/files", name="a.txt"
		parent, _ := item.FullPath.DirAndName()
		if err := stream.Send(&filer_pb.TraverseBfsMetadataResponse{
			Directory: parent,          // 父目录路径
			Entry:     item.ToProtoEntry(), // Entry 的 protobuf 表示
		}); err != nil {
			return fmt.Errorf("send traverse bfs metadata response: %w", err)
		}

		// 【步骤 3.3: 处理目录类型的 Entry】
		// 如果当前 Entry 不是目录，则无需继续遍历子项
		if !item.IsDirectory() {
			continue
		}

		// 【步骤 3.4: 遍历目录的所有子项并加入队列】
		// 使用 iterateDirectory 函数逐个读取子项
		// 并通过回调函数将子项加入 BFS 队列
		if err := fs.iterateDirectory(ctx, item.FullPath, func(entry *filer.Entry) error {
			// 将子项加入队列尾部
			// 这些子项会在当前层级的所有项处理完后才被处理
			// 从而保证 BFS 的层级顺序
			queue.Enqueue(entry)
			return nil
		}); err != nil {
			return err
		}
	}

	return nil
}

// iterateDirectory 分页遍历目录的所有子项
// 使用流式读取避免一次性加载大目录的所有条目到内存
//
// 工作原理:
//   1. 每次读取最多 1024 个条目
//   2. 通过 lastFileName 实现分页（类似游标）
//   3. 对每个条目调用回调函数 fn
//   4. 如果 fn 返回错误，立即停止遍历
//   5. 如果某批次没有返回任何条目，说明已遍历完毕
//
// 参数:
//   - dirPath: 要遍历的目录路径
//   - fn: 对每个条目执行的回调函数
//     如果返回 error，会立即停止遍历
//
// 返回:
//   - error: 遍历过程中的错误（来自 fn 或 StreamListDirectoryEntries）
func (fs *FilerServer) iterateDirectory(ctx context.Context, dirPath util.FullPath, fn func(entry *filer.Entry) error) (err error) {
	var lastFileName string // 分页游标，记录上一批次最后一个文件名
	var listErr error

	// 【循环分页读取】
	// 持续读取直到没有更多条目或发生错误
	for {
		var hasEntries bool // 标记本批次是否有条目

		// 【调用 Filer 的流式列表接口】
		// 参数说明:
		//   - dirPath: 目录路径
		//   - lastFileName: 分页游标，从这个文件名之后开始读取
		//   - false: 不包含已删除的条目
		//   - 1024: 每批最多读取 1024 个条目
		//   - "", "", "": 不过滤 namePattern, 不限制 collection, 不限制 replication
		//   - 回调函数: 对每个条目执行
		lastFileName, listErr = fs.filer.StreamListDirectoryEntries(ctx, dirPath, lastFileName, false, 1024, "", "", "", func(entry *filer.Entry) bool {
			hasEntries = true // 标记有条目返回

			// 执行用户提供的回调函数
			if fnErr := fn(entry); fnErr != nil {
				// 回调函数返回错误，保存错误并停止遍历
				err = fnErr
				return false // 返回 false 会停止 StreamListDirectoryEntries
			}

			// 继续处理下一个条目
			return true
		})

		// 【错误处理】
		// StreamListDirectoryEntries 本身发生错误
		if listErr != nil {
			return listErr
		}

		// 回调函数返回的错误
		if err != nil {
			return err
		}

		// 【终止条件】
		// 如果本批次没有返回任何条目，说明目录已遍历完毕
		if !hasEntries {
			return nil
		}

		// 继续读取下一批次
		// lastFileName 已在 StreamListDirectoryEntries 中更新为本批次最后一个文件名
	}
}
