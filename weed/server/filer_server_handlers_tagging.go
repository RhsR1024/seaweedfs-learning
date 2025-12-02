// Package weed_server 中的 filer_server_handlers_tagging.go 提供对象标签（Seaweed- 前缀）管理接口
// 包含添加/替换以及删除扩展属性的 HTTP 处理逻辑。
package weed_server

import (
	"net/http"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// PutTaggingHandler 处理文件标签的添加或替换
// 支持通过 HTTP PUT 添加或替换以 Seaweed- 开头的扩展属性
//
// 使用场景:
//   - 为文件添加自定义元数据标签
//   - 实现文件分类和管理
//   - 存储应用程序特定的元数据
//
// 标签格式:
//   - 标签名必须以 "Seaweed-" 开头（needle.PairNamePrefix）
//   - 标签值通过 HTTP 头部传递
//   - 如果标签已存在，会被新值替换
//
// 示例:
//   curl -X PUT -H "Seaweed-Category: photos" -H "Seaweed-Year: 2024" \
//     http://localhost:8888/path/to/file?tagging
//
// URL 要求:
//   - 必须包含 ?tagging 查询参数
func (fs *FilerServer) PutTaggingHandler(w http.ResponseWriter, r *http.Request) {

	// 【步骤 1: 初始化上下文和路径】
	ctx := r.Context()

	path := r.URL.Path
	// 移除路径末尾的斜杠（如果有）
	if strings.HasSuffix(path, "/") {
		path = path[:len(path)-1]
	}

	// 【步骤 2: 查找文件元数据】
	existingEntry, err := fs.filer.FindEntry(ctx, util.FullPath(path))
	if err != nil {
		// 文件不存在或查找失败
		writeJsonError(w, r, http.StatusNotFound, err)
		return
	}
	if existingEntry == nil {
		// 双重检查：确保 entry 不为 nil
		writeJsonError(w, r, http.StatusNotFound, err)
		return
	}

	// 【步骤 3: 初始化扩展属性映射】
	// 如果文件还没有扩展属性，创建一个新的 map
	if existingEntry.Extended == nil {
		existingEntry.Extended = make(map[string][]byte)
	}

	// 【步骤 4: 从 HTTP 头部提取标签】
	// 遍历所有请求头，查找以 "Seaweed-" 开头的头部
	for header, values := range r.Header {
		if strings.HasPrefix(header, needle.PairNamePrefix) {
			// 这是一个标签头部（例如: Seaweed-Category）
			for _, value := range values {
				// 将标签存储到扩展属性中
				// 注意: 如果有多个值，最后一个值会覆盖前面的值
				existingEntry.Extended[header] = []byte(value)
			}
		}
	}

	// 【步骤 5: 更新文件元数据】
	// CreateEntry 参数说明:
	//   - existingEntry: 更新后的元数据
	//   - O_EXCL=false: 允许覆盖现有 entry
	//   - isFromOtherCluster=false: 这是本地操作
	//   - signatures=nil: 不添加签名
	//   - skipCreateParentDir=false: 不跳过父目录创建（虽然这里应该已存在）
	//   - maxFilenameLength: 文件名长度限制
	if dbErr := fs.filer.CreateEntry(ctx, existingEntry, false, false, nil, false, fs.filer.MaxFilenameLength); dbErr != nil {
		glog.V(0).InfofCtx(ctx, "failing to update %s tagging : %v", path, dbErr)
		writeJsonError(w, r, http.StatusInternalServerError, dbErr)
		return
	}

	// 【步骤 6: 返回成功响应】
	// 使用 202 Accepted 表示请求已接受并处理
	writeJsonQuiet(w, r, http.StatusAccepted, nil)
	return
}

// DeleteTaggingHandler 处理文件标签的删除
// 删除 Seaweed- 前缀的扩展属性，支持删除全部或指定标签
//
// 删除模式:
//   1. 删除全部标签: 不提供 tagging 参数值
//      示例: curl -X DELETE http://localhost:8888/path/to/file?tagging
//   2. 删除指定标签: 提供逗号分隔的标签名列表
//      示例: curl -X DELETE http://localhost:8888/path/to/file?tagging=Category,Year
//
// 注意事项:
//   - 标签名不需要包含 "Seaweed-" 前缀
//   - 如果没有任何标签被删除，返回 304 Not Modified
//   - 如果删除成功，返回 202 Accepted
//
// URL 要求:
//   - 必须包含 ?tagging 查询参数
func (fs *FilerServer) DeleteTaggingHandler(w http.ResponseWriter, r *http.Request) {

	// 【步骤 1: 初始化上下文和路径】
	ctx := r.Context()

	path := r.URL.Path
	// 移除路径末尾的斜杠（如果有）
	if strings.HasSuffix(path, "/") {
		path = path[:len(path)-1]
	}

	// 【步骤 2: 查找文件元数据】
	existingEntry, err := fs.filer.FindEntry(ctx, util.FullPath(path))
	if err != nil {
		// 文件不存在或查找失败
		writeJsonError(w, r, http.StatusNotFound, err)
		return
	}
	if existingEntry == nil {
		// 双重检查：确保 entry 不为 nil
		writeJsonError(w, r, http.StatusNotFound, err)
		return
	}

	// 【步骤 3: 初始化扩展属性映射】
	// 如果文件还没有扩展属性，创建一个空 map（虽然没有标签可删除）
	if existingEntry.Extended == nil {
		existingEntry.Extended = make(map[string][]byte)
	}

	// 【步骤 4: 解析要删除的标签列表】
	// 从 URL 参数 tagging 中获取标签名列表（逗号分隔）
	// 例如: ?tagging=Category,Year
	toDelete := strings.Split(r.URL.Query().Get("tagging"), ",")

	// 使用 map 存储要删除的标签，方便快速查找
	deletions := make(map[string]struct{})
	for _, deletion := range toDelete {
		if deletion != "" {
			// 标签名不为空，添加到删除集合
			deletions[deletion] = struct{}{}
		}
	}

	// 【步骤 5: 删除标签】
	hasDeletion := false

	// 遍历所有扩展属性，查找需要删除的标签
	for header, _ := range existingEntry.Extended {
		if strings.HasPrefix(header, needle.PairNamePrefix) {
			// 这是一个标签（以 "Seaweed-" 开头）

			if len(deletions) == 0 {
				// 【模式 1: 删除全部标签】
				// 没有指定具体的标签名，删除所有标签
				delete(existingEntry.Extended, header)
				hasDeletion = true
			} else {
				// 【模式 2: 删除指定标签】
				// 提取标签名（移除 "Seaweed-" 前缀）
				// 例如: "Seaweed-Category" -> "Category"
				tag := header[len(needle.PairNamePrefix):]

				// 检查这个标签是否在删除列表中
				if _, found := deletions[tag]; found {
					delete(existingEntry.Extended, header)
					hasDeletion = true
				}
			}
		}
	}

	// 【步骤 6: 检查是否有删除操作】
	if !hasDeletion {
		// 没有任何标签被删除（可能标签不存在或未匹配）
		// 返回 304 Not Modified 表示没有变化
		writeJsonQuiet(w, r, http.StatusNotModified, nil)
		return
	}

	// 【步骤 7: 更新文件元数据】
	// 将删除标签后的元数据写回存储
	if dbErr := fs.filer.CreateEntry(ctx, existingEntry, false, false, nil, false, fs.filer.MaxFilenameLength); dbErr != nil {
		glog.V(0).InfofCtx(ctx, "failing to delete %s tagging : %v", path, dbErr)
		writeJsonError(w, r, http.StatusInternalServerError, dbErr)
		return
	}

	// 【步骤 8: 返回成功响应】
	// 使用 202 Accepted 表示请求已接受并处理
	writeJsonQuiet(w, r, http.StatusAccepted, nil)
	return
}
