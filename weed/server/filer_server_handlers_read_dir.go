// Package weed_server 中的 filer_server_handlers_read_dir.go 实现 Web UI 和 API 的目录列举逻辑
// 支持分页、名称过滤以及 JSON 或 HTML 两种返回形式。
package weed_server

import (
	"errors"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
	"net/http"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	ui "github.com/seaweedfs/seaweedfs/weed/server/filer_ui"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// listDirectoryHandler 处理目录列表请求（Web UI 和 API）
// 负责列出指定目录下的子目录与文件，默认按名称排序，并通过 lastFileName+limit 参数实现分页
//
// 分页机制:
//   - 当 lastFileName 为空时，首先返回子目录，之后才列出文件记录
//   - 当 lastFileName 不为空时，从该文件名之后继续列举
//   - limit 参数控制每页返回的条目数量
//
// 过滤功能:
//   - namePattern: 只返回匹配指定模式的文件（支持通配符）
//   - namePatternExclude: 排除匹配指定模式的文件
//
// 响应格式:
//   - Accept: application/json - 返回 JSON 格式
//   - 其他 - 返回 HTML 页面（Web UI）
//
// URL 参数:
//   - limit: 每页显示的条目数（默认使用配置中的 DirListingLimit）
//   - lastFileName: 上一页的最后一个文件名（用于分页）
//   - namePattern: 文件名匹配模式（可选）
//   - namePatternExclude: 文件名排除模式（可选）
func (fs *FilerServer) listDirectoryHandler(w http.ResponseWriter, r *http.Request) {
	// 【步骤 1: 初始化和权限检查】
	ctx := r.Context()

	// 检查是否允许暴露目录数据
	// 如果配置禁用了目录列表功能，返回 403 错误
	if fs.option.ExposeDirectoryData == false {
		writeJsonError(w, r, http.StatusForbidden, errors.New("ui is disabled"))
		return
	}

	// 【步骤 2: 记录监控指标】
	// 增加目录列表操作的计数器，用于 Prometheus 监控
	stats.FilerHandlerCounter.WithLabelValues(stats.DirList).Inc()

	// 【步骤 3: 解析请求路径】
	path := r.URL.Path

	// 清理路径：移除末尾的斜杠（除了根目录 "/"）
	// 例如: "/photos/" -> "/photos"
	if strings.HasSuffix(path, "/") && len(path) > 1 {
		path = path[:len(path)-1]
	}

	// 【步骤 4: 解析分页参数】
	// 尝试从查询参数中获取 limit
	limit, limitErr := strconv.Atoi(r.FormValue("limit"))
	if limitErr != nil {
		// 如果没有提供或解析失败，使用配置的默认值
		limit = fs.option.DirListingLimit
	}

	// 【步骤 5: 解析过滤参数】
	// lastFileName: 分页游标，从这个文件名之后继续列举
	lastFileName := r.FormValue("lastFileName")

	// namePattern: 文件名匹配模式（例如: "*.jpg" 只列出 jpg 文件）
	namePattern := r.FormValue("namePattern")

	// namePatternExclude: 文件名排除模式（例如: "*.tmp" 排除临时文件）
	namePatternExclude := r.FormValue("namePatternExclude")

	// 【步骤 6: 调用 Filer 核心接口列举目录】
	// ListDirectoryEntries 参数说明:
	//   - path: 目录路径
	//   - lastFileName: 分页游标
	//   - includeLastFile=false: 不包含 lastFileName 本身
	//   - limit: 返回条目数量限制
	//   - prefix="": 不使用前缀过滤（已经通过 path 指定了目录）
	//   - namePattern: 文件名匹配模式
	//   - namePatternExclude: 文件名排除模式
	entries, shouldDisplayLoadMore, err := fs.filer.ListDirectoryEntries(ctx, util.FullPath(path), lastFileName, false, int64(limit), "", namePattern, namePatternExclude)

	// 【步骤 7: 处理错误】
	if err != nil {
		glog.V(0).InfofCtx(ctx, "listDirectory %s %s %d: %s", path, lastFileName, limit, err)
		// 目录不存在或没有权限，返回 404
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// 【步骤 8: 路径规范化】
	// 将根目录 "/" 转换为空字符串，以便前端 URL 拼接
	if path == "/" {
		path = ""
	}

	// 【步骤 9: 计算响应元数据】
	emptyFolder := true
	if len(entries) > 0 {
		// 更新 lastFileName 为本次返回的最后一个条目的名称
		// 客户端可以用这个值作为下一页的分页游标
		lastFileName = entries[len(entries)-1].Name()
		emptyFolder = false
	}

	glog.V(4).InfofCtx(ctx, "listDirectory %s, last file %s, limit %d: %d items", path, lastFileName, limit, len(entries))

	// 【步骤 10: 根据客户端期望的格式返回响应】
	if r.Header.Get("Accept") == "application/json" {
		// 【情况 1: 返回 JSON 格式（API 调用）】
		writeJsonQuiet(w, r, http.StatusOK, struct {
			Version               string      // SeaweedFS 版本号
			Path                  string      // 当前目录路径
			Entries               interface{} // 目录条目列表
			Limit                 int         // 每页条目数
			LastFileName          string      // 本页最后一个文件名（用于下一页分页）
			ShouldDisplayLoadMore bool        // 是否还有更多数据可以加载
			EmptyFolder           bool        // 是否为空目录
		}{
			version.Version(),
			path,
			entries,
			limit,
			lastFileName,
			shouldDisplayLoadMore,
			emptyFolder,
		})
		return
	}

	// 【情况 2: 返回 HTML 页面（Web UI）】
	// 使用模板引擎渲染 HTML 页面
	err = ui.StatusTpl.Execute(w, struct {
		Version               string          // SeaweedFS 版本号
		Path                  string          // 当前目录路径
		Breadcrumbs           []ui.Breadcrumb // 面包屑导航（例如: Home > photos > 2024）
		Entries               interface{}     // 目录条目列表
		Limit                 int             // 每页条目数
		LastFileName          string          // 本页最后一个文件名
		ShouldDisplayLoadMore bool            // 是否显示"加载更多"按钮
		EmptyFolder           bool            // 是否为空目录
		ShowDirectoryDelete   bool            // 是否显示删除目录按钮
	}{
		version.Version(),
		path,
		ui.ToBreadcrumb(path), // 将路径转换为面包屑导航
		entries,
		limit,
		lastFileName,
		shouldDisplayLoadMore,
		emptyFolder,
		fs.option.ShowUIDirectoryDelete, // 配置项：是否允许从 UI 删除目录
	})

	// 【步骤 11: 处理模板渲染错误】
	if err != nil {
		glog.V(0).InfofCtx(ctx, "Template Execute Error: %v", err)
	}

}
