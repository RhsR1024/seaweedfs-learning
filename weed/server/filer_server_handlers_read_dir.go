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

// listDirectoryHandler 负责列出指定目录下的子目录与文件
// 默认按名称排序，并通过 lastFileName+limit 参数实现分页
// 当 lastFileName 为空时，首先返回子目录，之后才列出文件记录
func (fs *FilerServer) listDirectoryHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if fs.option.ExposeDirectoryData == false {
		writeJsonError(w, r, http.StatusForbidden, errors.New("ui is disabled"))
		return
	}

	stats.FilerHandlerCounter.WithLabelValues(stats.DirList).Inc()

	path := r.URL.Path
	if strings.HasSuffix(path, "/") && len(path) > 1 {
		path = path[:len(path)-1]
	}

	limit, limitErr := strconv.Atoi(r.FormValue("limit"))
	if limitErr != nil {
		limit = fs.option.DirListingLimit
	}

	lastFileName := r.FormValue("lastFileName")
	namePattern := r.FormValue("namePattern")
	namePatternExclude := r.FormValue("namePatternExclude")

	entries, shouldDisplayLoadMore, err := fs.filer.ListDirectoryEntries(ctx, util.FullPath(path), lastFileName, false, int64(limit), "", namePattern, namePatternExclude)

	if err != nil {
		glog.V(0).InfofCtx(ctx, "listDirectory %s %s %d: %s", path, lastFileName, limit, err)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if path == "/" {
		path = ""
	}

	emptyFolder := true
	if len(entries) > 0 {
		lastFileName = entries[len(entries)-1].Name()
		emptyFolder = false
	}

	glog.V(4).InfofCtx(ctx, "listDirectory %s, last file %s, limit %d: %d items", path, lastFileName, limit, len(entries))

	if r.Header.Get("Accept") == "application/json" {
		writeJsonQuiet(w, r, http.StatusOK, struct {
			Version               string
			Path                  string
			Entries               interface{}
			Limit                 int
			LastFileName          string
			ShouldDisplayLoadMore bool
			EmptyFolder           bool
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

	err = ui.StatusTpl.Execute(w, struct {
		Version               string
		Path                  string
		Breadcrumbs           []ui.Breadcrumb
		Entries               interface{}
		Limit                 int
		LastFileName          string
		ShouldDisplayLoadMore bool
		EmptyFolder           bool
		ShowDirectoryDelete   bool
	}{
		version.Version(),
		path,
		ui.ToBreadcrumb(path),
		entries,
		limit,
		lastFileName,
		shouldDisplayLoadMore,
		emptyFolder,
		fs.option.ShowUIDirectoryDelete,
	})
	if err != nil {
		glog.V(0).InfofCtx(ctx, "Template Execute Error: %v", err)
	}

}
