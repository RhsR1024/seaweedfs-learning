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

// PutTaggingHandler 支持通过 HTTP PUT 添加或替换以 Seaweed- 开头的扩展属性
// 示例:
//   curl -X PUT -H "Seaweed-Name1: value1" http://localhost:8888/path/to/file?tagging
func (fs *FilerServer) PutTaggingHandler(w http.ResponseWriter, r *http.Request) {

	ctx := r.Context()

	path := r.URL.Path
	if strings.HasSuffix(path, "/") {
		path = path[:len(path)-1]
	}

	existingEntry, err := fs.filer.FindEntry(ctx, util.FullPath(path))
	if err != nil {
		writeJsonError(w, r, http.StatusNotFound, err)
		return
	}
	if existingEntry == nil {
		writeJsonError(w, r, http.StatusNotFound, err)
		return
	}

	if existingEntry.Extended == nil {
		existingEntry.Extended = make(map[string][]byte)
	}

	for header, values := range r.Header {
		if strings.HasPrefix(header, needle.PairNamePrefix) {
			for _, value := range values {
				existingEntry.Extended[header] = []byte(value)
			}
		}
	}

	if dbErr := fs.filer.CreateEntry(ctx, existingEntry, false, false, nil, false, fs.filer.MaxFilenameLength); dbErr != nil {
		glog.V(0).InfofCtx(ctx, "failing to update %s tagging : %v", path, dbErr)
		writeJsonError(w, r, http.StatusInternalServerError, dbErr)
		return
	}

	writeJsonQuiet(w, r, http.StatusAccepted, nil)
	return
}

// DeleteTaggingHandler 删除 Seaweed- 前缀的扩展属性
// 如果 URL 参数 tagging=tag1,tag2 存在，则删除指定标签，否则删除全部
// 示例:
//   curl -X DELETE http://localhost:8888/path/to/file?tagging
func (fs *FilerServer) DeleteTaggingHandler(w http.ResponseWriter, r *http.Request) {

	ctx := r.Context()

	path := r.URL.Path
	if strings.HasSuffix(path, "/") {
		path = path[:len(path)-1]
	}

	existingEntry, err := fs.filer.FindEntry(ctx, util.FullPath(path))
	if err != nil {
		writeJsonError(w, r, http.StatusNotFound, err)
		return
	}
	if existingEntry == nil {
		writeJsonError(w, r, http.StatusNotFound, err)
		return
	}

	if existingEntry.Extended == nil {
		existingEntry.Extended = make(map[string][]byte)
	}

	// parse out tags to be deleted
	toDelete := strings.Split(r.URL.Query().Get("tagging"), ",")
	deletions := make(map[string]struct{})
	for _, deletion := range toDelete {
		if deletion != "" {
			deletions[deletion] = struct{}{}
		}
	}

	// delete all tags or specific tags
	hasDeletion := false
	for header, _ := range existingEntry.Extended {
		if strings.HasPrefix(header, needle.PairNamePrefix) {
			if len(deletions) == 0 {
				delete(existingEntry.Extended, header)
				hasDeletion = true
			} else {
				tag := header[len(needle.PairNamePrefix):]
				if _, found := deletions[tag]; found {
					delete(existingEntry.Extended, header)
					hasDeletion = true
				}
			}
		}
	}

	if !hasDeletion {
		writeJsonQuiet(w, r, http.StatusNotModified, nil)
		return
	}

	if dbErr := fs.filer.CreateEntry(ctx, existingEntry, false, false, nil, false, fs.filer.MaxFilenameLength); dbErr != nil {
		glog.V(0).InfofCtx(ctx, "failing to delete %s tagging : %v", path, dbErr)
		writeJsonError(w, r, http.StatusInternalServerError, dbErr)
		return
	}

	writeJsonQuiet(w, r, http.StatusAccepted, nil)
	return
}
