// Package volume_info 提供 Volume 信息文件（.vif）的读写功能
//
// Volume 信息文件（.vif）用于存储：
// - 远程存储文件列表（EC 分片或完整备份在云存储上）
// - Volume 版本信息
// - 副本配置
// - 字节偏移量（用于增量同步）
// - 数据文件大小
// - 过期时间
// - 只读标志
//
// 文件格式：JSON（protobuf 的 JSON 序列化）
//
// 使用场景：
// - Volume 迁移到云存储（S3、Rclone 等）
// - EC Volume 的分片信息管理
// - Volume 的元数据持久化
package volume_info

import (
	"fmt"
	"os"

	jsonpb "google.golang.org/protobuf/encoding/protojson"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	// 导入远程存储 Backend 插件（通过 init() 函数自动注册）
	_ "github.com/seaweedfs/seaweedfs/weed/storage/backend/rclone_backend"
	_ "github.com/seaweedfs/seaweedfs/weed/storage/backend/s3_backend"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// MaybeLoadVolumeInfo 尝试加载 Volume 信息文件（.vif）
//
// 功能：
// - 检查 .vif 文件是否存在
// - 读取并解析 JSON 格式的 VolumeInfo
// - 支持旧版本格式的自动转换
// - 返回远程文件状态
//
// 参数：
//   - fileName: .vif 文件的完整路径
//
// 返回值：
//   - volumeInfo: Volume 信息对象（永远不为 nil，即使文件不存在也返回空对象）
//   - hasRemoteFile: 是否有远程文件（Files 列表非空）
//   - hasVolumeInfoFile: .vif 文件是否存在
//   - err: 读取或解析错误
//
// 使用示例：
//
//	volumeInfo, hasRemote, hasFile, err := MaybeLoadVolumeInfo("/data/1.vif")
//	if err != nil {
//	    // 处理错误
//	}
//	if hasRemote {
//	    // Volume 有远程备份
//	}
//
// MaybeLoadVolumeInfo load the file data as *volume_server_pb.VolumeInfo, the returned volumeInfo will not be nil
func MaybeLoadVolumeInfo(fileName string) (volumeInfo *volume_server_pb.VolumeInfo, hasRemoteFile bool, hasVolumeInfoFile bool, err error) {

	// 初始化空的 VolumeInfo（确保返回值不为 nil）
	volumeInfo = &volume_server_pb.VolumeInfo{}

	glog.V(1).Infof("maybeLoadVolumeInfo checks %s", fileName)

	// 检查文件是否存在且可读
	if exists, canRead, _, _, _ := util.CheckFile(fileName); !exists || !canRead {
		if !exists {
			// 文件不存在，返回空的 VolumeInfo（不是错误）
			return
		}

		// 文件存在但不可读
		hasVolumeInfoFile = true
		if !canRead {
			glog.Warningf("can not read %s", fileName)
			err = fmt.Errorf("can not read %s", fileName)
			return
		}
		return
	}

	// 文件存在且可读
	hasVolumeInfoFile = true

	glog.V(1).Infof("maybeLoadVolumeInfo reads %s", fileName)

	// 读取文件内容
	fileData, readErr := os.ReadFile(fileName)
	if readErr != nil {
		glog.Warningf("fail to read %s : %v", fileName, readErr)
		err = fmt.Errorf("fail to read %s : %v", fileName, readErr)
		return

	}

	glog.V(1).Infof("maybeLoadVolumeInfo Unmarshal volume info %v", fileName)

	// 解析 JSON 数据到 VolumeInfo
	if err = jsonpb.Unmarshal(fileData, volumeInfo); err != nil {
		// 解析失败，尝试旧版本格式
		if oldVersionErr := tryOldVersionVolumeInfo(fileData, volumeInfo); oldVersionErr != nil {
			// 新旧格式都解析失败
			glog.Warningf("unmarshal error: %v oldFormat: %v", err, oldVersionErr)
			err = fmt.Errorf("unmarshal error: %w oldFormat: %v", err, oldVersionErr)
			return
		} else {
			// 旧格式解析成功，清除错误
			err = nil
		}
	}

	// 检查是否有远程文件
	if len(volumeInfo.GetFiles()) == 0 {
		// 没有远程文件
		return
	}

	// 有远程文件
	hasRemoteFile = true

	return
}

// SaveVolumeInfo 保存 Volume 信息到文件
//
// 功能：
// - 将 VolumeInfo 序列化为 JSON 格式
// - 使用缩进格式化（便于人工阅读和调试）
// - 检查文件写入权限
//
// 参数：
//   - fileName: 目标文件路径（.vif 文件）
//   - volumeInfo: 要保存的 Volume 信息
//
// 返回值：
//   - error: 写入错误
//
// JSON 格式示例：
//
//	{
//	  "version": 3,
//	  "replication": "001",
//	  "files": [
//	    {
//	      "backend_type": "s3",
//	      "backend_name": "my-bucket",
//	      "key": "volumes/1.dat"
//	    }
//	  ],
//	  "bytes_offset": 1048576,
//	  "dat_file_size": 10485760,
//	  "expire_at_sec": 1704067200,
//	  "read_only": false
//	}
func SaveVolumeInfo(fileName string, volumeInfo *volume_server_pb.VolumeInfo) error {

	// 检查文件是否存在且可写
	if exists, _, canWrite, _, _ := util.CheckFile(fileName); exists && !canWrite {
		return fmt.Errorf("failed to check %s not writable", fileName)
	}

	// 配置 JSON 序列化选项
	m := jsonpb.MarshalOptions{
		AllowPartial:    true, // 允许部分字段为空
		EmitUnpopulated: true, // 输出未填充的字段（显示默认值）
		Indent:          "  ", // 使用 2 个空格缩进
	}

	// 将 VolumeInfo 序列化为 JSON
	text, marshalErr := m.Marshal(volumeInfo)
	if marshalErr != nil {
		return fmt.Errorf("failed to marshal %s: %v", fileName, marshalErr)
	}

	// 写入文件（权限 0644：所有者读写，组和其他用户只读）
	if err := util.WriteFile(fileName, text, 0644); err != nil {
		return fmt.Errorf("failed to write %s: %v", fileName, err)
	}

	return nil
}

// tryOldVersionVolumeInfo 尝试解析旧版本的 VolumeInfo 格式
//
// 旧版本格式差异：
// - 使用 DestroyTime 而非 ExpireAtSec
// - 部分字段名称不同
//
// 参数：
//   - data: JSON 字节数据
//   - volumeInfo: 输出参数，解析后的结果
//
// 返回值：
//   - error: 解析错误
//
// 注意：
// - 此函数用于向后兼容，处理旧版本 SeaweedFS 生成的 .vif 文件
// - 解析成功后，字段会被映射到新版本的 VolumeInfo 结构
func tryOldVersionVolumeInfo(data []byte, volumeInfo *volume_server_pb.VolumeInfo) error {
	// 尝试解析为旧版本格式
	oldVersionVolumeInfo := &volume_server_pb.OldVersionVolumeInfo{}
	if err := jsonpb.Unmarshal(data, oldVersionVolumeInfo); err != nil {
		return fmt.Errorf("failed to unmarshal old version volume info: %w", err)
	}

	// 将旧版本字段映射到新版本
	volumeInfo.Files = oldVersionVolumeInfo.Files
	volumeInfo.Version = oldVersionVolumeInfo.Version
	volumeInfo.Replication = oldVersionVolumeInfo.Replication
	volumeInfo.BytesOffset = oldVersionVolumeInfo.BytesOffset
	volumeInfo.DatFileSize = oldVersionVolumeInfo.DatFileSize
	// 字段名变化：DestroyTime -> ExpireAtSec
	volumeInfo.ExpireAtSec = oldVersionVolumeInfo.DestroyTime
	volumeInfo.ReadOnly = oldVersionVolumeInfo.ReadOnly

	return nil
}
