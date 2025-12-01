// Package main 实现了修改 Volume SuperBlock 的离线工具
//
// 功能说明：
// 此工具用于在 Volume Server 离线状态下修改 .dat 文件的 SuperBlock 头部信息
// 可以修改的属性包括：
//   - 副本策略 (Replication)：如 "000"、"001"、"100" 等
//   - TTL (Time To Live)：文件过期时间设置
//
// 使用场景：
//   1. 需要调整已有 Volume 的副本策略
//   2. 需要修改 Volume 的 TTL 设置
//   3. 需要在不重新创建 Volume 的情况下调整存储策略
//
// 注意事项：
//   - 必须在 Volume Server 停止后使用
//   - 修改会直接写入 .dat 文件的前 8 字节（SuperBlock）
//   - 修改后需要确保 .idx 索引文件与 .dat 文件匹配
package main

import (
	"flag"
	"fmt"
	"os"
	"path"
	"strconv"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

// 命令行参数定义
var (
	// fixVolumePath Volume 文件所在的目录路径
	// 默认值：/tmp
	fixVolumePath = flag.String("dir", "/tmp", "data directory to store files")

	// fixVolumeCollection Volume 所属的集合名称
	// 如果设置了 collection，文件名格式为：{collection}_{volumeId}.dat
	// 如果未设置，文件名格式为：{volumeId}.dat
	// 默认值：空字符串（无集合）
	fixVolumeCollection = flag.String("collection", "", "the volume collection name")

	// fixVolumeId 要修改的 Volume ID
	// 必须是已存在的 Volume，工具会打开对应的 .dat 文件
	// 默认值：-1（无效值，必须指定）
	fixVolumeId = flag.Int("volumeId", -1, "a volume id. The volume should already exist in the dir. The volume index file should not exist.")

	// targetReplica 目标副本策略
	// 格式：三位数字字符串，如 "000"、"001"、"100"
	// 如果为空字符串，则仅显示当前副本策略，不进行修改
	// 默认值：空字符串（仅查看模式）
	targetReplica = flag.String("replication", "", "If just empty, only print out current replication setting.")

	// targetTTL 目标 TTL（Time To Live）设置
	// 格式：数字 + 单位，如 "3m"（3分钟）、"2h"（2小时）、"5d"（5天）
	// 如果为空字符串，则仅显示当前 TTL，不进行修改
	// 默认值：空字符串（仅查看模式）
	targetTTL = flag.String("ttl", "", "If just empty, only print out current ttl setting.")
)

/*
工具使用说明：

本工具用于修改 Volume 文件 (.dat) 头部的副本策略或 TTL 设置
使用前必须关闭拥有这些 Volume 的 Volume Server

操作步骤：

1. 在本地修改 .dat 文件
   // 仅查看当前副本策略和 TTL
   go run change_superblock.go -volumeId=9 -dir=/Users/chrislu/Downloads
   输出：
   Current Volume Replication: 000
   Current Volume TTL: (empty)

   // 修改副本策略为 001（同机架不同服务器 1 个副本）
   go run change_superblock.go -volumeId=9 -dir=/Users/chrislu/Downloads -replication 001
   输出：
   Current Volume Replication: 000
   Current Volume TTL: (empty)
   Changing replication to: 001
   Change Applied.

   // 修改 TTL 为 3 天
   go run change_superblock.go -volumeId=9 -dir=/Users/chrislu/Downloads -ttl 3d
   输出：
   Current Volume Replication: 001
   Current Volume TTL: (empty)
   Changing ttl to: 3d
   Change Applied.

   // 同时修改副本策略和 TTL
   go run change_superblock.go -volumeId=9 -dir=/Users/chrislu/Downloads -replication 100 -ttl 7d
   输出：
   Current Volume Replication: 001
   Current Volume TTL: (empty)
   Changing replication to: 100
   Changing ttl to: 7d
   Change Applied.

2. 将修改后的 .dat 文件和相关的 .idx 文件复制到远程服务器
   注意：必须同时复制 .dat 和 .idx 文件，保持它们的一致性

3. 重启 Volume Server 或启动新的 Volume Server
   Volume Server 会读取新的 SuperBlock 配置

副本策略格式说明：
  - "000": 无副本
  - "001": 同机架不同服务器 1 个副本
  - "010": 同数据中心不同机架 1 个副本
  - "100": 不同数据中心 1 个副本
  - "200": 不同数据中心 2 个副本

TTL 格式说明：
  - "3m": 3 分钟
  - "2h": 2 小时
  - "5d": 5 天
  - "2w": 2 周
  - "1M": 1 个月
  - "1y": 1 年
*/
func main() {
	// 解析命令行参数
	flag.Parse()

	// 初始化全局 HTTP 客户端
	// 虽然本工具不直接使用 HTTP，但某些依赖包可能需要
	util_http.NewGlobalHttpClient()

	// 【步骤 1：构造 Volume 文件名】
	// 将 volumeId 转换为字符串，如：9 -> "9"
	fileName := strconv.Itoa(*fixVolumeId)

	// 如果指定了 collection，则在文件名前加上 collection 前缀
	// 例如：collection="images", volumeId=9 -> "images_9"
	if *fixVolumeCollection != "" {
		fileName = *fixVolumeCollection + "_" + fileName
	}

	// 【步骤 2：打开 .dat 文件】
	// 拼接完整路径：dir + fileName + ".dat"
	// 使用 O_RDWR 模式（读写模式），权限 0644
	// 注意：必须是已存在的文件，不会创建新文件
	datFile, err := os.OpenFile(path.Join(*fixVolumePath, fileName+".dat"), os.O_RDWR, 0644)
	if err != nil {
		glog.Fatalf("Open Volume Data File [ERROR]: %v", err)
	}

	// 创建磁盘后端接口
	// backend.DiskFile 封装了文件操作，提供统一的存储接口
	datBackend := backend.NewDiskFile(datFile)
	defer datBackend.Close()

	// 【步骤 3：读取 SuperBlock】
	// SuperBlock 位于 .dat 文件的前 8 字节
	// 包含：Version(1字节) + ReplicaPlacement(1字节) + TTL(2字节) + CompactionRevision(2字节) + Padding(2字节)
	// 详见：weed/storage/super_block/super_block.go
	superBlock, err := super_block.ReadSuperBlock(datBackend)

	if err != nil {
		glog.Fatalf("cannot parse existing super block: %v", err)
	}

	// 【步骤 4：显示当前配置】
	// 打印当前的副本策略和 TTL 设置
	fmt.Printf("Current Volume Replication: %s\n", superBlock.ReplicaPlacement)
	fmt.Printf("Current Volume TTL: %s\n", superBlock.Ttl.String())

	// 【步骤 5：处理副本策略修改】
	// hasChange 标记是否有配置变更，用于控制是否写入磁盘
	hasChange := false

	// 如果用户指定了目标副本策略（-replication 参数非空）
	if *targetReplica != "" {
		// 解析副本策略字符串，如 "001"、"100" 等
		// NewReplicaPlacementFromString 会验证格式是否正确
		// 格式要求：三位数字，每位范围 0-9
		replica, err := super_block.NewReplicaPlacementFromString(*targetReplica)

		if err != nil {
			// 解析失败，可能的原因：
			// 1. 不是三位数字（如 "00"、"0001"）
			// 2. 包含非数字字符（如 "abc"）
			// 3. 数字超出范围（如 "999" 可能不合法）
			glog.Fatalf("cannot parse target replica %s: %v", *targetReplica, err)
		}

		// 显示即将应用的新副本策略
		fmt.Printf("Changing replication to: %s\n", replica)

		// 更新 SuperBlock 中的副本策略字段
		// 注意：此时仅修改内存中的对象，尚未写入磁盘
		superBlock.ReplicaPlacement = replica
		hasChange = true
	}

	// 【步骤 6：处理 TTL 修改】
	// 如果用户指定了目标 TTL（-ttl 参数非空）
	if *targetTTL != "" {
		// 解析 TTL 字符串，如 "3m"、"2h"、"5d" 等
		// ReadTTL 会将字符串转换为内部 TTL 结构
		// 支持的单位：m(分钟)、h(小时)、d(天)、w(周)、M(月)、y(年)
		ttl, err := needle.ReadTTL(*targetTTL)

		if err != nil {
			// 解析失败，可能的原因：
			// 1. 格式错误（如 "3x"、"abc"）
			// 2. 数值超出范围
			// 3. 单位不支持
			glog.Fatalf("cannot parse target ttl %s: %v", *targetTTL, err)
		}

		// 显示即将应用的新 TTL 设置
		fmt.Printf("Changing ttl to: %s\n", ttl)

		// 更新 SuperBlock 中的 TTL 字段
		// 注意：此时仅修改内存中的对象，尚未写入磁盘
		superBlock.Ttl = ttl
		hasChange = true
	}

	// 【步骤 7：写入 SuperBlock】
	// 只有在有变更的情况下才写入磁盘
	if hasChange {

		// 将 SuperBlock 对象序列化为字节数组
		// SuperBlock 固定为 8 字节：
		//   [0]    - Version (1 字节)
		//   [1]    - ReplicaPlacement (1 字节)
		//   [2-3]  - TTL (2 字节，大端序)
		//   [4-5]  - CompactionRevision (2 字节)
		//   [6-7]  - Padding (2 字节，保留)
		header := superBlock.Bytes()

		// 将序列化后的 SuperBlock 写入 .dat 文件的开头（offset=0）
		// WriteAt 参数：
		//   - header: 要写入的数据（8 字节）
		//   - 0: 写入位置（文件开头）
		// 返回值：
		//   - n: 实际写入的字节数
		//   - e: 错误信息
		if n, e := datBackend.WriteAt(header, 0); n == 0 || e != nil {
			// 写入失败的可能原因：
			// 1. 磁盘空间不足
			// 2. 文件权限问题
			// 3. 磁盘 I/O 错误
			// 4. 文件已被其他进程占用
			glog.Fatalf("cannot write super block: %v", e)
		}

		// 写入成功，提示用户
		// 注意：修改后需要重启 Volume Server 才能生效
		fmt.Println("Change Applied.")
	}

}
