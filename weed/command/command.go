// Package command 定义了 SeaweedFS 的所有子命令
// 每个子命令负责启动不同的服务组件或执行特定的操作
package command

import (
	"fmt"
	"os"
	"strings"

	flag "github.com/seaweedfs/seaweedfs/weed/util/fla9"
)

// Commands 是所有可用命令的注册列表
// SeaweedFS 采用子命令模式，每个命令对应一个特定的功能
// 主要的命令包括：
//   - master: 启动 Master Server（元数据管理，卷分配）
//   - volume: 启动 Volume Server（实际存储数据）
//   - filer: 启动 Filer Server（文件系统接口）
//   - s3: 启动 S3 兼容的对象存储接口
//   - mount: 挂载为 FUSE 文件系统
//   - server: 一次性启动 master + volume + filer（适合开发测试）
var Commands = []*Command{
	cmdAdmin,                  // 管理命令（锁定/解锁卷等）
	cmdAutocomplete,           // 启用命令行自动补全
	cmdUnautocomplete,         // 禁用命令行自动补全
	cmdBackup,                 // 备份卷数据
	cmdBenchmark,              // 性能基准测试
	cmdCompact,                // 压缩卷文件，回收空间
	cmdDownload,               // 下载文件
	cmdExport,                 // 导出卷数据
	cmdFiler,                  // 启动 Filer Server（文件服务器）
	cmdFilerBackup,            // 备份 Filer 元数据
	cmdFilerCat,               // 查看 Filer 中的文件内容
	cmdFilerCopy,              // 在 Filer 之间复制文件
	cmdFilerMetaBackup,        // 备份 Filer 元数据（增量）
	cmdFilerMetaTail,          // 实时监控 Filer 元数据变化
	cmdFilerRemoteGateway,     // Filer 远程网关（跨数据中心）
	cmdFilerRemoteSynchronize, // Filer 远程同步
	cmdFilerReplicate,         // Filer 复制
	cmdFilerSynchronize,       // Filer 同步
	cmdFix,                    // 修复卷数据
	cmdFuse,                   // FUSE 挂载（已废弃，使用 mount）
	cmdIam,                    // IAM 身份认证服务
	cmdMaster,                 // 启动 Master Server（核心组件）
	cmdMasterFollower,         // 启动 Master Follower（高可用）
	cmdMount,                  // FUSE 挂载文件系统
	cmdMqAgent,                // 消息队列 Agent
	cmdMqBroker,               // 消息队列 Broker
	cmdMqKafkaGateway,         // Kafka 兼容的消息队列网关
	cmdDB,                     // 数据库操作命令
	cmdS3,                     // 启动 S3 API 服务器
	cmdScaffold,               // 生成配置文件模板
	cmdServer,                 // 一体化服务器（master+volume+filer）
	cmdShell,                  // 交互式 Shell
	cmdSql,                    // SQL 查询接口
	cmdUpdate,                 // 更新 SeaweedFS 版本
	cmdUpload,                 // 上传文件
	cmdVersion,                // 显示版本信息
	cmdVolume,                 // 启动 Volume Server（存储节点）
	cmdWebDav,                 // 启动 WebDAV 服务器
	cmdSftp,                   // 启动 SFTP 服务器
	cmdWorker,                 // 后台工作进程
}

// Command 表示一个可执行的子命令
// 每个命令包含执行逻辑、参数定义和帮助文档
type Command struct {
	// Run runs the command.
	// The args are the arguments after the command name.
	// Run 执行命令的主逻辑函数
	// args 是命令名称之后的参数列表
	// 返回值：true 表示成功，false 表示失败
	Run func(cmd *Command, args []string) bool

	// UsageLine is the one-line usage message.
	// The first word in the line is taken to be the command name.
	// UsageLine 是单行的用法说明
	// 第一个单词被识别为命令名称
	// 例如："master -port=9333 -mdir=/tmp/master"
	UsageLine string

	// Short is the short description shown in the 'go help' output.
	// Short 是在 'weed help' 输出中显示的简短描述
	// 通常是一句话说明命令的功能
	Short string

	// Long is the long message shown in the 'go help <this-command>' output.
	// Long 是在 'weed help <command>' 输出中显示的详细说明
	// 可以包含多行文本，详细描述命令的用法和示例
	Long string

	// Flag is a set of flags specific to this command.
	// Flag 是这个命令特有的参数集合
	// 每个命令有自己独立的参数定义（如端口号、目录等）
	Flag flag.FlagSet

	// IsDebug 指示是否启用调试模式
	// 由命令设置，影响日志输出的详细程度
	IsDebug *bool
}

// Name returns the command's name: the first word in the usage line.
// Name 返回命令的名称：UsageLine 中的第一个单词
// 例如："master -port=9333" 返回 "master"
func (c *Command) Name() string {
	name := c.UsageLine
	// 查找第一个空格的位置
	i := strings.Index(name, " ")
	if i >= 0 {
		// 截取空格之前的部分作为命令名
		name = name[:i]
	}
	return name
}

// Usage 打印命令的使用说明并退出程序
// 包括示例用法、参数列表和详细描述
func (c *Command) Usage() {
	fmt.Fprintf(os.Stderr, "Example: weed %s\n", c.UsageLine)
	fmt.Fprintf(os.Stderr, "Default Usage:\n")
	c.Flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "Description:\n")
	fmt.Fprintf(os.Stderr, "  %s\n", strings.TrimSpace(c.Long))
	os.Exit(2)
}

// Runnable reports whether the command can be run; otherwise
// it is a documentation pseudo-command such as importpath.
// Runnable 报告命令是否可以执行
// 如果 Run 为 nil，说明这只是一个文档伪命令，不能实际执行
func (c *Command) Runnable() bool {
	return c.Run != nil
}
