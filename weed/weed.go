// SeaweedFS 主程序入口
// SeaweedFS 是一个分布式文件系统，用于存储和服务数十亿个文件
// 本文件是整个 SeaweedFS 系统的命令行入口
package main

import (
	"embed"  // Go 1.16+ 的嵌入文件系统支持，用于将静态资源嵌入到二进制文件中
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"sync"          // 用于并发控制，这里主要是 exitStatus 的线程安全
	"text/template" // 用于渲染帮助信息的模板
	"time"
	"unicode"
	"unicode/utf8"

	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	"github.com/seaweedfs/seaweedfs/weed/util"
	flag "github.com/seaweedfs/seaweedfs/weed/util/fla9" // 自定义的 flag 解析库

	"github.com/getsentry/sentry-go" // Sentry 错误追踪服务
	"github.com/seaweedfs/seaweedfs/weed/command"
	"github.com/seaweedfs/seaweedfs/weed/glog" // Google 风格的日志库
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

// IsDebug 全局调试标志，由具体的命令设置
// 用于控制是否输出详细的调试信息
var IsDebug *bool

// commands 存储所有可用的子命令列表
// 包括 master, volume, filer, s3, mount 等所有 SeaweedFS 的功能模块
var commands = command.Commands

// exitStatus 程序退出状态码
// 0 = 成功, 1 = 一般错误, 2 = 语法错误
var exitStatus = 0

// exitMu 保护 exitStatus 的互斥锁，确保多个 goroutine 并发修改时的安全性
var exitMu sync.Mutex

// setExitStatus 线程安全地设置退出状态码
// 总是保留最高的错误级别（数值越大，错误越严重）
func setExitStatus(n int) {
	exitMu.Lock()
	if exitStatus < n {
		exitStatus = n
	}
	exitMu.Unlock()
}

// go:embed static
// static 嵌入 static 目录下的所有静态资源文件
// 这些文件会被编译到二进制文件中，无需外部文件依赖
var static embed.FS

// init 包初始化函数，在 main 函数之前自动执行
func init() {
	// 从嵌入的文件系统中提取 static 子目录，供 Web 服务器使用
	// 这些静态资源用于 Filer、Master 等组件的 Web UI
	weed_server.StaticFS, _ = fs.Sub(static, "static")

	// 注册 config_dir 命令行参数，用于指定配置文件目录
	// SeaweedFS 支持使用 TOML 格式的配置文件
	flag.Var(&util.ConfigurationFileDirectory, "config_dir", "directory with toml configuration files")
}

// main 是程序的主入口函数
// 功能流程：
// 1. 初始化日志和监控系统
// 2. 处理命令行自动补全
// 3. 解析命令行参数
// 4. 根据子命令分发到对应的处理函数
func main() {
	// 设置日志文件的最大大小为 10MB
	// 当日志文件超过这个大小时会自动轮转
	glog.MaxSize = 1024 * 1024 * 10

	// 设置日志文件的最大保留数量为 5 个
	// 超过数量的旧日志文件会被删除
	glog.MaxFileCount = 5

	// 设置自定义的 usage 函数，当用户输入错误参数时显示帮助信息
	flag.Usage = usage

	// 初始化 Sentry 错误追踪服务
	// Sentry 用于收集生产环境中的错误和性能数据
	err := sentry.Init(sentry.ClientOptions{
		SampleRate:       0.1, // 采样率 10%，只上报 10% 的错误（避免过多数据）
		EnableTracing:    true, // 启用性能追踪功能
		TracesSampleRate: 0.1,  // 追踪采样率 10%
	})
	if err != nil {
		// Sentry 初始化失败不影响程序运行，仅输出错误信息
		fmt.Fprintf(os.Stderr, "sentry.Init: %v", err)
	}
	// Flush buffered events before the program terminates.
	// Set the timeout to the maximum duration the program can afford to wait.
	// 确保程序退出前将所有缓冲的事件发送到 Sentry
	// 最多等待 2 秒钟
	defer sentry.Flush(2 * time.Second)

	// 处理命令行自动补全功能
	// 如果用户在 shell 中触发了自动补全（如按 Tab 键），这里会返回补全建议
	// 如果处理了自动补全请求，直接返回，不继续执行后续逻辑
	if command.AutocompleteMain(commands) {
		return
	}

	// 解析命令行参数（全局参数，如日志级别等）
	flag.Parse()

	// 获取解析后的所有非 flag 参数（即子命令和子命令的参数）
	// 例如：weed master -port=9333 中，args = ["master", "-port=9333"]
	args := flag.Args()
	if len(args) < 1 {
		// 如果没有提供任何子命令，显示用法说明并退出
		usage()
	}

	// 特殊处理 help 命令
	// 用法：weed help [command]
	if args[0] == "help" {
		help(args[1:])
		// 如果请求的是特定命令的帮助信息，额外打印该命令的参数默认值
		for _, cmd := range commands {
			if len(args) >= 2 && cmd.Name() == args[1] && cmd.Run != nil {
				fmt.Fprintf(os.Stderr, "Default Parameters:\n")
				cmd.Flag.PrintDefaults()
			}
		}
		return
	}

	// 初始化全局 HTTP 客户端
	// 用于各个组件之间的 HTTP 通信（如 Volume Server 与 Master Server 之间）
	util_http.InitGlobalHttpClient()

	// 遍历所有注册的命令，查找匹配用户输入的子命令
	for _, cmd := range commands {
		// 检查命令名称是否匹配，且命令是可运行的（有 Run 函数）
		if cmd.Name() == args[0] && cmd.Run != nil {
			// 设置命令的 Usage 函数
			cmd.Flag.Usage = func() { cmd.Usage() }

			// 解析该子命令的专属参数
			// 例如：weed master -port=9333 会解析 -port=9333
			cmd.Flag.Parse(args[1:])

			// 获取子命令参数解析后剩余的参数
			args = cmd.Flag.Args()

			// 设置全局调试标志
			IsDebug = cmd.IsDebug

			// 执行子命令的 Run 函数
			// 如果返回 false，表示命令执行失败
			if !cmd.Run(cmd, args) {
				fmt.Fprintf(os.Stderr, "\n")
				cmd.Flag.Usage()
				fmt.Fprintf(os.Stderr, "Default Parameters:\n")
				cmd.Flag.PrintDefaults()
				// Command execution failed - general error
				// 命令执行失败，设置退出状态码为 1
				setExitStatus(1)
			}
			// 执行清理工作并退出程序
			exit()
			return
		}
	}

	// Unknown command - syntax error
	// 如果没有找到匹配的子命令，说明用户输入了未知的命令
	fmt.Fprintf(os.Stderr, "weed: unknown subcommand %q\nRun 'weed help' for usage.\n", args[0])
	// 设置退出状态码为 2（语法错误）
	setExitStatus(2)
	exit()
}

// usageTemplate 是使用说明的模板字符串
// 使用 Go 的 text/template 语法
// {{range .}} 遍历所有命令，{{if .Runnable}} 只显示可运行的命令
var usageTemplate = `
SeaweedFS: store billions of files and serve them fast!

Usage:

	weed command [arguments]

The commands are:
{{range .}}{{if .Runnable}}
    {{.Name | printf "%-11s"}} {{.Short}}{{end}}{{end}}

Use "weed help [command]" for more information about a command.

`

// helpTemplate 是单个命令帮助信息的模板
// 显示命令的用法和详细描述
var helpTemplate = `{{if .Runnable}}Usage: weed {{.UsageLine}}
{{end}}
  {{.Long}}
`

// tmpl executes the given template text on data, writing the result to w.
// tmpl 执行模板渲染，将数据 data 应用到模板 text 上，结果写入 w
// 参数：
//   w: 输出目标（如 os.Stdout 或 os.Stderr）
//   text: 模板文本
//   data: 要渲染的数据
func tmpl(w io.Writer, text string, data interface{}) {
	// 创建新的模板对象
	t := template.New("top")
	// 注册自定义的模板函数
	// trim: 去除字符串首尾空白
	// capitalize: 将字符串首字母大写
	t.Funcs(template.FuncMap{"trim": strings.TrimSpace, "capitalize": capitalize})
	// 解析模板，Must 会在解析失败时 panic
	template.Must(t.Parse(text))
	// 执行模板渲染
	if err := t.Execute(w, data); err != nil {
		panic(err)
	}
}

// capitalize 将字符串的首字母转换为大写
// 用于模板渲染中的格式化
func capitalize(s string) string {
	if s == "" {
		return s
	}
	// 解码第一个 UTF-8 字符（支持多字节字符）
	r, n := utf8.DecodeRuneInString(s)
	// 返回首字母大写 + 剩余部分
	return string(unicode.ToTitle(r)) + s[n:]
}

// printUsage 打印使用说明到指定的输出流
func printUsage(w io.Writer) {
	tmpl(w, usageTemplate, commands)
}

// usage 显示完整的使用说明
// 包括所有可用的命令和全局日志选项
// 调用此函数后会设置退出状态码为 2 并退出程序
func usage() {
	printUsage(os.Stderr)
	fmt.Fprintf(os.Stderr, "For Logging, use \"weed [logging_options] [command]\". The logging options are:\n")
	flag.PrintDefaults()
	// Invalid command line usage - syntax error
	// 无效的命令行用法 - 语法错误
	setExitStatus(2)
	exit()
}

// help implements the 'help' command.
// help 实现 'help' 命令的处理逻辑
// 用法：
//   weed help           - 显示所有命令的列表
//   weed help [command] - 显示特定命令的详细帮助
func help(args []string) {
	if len(args) == 0 {
		// 没有指定具体命令，显示所有命令的概览
		printUsage(os.Stdout)
		// Success - help displayed correctly
		// 成功显示帮助信息
		return
	}
	if len(args) != 1 {
		// 参数个数错误，help 命令只接受 0 或 1 个参数
		fmt.Fprintf(os.Stderr, "usage: weed help command\n\nToo many arguments given.\n")
		// Invalid help usage - syntax error
		// 无效的 help 用法 - 语法错误
		setExitStatus(2)
		exit()
	}

	// 获取要查询帮助的命令名称
	arg := args[0]

	// 遍历所有命令，查找匹配的命令
	for _, cmd := range commands {
		if cmd.Name() == arg {
			// 找到匹配的命令，显示其详细帮助信息
			tmpl(os.Stdout, helpTemplate, cmd)
			// Success - help for specific command displayed correctly
			// 成功显示特定命令的帮助信息
			return
		}
	}

	// 没有找到匹配的命令
	fmt.Fprintf(os.Stderr, "Unknown help topic %#q.  Run 'weed help'.\n", arg)
	// Unknown help topic - syntax error
	// 未知的帮助主题 - 语法错误
	setExitStatus(2)
	exit()
}

// atexitFuncs 存储程序退出前需要执行的清理函数列表
// 这些函数会在 exit() 被调用时按注册顺序执行
var atexitFuncs []func()

// atexit 注册一个在程序退出时需要执行的清理函数
// 类似 C 语言的 atexit() 函数
// 用于资源清理、连接关闭等善后工作
func atexit(f func()) {
	atexitFuncs = append(atexitFuncs, f)
}

// exit 执行所有注册的清理函数，然后退出程序
// 按照函数注册的顺序依次执行清理
// 最后使用 exitStatus 作为退出码调用 os.Exit()
func exit() {
	// 依次执行所有注册的清理函数
	for _, f := range atexitFuncs {
		f()
	}
	// 使用设置的退出状态码退出程序
	os.Exit(exitStatus)
}

// debug 输出调试级别的日志信息
// 只有当日志级别设置为 4 或更高时才会输出
// glog.V(4) 表示详细调试级别（级别越高越详细）
func debug(params ...interface{}) {
	glog.V(4).Infoln(params...)
}
