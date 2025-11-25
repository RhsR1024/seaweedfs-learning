package command

import (
	"fmt"
	"github.com/posener/complete"
	completeinstall "github.com/posener/complete/cmd/install"
	flag "github.com/seaweedfs/seaweedfs/weed/util/fla9"
	"os"
	"path/filepath"
	"runtime"
)

// AutocompleteMain 是命令行自动补全的主入口函数
//
// 功能说明：
//  1. 解析所有可用的命令及其标志（flags）
//  2. 构建自动补全的命令树结构
//  3. 注册全局标志和帮助子命令
//  4. 启动自动补全引擎进行补全匹配
//
// 参数：
//   commands - 所有可用的 SeaweedFS 命令列表（如 volume, master, filer 等）
//
// 返回值：
//   bool - 如果当前处于自动补全模式返回 true，否则返回 false
//
// 工作原理：
//   当用户在终端按 Tab 键时，shell 会设置 COMP_LINE 等环境变量，
//   然后调用本程序。程序检测到这些环境变量后，解析命令上下文，
//   返回可能的补全选项给 shell 显示。
func AutocompleteMain(commands []*Command) bool {
	// subCommands 存储所有子命令的补全配置
	// 键：命令名（如 "volume", "master"）
	// 值：该命令的补全配置（包括其标志）
	subCommands := make(map[string]complete.Command)

	// helpSubCommands 存储帮助命令的子命令列表
	// 用于 "weed help <subcommand>" 的补全
	helpSubCommands := make(map[string]complete.Command)

	// 遍历所有命令，为每个命令构建补全配置
	for _, cmd := range commands {
		// flags 存储当前命令的所有标志
		// 键：标志名（如 "-port"）
		// 值：补全预测器（这里使用 PredictAnything 表示接受任意输入）
		flags := make(map[string]complete.Predictor)

		// 访问命令的所有标志，将其注册到补全系统
		// 示例：对于 "weed volume -port=8080"，会注册 "-port" 标志
		cmd.Flag.VisitAll(func(flag *flag.Flag) {
			// 为每个标志添加 "-" 前缀，并设置为接受任意值
			flags["-"+flag.Name] = complete.PredictAnything
		})

		// 创建子命令的补全配置
		// 示例：subCommands["volume"] = {Flags: {"-port": ..., "-mserver": ...}}
		subCommands[cmd.Name()] = complete.Command{
			Flags: flags,
		}

		// 将命令名添加到帮助子命令列表
		// 用于补全 "weed help volume"
		helpSubCommands[cmd.Name()] = complete.Command{}
	}

	// 注册 "help" 命令，其子命令是所有可用命令
	// 示例：允许补全 "weed help vol<Tab>" -> "weed help volume"
	subCommands["help"] = complete.Command{Sub: helpSubCommands}

	// globalFlags 存储全局标志（适用于所有命令）
	// 示例："-v" 用于显示版本
	globalFlags := make(map[string]complete.Predictor)

	// 访问全局标志集，注册所有全局标志
	// 这些标志可以在任何子命令后使用
	flag.VisitAll(func(flag *flag.Flag) {
		globalFlags["-"+flag.Name] = complete.PredictAnything
	})

	// 构建 weed 命令的完整补全配置
	weedCmd := complete.Command{
		Sub:         subCommands,  // 子命令列表（volume, master, filer 等）
		Flags:       globalFlags,  // 全局标志列表
		GlobalFlags: complete.Flags{"-h": complete.PredictNothing}, // -h 标志不需要参数
	}

	// 创建自动补全对象
	// "weed" 是命令名，weedCmd 是补全配置
	cmp := complete.New("weed", weedCmd)

	// 执行补全逻辑
	// 如果检测到 COMP_LINE 环境变量，说明处于补全模式，
	// 会输出补全建议并返回 true
	// 否则返回 false，程序继续正常执行
	return cmp.Complete()
}

// printAutocompleteScript 生成指定 shell 的自动补全脚本
//
// 功能说明：
//   根据不同的 shell 类型（bash/zsh/fish），生成相应的补全脚本，
//   并输出到标准输出。用户可以将输出添加到 shell 配置文件中。
//
// 参数：
//   shell - shell 类型，支持 "bash", "zsh", "fish"
//
// 返回值：
//   bool - 成功返回 true，失败返回 false
//
// 使用示例：
//   weed autocomplete bash >> ~/.bashrc
//   eval "$(weed autocomplete zsh)"
func printAutocompleteScript(shell string) bool {
	// 获取当前执行文件的路径
	// 补全脚本需要知道 weed 可执行文件的位置，
	// 以便在补全时调用它
	bin, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get executable path: %s\n", err)
		return false
	}

	// 转换为绝对路径
	// 确保在任何目录下都能正确调用 weed 命令
	binPath, err := filepath.Abs(bin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get absolute path: %s\n", err)
		return false
	}

	// 根据不同的 shell 生成对应的补全脚本
	switch shell {
	case "bash":
		// Bash 补全脚本
		// complete -C 指定补全命令，当用户按 Tab 时，
		// shell 会调用 weed 程序并设置相关环境变量
		fmt.Printf("complete -C %q weed\n", binPath)

	case "zsh":
		// Zsh 补全脚本
		// 首先加载 bashcompinit 以支持 bash 风格的补全
		fmt.Printf("autoload -U +X bashcompinit && bashcompinit\n")
		// 然后注册补全命令，-o nospace 表示补全后不添加空格
		fmt.Printf("complete -o nospace -C %q weed\n", binPath)

	case "fish":
		// Fish shell 补全脚本
		// Fish 使用函数来定义补全逻辑
		fmt.Printf(`function __complete_weed
    set -lx COMP_LINE (commandline -cp)
    test -z (commandline -ct)
    and set COMP_LINE "$COMP_LINE "
    %q
end
complete -f -c weed -a "(__complete_weed)"
`, binPath)
		// 解释：
		// - set -lx COMP_LINE 设置当前命令行内容
		// - commandline -cp 获取光标前的命令
		// - test -z (commandline -ct) 检查光标处是否为空
		// - complete -f -c weed 为 weed 命令注册补全
		// - -a "(__complete_weed)" 指定补全函数

	default:
		// 不支持的 shell 类型
		fmt.Fprintf(os.Stderr, "unsupported shell: %s. Supported shells: bash, zsh, fish\n", shell)
		return false
	}
	return true
}

// installAutoCompletion 自动安装自动补全到 shell 配置文件
//
// 功能说明：
//   自动检测用户的 shell 类型，并将补全脚本添加到相应的配置文件
//   （如 ~/.bashrc, ~/.zshrc, ~/.config/fish/config.fish）
//
// 返回值：
//   bool - 成功返回 true，失败返回 false
//
// 注意：
//   - Windows 系统不支持此功能
//   - 安装后需要重启 shell 或执行 source 命令才能生效
func installAutoCompletion() bool {
	// 检查操作系统
	// Windows 不支持 Unix-style 的补全机制
	if runtime.GOOS == "windows" {
		fmt.Println("Windows is not supported")
		return false
	}

	// 调用 posener/complete 库的安装函数
	// 该函数会自动检测 shell 类型并修改配置文件
	// 示例：在 ~/.bashrc 中添加 "complete -C /path/to/weed weed"
	err := completeinstall.Install("weed")
	if err != nil {
		fmt.Printf("install failed! %s\n", err)
		return false
	}

	// 提示用户重启 shell
	// 因为配置文件修改需要重新加载才能生效
	fmt.Printf("autocompletion is enabled. Please restart your shell.\n")
	return true
}

// uninstallAutoCompletion 从 shell 配置文件中卸载自动补全
//
// 功能说明：
//   移除之前通过 installAutoCompletion 添加的补全配置
//
// 返回值：
//   bool - 成功返回 true，失败返回 false
//
// 注意：
//   - Windows 系统不支持此功能
//   - 卸载后需要重启 shell 才能生效
func uninstallAutoCompletion() bool {
	// 检查操作系统
	if runtime.GOOS == "windows" {
		fmt.Println("Windows is not supported")
		return false
	}

	// 调用卸载函数
	// 该函数会从 shell 配置文件中删除补全相关的配置
	err := completeinstall.Uninstall("weed")
	if err != nil {
		fmt.Printf("uninstall failed! %s\n", err)
		return false
	}

	// 提示用户重启 shell
	fmt.Printf("autocompletion is disabled. Please restart your shell.\n")
	return true
}

// cmdAutocomplete 是 "weed autocomplete" 命令的定义
//
// 命令用途：
//   生成或安装 shell 自动补全脚本，提升命令行使用体验
//
// 使用方式：
//   weed autocomplete [bash|zsh|fish]  # 打印补全脚本到标准输出
//   weed autocomplete install          # 自动安装到 shell 配置
//   weed autocomplete                  # 默认行为：自动安装
//
// 典型场景：
//   1. 手动安装：weed autocomplete bash >> ~/.bashrc
//   2. 使用 eval：eval "$(weed autocomplete zsh)"
//   3. 自动安装：weed autocomplete install
var cmdAutocomplete = &Command{
	Run:       runAutocomplete,  // 执行函数
	UsageLine: "autocomplete [shell]",  // 用法说明
	Short:     "generate or install shell autocomplete script",  // 简短描述
	Long: `Generate shell autocomplete script or install it to your shell configuration.

Usage:
    weed autocomplete [bash|zsh|fish]  # print autocomplete script to stdout
    weed autocomplete install          # install to shell config files

    When a shell name is provided, the autocomplete script is printed to stdout.
    You can then add it to your shell configuration manually, e.g.:

        # For bash:
        weed autocomplete bash >> ~/.bashrc

        # Or use eval in your shell config:
        eval "$(weed autocomplete bash)"

    When 'install' is provided (or no argument), the script is automatically
    installed to your shell configuration files.

    Supported shells are bash, zsh, and fish.
    Windows is not supported.

`,
}

// runAutocomplete 是 "weed autocomplete" 命令的执行函数
//
// 参数：
//   cmd  - 命令对象本身
//   args - 命令行参数列表
//
// 返回值：
//   bool - 成功返回 true，失败返回 false
//
// 执行逻辑：
//   1. 无参数：默认执行安装
//   2. 参数为 "install"：执行安装
//   3. 参数为 shell 名称：打印对应的补全脚本
//   4. 参数过多：显示用法并返回失败
func runAutocomplete(cmd *Command, args []string) bool {
	// 情况 1：无参数，默认行为是安装
	// 示例：weed autocomplete
	if len(args) == 0 {
		// Default behavior: install
		return installAutoCompletion()
	}

	// 情况 2：参数过多（只接受 0 或 1 个参数）
	// 示例：weed autocomplete bash zsh（错误）
	if len(args) > 1 {
		cmd.Usage()  // 显示用法帮助
		return false
	}

	// 获取第一个参数
	shell := args[0]

	// 情况 3：参数为 "install"，执行安装
	// 示例：weed autocomplete install
	if shell == "install" {
		return installAutoCompletion()
	}

	// 情况 4：参数为 shell 名称，打印补全脚本
	// 示例：weed autocomplete bash
	// Print the autocomplete script for the specified shell
	return printAutocompleteScript(shell)
}

// cmdUnautocomplete 是 "weed autocomplete.uninstall" 命令的定义
//
// 命令用途：
//   从 shell 配置中卸载自动补全功能
//
// 使用方式：
//   weed autocomplete.uninstall
//
// 注意：
//   - 该命令不接受任何参数
//   - Windows 系统不支持
//   - 卸载后需要重启 shell 才能生效
var cmdUnautocomplete = &Command{
	Run:       runUnautocomplete,  // 执行函数
	UsageLine: "autocomplete.uninstall",  // 用法说明
	Short:     "uninstall autocomplete",  // 简短描述
	Long: `weed autocomplete is uninstalled in the shell.

    Windows is not supported.

`,
}

// runUnautocomplete 是 "weed autocomplete.uninstall" 命令的执行函数
//
// 参数：
//   cmd  - 命令对象本身
//   args - 命令行参数列表（应该为空）
//
// 返回值：
//   bool - 成功返回 true，失败返回 false
//
// 执行逻辑：
//   检查参数数量，如果有参数则显示用法，然后执行卸载
func runUnautocomplete(cmd *Command, args []string) bool {
	// 检查参数数量
	// 该命令不接受任何参数
	if len(args) != 0 {
		cmd.Usage()  // 显示用法帮助
	}

	// 执行卸载操作
	// 会从 shell 配置文件中移除补全相关的配置
	return uninstallAutoCompletion()
}
