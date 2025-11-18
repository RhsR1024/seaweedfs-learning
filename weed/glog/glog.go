// Go 支持分级日志,类似于 https://code.google.com/p/google-glog/
//
// Copyright 2013 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package glog 实现了类似 Google 内部 C++ INFO/ERROR/V 设置的日志功能
// 它提供了 Info、Warning、Error、Fatal 等函数,以及格式化变体如 Infof
// 同时支持通过 -v 和 -vmodule=file=2 标志控制的 V 风格日志
//
// 基本示例:
//
//	glog.Info("准备击退入侵者")
//
//	glog.Fatalf("初始化失败: %s", err)
//
// 关于 V 函数的使用示例说明:
//
//	if glog.V(2) {
//		glog.Info("启动事务...")
//	}
//
//	glog.V(2).Infoln("已处理", nItems, "个元素")
//
// 日志输出会被缓冲并定期使用 Flush 写入
// 程序应在退出前调用 Flush 以确保所有日志输出被写入
//
// 默认情况下,所有日志语句都写入临时目录中的文件
// 本包提供了几个修改此行为的标志
// 因此,在进行任何日志记录之前必须调用 flag.Parse
//
//	-logtostderr=false
//		日志写入标准错误而不是文件
//	-alsologtostderr=false
//		日志除了写入文件外也写入标准错误
//	-stderrthreshold=ERROR
//		此级别及以上的日志事件除了写入文件外也写入标准错误
//	-logdir=""
//		日志文件将写入此目录而不是默认临时目录
//
//	其他标志提供调试辅助
//
//	-log_backtrace_at=""
//		设置为文件和行号(如 -log_backtrace_at=gopherflakes.go:234)
//		当执行到该语句时,会将堆栈跟踪写入 Info 日志
//		(与 -vmodule 不同,必须包含 ".go")
//	-v=0
//		启用指定级别的 V 分级日志
//	-vmodule=""
//		参数语法是逗号分隔的 pattern=N 列表
//		pattern 是字面文件名(不含 ".go" 后缀)或 "glob" 模式
//		N 是 V 级别。例如:
//			-vmodule=gopher*=3
//		将所有以 "gopher" 开头的 Go 文件的 V 级别设为 3
package glog

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	stdLog "log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	flag "github.com/seaweedfs/seaweedfs/weed/util/fla9"
)

// severity 标识日志的严重程度类型:info、warning 等
// 它也实现了 flag.Value 接口
// -stderrthreshold 标志是 severity 类型,只能通过 flag.Value 接口修改
// 这些值与 C++ 中的相应常量匹配
type severity int32 // sync/atomic int32

// These constants identify the log levels in order of increasing severity.
// 这些常量按严重程度递增顺序标识日志级别
// 写入高严重度日志文件的消息也会写入每个较低严重度的日志文件
const (
	infoLog severity = iota // INFO 级别: 一般信息
	warningLog              // WARNING 级别: 警告信息
	errorLog                // ERROR 级别: 错误信息
	fatalLog                // FATAL 级别: 致命错误,会导致程序退出
	numSeverity = 4         // 日志级别总数
)

// severityChar 日志级别的单字符表示,用于日志头部
// I=Info, W=Warning, E=Error, F=Fatal
const severityChar = "IWEF"

// severityName 日志级别的完整名称数组
var severityName = []string{
	infoLog:    "INFO",
	warningLog: "WARNING",
	errorLog:   "ERROR",
	fatalLog:   "FATAL",
}

// get returns the value of the severity.
// get 返回 severity 的值,使用原子操作保证线程安全
func (s *severity) get() severity {
	return severity(atomic.LoadInt32((*int32)(s)))
}

// set sets the value of the severity.
// set 设置 severity 的值,使用原子操作保证线程安全
func (s *severity) set(val severity) {
	atomic.StoreInt32((*int32)(s), int32(val))
}

// String is part of the flag.Value interface.
// String 是 flag.Value 接口的一部分,返回 severity 的字符串表示
func (s *severity) String() string {
	return strconv.FormatInt(int64(*s), 10)
}

// Get is part of the flag.Value interface.
// Get 是 flag.Value 接口的一部分,返回 severity 的当前值
func (s *severity) Get() interface{} {
	return *s
}

// Set is part of the flag.Value interface.
// Set 是 flag.Value 接口的一部分,从字符串设置 severity 值
// 支持名称(INFO/WARNING/ERROR/FATAL)或数值形式
func (s *severity) Set(value string) error {
	var threshold severity
	// Is it a known name?
	// 是否为已知名称?
	if v, ok := severityByName(value); ok {
		threshold = v
	} else {
		v, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		threshold = severity(v)
	}
	logging.stderrThreshold.set(threshold)
	return nil
}

// severityByName 通过名称查找对应的 severity 级别
// 返回 severity 值和是否找到的布尔值
func severityByName(s string) (severity, bool) {
	s = strings.ToUpper(s)
	for i, name := range severityName {
		if name == s {
			return severity(i), true
		}
	}
	return 0, false
}

// OutputStats 跟踪输出行数和写入的字节数
type OutputStats struct {
	lines int64 // 输出的日志行数
	bytes int64 // 输出的字节数
}

// Lines 返回已写入的行数
func (s *OutputStats) Lines() int64 {
	return atomic.LoadInt64(&s.lines)
}

// Bytes 返回已写入的字节数
func (s *OutputStats) Bytes() int64 {
	return atomic.LoadInt64(&s.bytes)
}

// Stats 跟踪每个严重程度级别的输出行数和字节数
// 值必须使用 atomic.LoadInt64 读取
var Stats struct {
	Info, Warning, Error OutputStats
}

// severityStats 将 severity 级别映射到对应的统计信息
var severityStats = [numSeverity]*OutputStats{
	infoLog:    &Stats.Info,
	warningLog: &Stats.Warning,
	errorLog:   &Stats.Error,
}

// Level 是导出的,因为它出现在 V 的参数中,是 v 标志的类型,可以通过编程方式设置
// 它是一个不同的类型,因为我们想将其与 logType 区分开
// level 类型的变量只在 logging.mu 下更改
// -v 标志只通过原子操作读取,因此日志模块的状态是一致的

// Level 被视为 sync/atomic int32

// Level 指定 V 日志的详细程度级别
// *Level 实现 flag.Value; -v 标志是 Level 类型,只能通过 flag.Value 接口修改
type Level int32

// get 返回 Level 的值
func (l *Level) get() Level {
	return Level(atomic.LoadInt32((*int32)(l)))
}

// set 设置 Level 的值
func (l *Level) set(val Level) {
	atomic.StoreInt32((*int32)(l), int32(val))
}

// String is part of the flag.Value interface.
// String 是 flag.Value 接口的一部分,返回 Level 的字符串表示
func (l *Level) String() string {
	return strconv.FormatInt(int64(*l), 10)
}

// Get is part of the flag.Value interface.
// Get 是 flag.Value 接口的一部分,返回 Level 的当前值
func (l *Level) Get() interface{} {
	return *l
}

// Set is part of the flag.Value interface.
// Set 是 flag.Value 接口的一部分,从字符串解析并设置 Level 值
func (l *Level) Set(value string) error {
	v, err := strconv.Atoi(value)
	if err != nil {
		return err
	}
	logging.mu.Lock()
	defer logging.mu.Unlock()
	logging.setVState(Level(v), logging.vmodule.filter, false)
	return nil
}

// moduleSpec 表示 -vmodule 标志的设置
// 用于按文件粒度控制日志级别
type moduleSpec struct {
	filter []modulePat // 文件模式过滤器列表
}

// modulePat 包含 -vmodule 标志的过滤器
// 它持有详细程度级别和要匹配的文件模式
type modulePat struct {
	pattern string // 文件名模式(可以包含通配符)
	literal bool   // 模式是否为字面字符串(不含通配符)
	level   Level  // 该模式对应的日志级别
}

// match 报告文件是否与模式匹配
// 如果模式不包含元字符,则使用字符串比较
func (m *modulePat) match(file string) bool {
	if m.literal {
		return file == m.pattern
	}
	match, _ := filepath.Match(m.pattern, file)
	return match
}

// String 是 flag.Value 接口的一部分
// 返回 vmodule 设置的字符串表示
func (m *moduleSpec) String() string {
	// 因为类型不是原子的,所以需要加锁。TODO: 清理这个
	logging.mu.Lock()
	defer logging.mu.Unlock()
	var b bytes.Buffer
	for i, f := range m.filter {
		if i > 0 {
			b.WriteRune(',')
		}
		fmt.Fprintf(&b, "%s=%d", f.pattern, f.level)
	}
	return b.String()
}

// Get 是 (Go 1.2) flag.Getter 接口的一部分
// 由于结构体未导出,总是为此标志类型返回 nil
func (m *moduleSpec) Get() interface{} {
	return nil
}

// errVmoduleSyntax vmodule 语法错误
var errVmoduleSyntax = errors.New("syntax error: expect comma-separated list of filename=N")

// Set 是 flag.Value 接口的一部分
// 解析 vmodule 标志的值,语法: -vmodule=recordio=2,file=1,gfs*=3
func (m *moduleSpec) Set(value string) error {
	var filter []modulePat
	for _, pat := range strings.Split(value, ",") {
		if len(pat) == 0 {
			// 空字符串(如尾随逗号)可以忽略
			continue
		}
		patLev := strings.Split(pat, "=")
		if len(patLev) != 2 || len(patLev[0]) == 0 || len(patLev[1]) == 0 {
			return errVmoduleSyntax
		}
		pattern := patLev[0]
		v, err := strconv.Atoi(patLev[1])
		if err != nil {
			return errors.New("syntax error: expect comma-separated list of filename=N")
		}
		if v < 0 {
			return errors.New("negative value for vmodule level")
		}
		if v == 0 {
			continue // 忽略。这是无害的,但没有必要承担开销
		}
		// TODO: 检查过滤器的语法?
		filter = append(filter, modulePat{pattern, isLiteral(pattern), Level(v)})
	}
	logging.mu.Lock()
	defer logging.mu.Unlock()
	logging.setVState(logging.verbosity, filter, true)
	return nil
}

// isLiteral 报告模式是否为字面字符串,即没有元字符
// 需要调用 filepath.Match 来匹配模式
func isLiteral(pattern string) bool {
	return !strings.ContainsAny(pattern, `\*?[]`)
}

// traceLocation 表示 -log_backtrace_at 标志的设置
// 用于在特定代码位置打印堆栈跟踪
type traceLocation struct {
	file string // 文件名
	line int    // 行号
}

// isSet 报告是否已指定跟踪位置
// 需要持有 logging.mu 锁
func (t *traceLocation) isSet() bool {
	return t.line > 0
}

// match 报告指定的文件和行是否与跟踪位置匹配
// 参数 file 是完整路径,而非标志中指定的基本名称
// 需要持有 logging.mu 锁
func (t *traceLocation) match(file string, line int) bool {
	if t.line != line {
		return false
	}
	if i := strings.LastIndex(file, "/"); i >= 0 {
		file = file[i+1:]
	}
	return t.file == file
}

// String 是 flag.Value 接口的一部分
// 返回跟踪位置的字符串表示
func (t *traceLocation) String() string {
	// 因为类型不是原子的,所以需要加锁。TODO: 清理这个
	logging.mu.Lock()
	defer logging.mu.Unlock()
	return fmt.Sprintf("%s:%d", t.file, t.line)
}

// Get 是 (Go 1.2) flag.Getter 接口的一部分
// 由于结构体未导出,总是为此标志类型返回 nil
func (t *traceLocation) Get() interface{} {
	return nil
}

// errTraceSyntax 跟踪位置语法错误
var errTraceSyntax = errors.New("syntax error: expect file.go:234")

// Set 是 flag.Value 接口的一部分
// 解析跟踪位置,语法: -log_backtrace_at=gopherflakes.go:234
// 注意与 vmodule 不同,这里包含文件扩展名
func (t *traceLocation) Set(value string) error {
	if value == "" {
		// 取消设置
		t.line = 0
		t.file = ""
	}
	fields := strings.Split(value, ":")
	if len(fields) != 2 {
		return errTraceSyntax
	}
	file, line := fields[0], fields[1]
	if !strings.Contains(file, ".") {
		return errTraceSyntax
	}
	v, err := strconv.Atoi(line)
	if err != nil {
		return errTraceSyntax
	}
	if v <= 0 {
		return errors.New("negative or zero value for level")
	}
	logging.mu.Lock()
	defer logging.mu.Unlock()
	t.line = v
	t.file = file
	return nil
}

// flushSyncWriter 是日志目标满足的接口
// 组合了 io.Writer、Flush 和 Sync 功能
type flushSyncWriter interface {
	Flush() error // 刷新缓冲区到底层存储
	Sync() error  // 同步数据到磁盘
	io.Writer     // 写入接口
}

// init 初始化函数,注册命令行标志并启动刷新守护进程
func init() {
	// 注册各类日志控制标志
	flag.BoolVar(&logging.toStderr, "logtostderr", false, "log to standard error instead of files")
	flag.BoolVar(&logging.alsoToStderr, "alsologtostderr", true, "log to standard error as well as files")
	flag.Var(&logging.verbosity, "v", "log levels [0|1|2|3|4], default to 0")
	flag.Var(&logging.stderrThreshold, "stderrthreshold", "logs at or above this threshold go to stderr")
	flag.Var(&logging.vmodule, "vmodule", "comma-separated list of pattern=N settings for file-filtered logging")
	flag.Var(&logging.traceLocation, "log_backtrace_at", "when logging hits line file:N, emit a stack trace")

	// 默认 stderrThreshold 是 ERROR
	logging.stderrThreshold = errorLog

	// 初始化 V 日志状态
	logging.setVState(0, nil, false)
	// 启动后台刷新守护进程
	go logging.flushDaemon()
}

// Flush 刷新所有待处理的日志 I/O
func Flush() {
	logging.lockAndFlushAll()
}

// loggingT 收集日志设置的所有全局状态
type loggingT struct {
	// 布尔标志。不以原子方式处理,因为 flag.Value 接口
	// 不允许我们避免 =true,而这个简写对于兼容性是必需的
	// TODO: 这是否足够重要以至于需要修复?似乎不太可能
	toStderr     bool // -logtostderr 标志: 输出到标准错误而非文件
	alsoToStderr bool // -alsologtostderr 标志: 同时输出到标准错误和文件

	// Level 标志。以原子方式处理
	stderrThreshold severity // -stderrthreshold 标志: 此级别及以上输出到标准错误

	// freeList 是字节缓冲区列表,在 freeListMu 下维护
	freeList *buffer
	// freeListMu 维护空闲列表。它与主互斥锁分离
	// 这样缓冲区可以在不持有主锁的情况下被获取和打印,以提高并行性
	freeListMu sync.Mutex

	// mu 保护此结构的其余元素,用于同步日志记录
	mu sync.Mutex
	// file 持有每种日志类型的写入器
	file [numSeverity]flushSyncWriter
	// pcs 在 V 中用于避免在计算调用者 PC 时进行分配
	pcs [1]uintptr
	// vmap 是每个 V() 调用点的 V Level 缓存,通过 PC 标识
	// 每当 vmodule 标志更改状态时都会被清除
	vmap map[uintptr]Level
	// filterLength 存储 vmodule 过滤器链的长度
	// 如果大于零,表示启用了 vmodule
	// 可以使用 sync.LoadInt32 安全读取,但只能在 mu 下修改
	filterLength int32
	// traceLocation 是 -log_backtrace_at 标志的状态
	traceLocation traceLocation
	// 这些标志只在锁下修改,尽管 verbosity 可以使用 atomic.LoadInt32 安全获取
	vmodule   moduleSpec // -vmodule 标志的状态
	verbosity Level      // V 日志级别,-v 标志的值

	// added by seaweedfs
	exited bool // 标记程序是否已退出,避免退出后继续写日志
}

// buffer 持有一个字节 Buffer 以供重用。零值即可使用
type buffer struct {
	bytes.Buffer
	tmp  [64]byte // 用于创建头部的临时字节数组
	next *buffer  // 链表中的下一个缓冲区
}

// logging 全局日志实例
var logging loggingT

// setVState 为 V 日志设置一致的状态
// 需要持有 l.mu 锁
func (l *loggingT) setVState(verbosity Level, filter []modulePat, setFilter bool) {
	// 关闭详细程度,这样在转换时 V 不会触发
	logging.verbosity.set(0)
	// 过滤器长度同样设为 0
	atomic.StoreInt32(&logging.filterLength, 0)

	// 设置新过滤器,如果过滤器已更改,则清除 pc->Level 映射
	if setFilter {
		logging.vmodule.filter = filter
		logging.vmap = make(map[uintptr]Level)
	}

	// 现在一切都一致了,所以启用过滤和详细程度
	// 启用顺序与 V 函数中的相反
	atomic.StoreInt32(&logging.filterLength, int32(len(filter)))
	logging.verbosity.set(verbosity)
}

// getBuffer 返回一个新的、可立即使用的缓冲区
// 从空闲列表中获取,如果空闲列表为空则创建新的
func (l *loggingT) getBuffer() *buffer {
	l.freeListMu.Lock()
	b := l.freeList
	if b != nil {
		l.freeList = b.next
	}
	l.freeListMu.Unlock()
	if b == nil {
		b = new(buffer)
	} else {
		b.next = nil
		b.Reset()
	}
	return b
}

// putBuffer 将缓冲区返回到空闲列表
// 过大的缓冲区(>=256字节)不会被回收,让其自然消亡
func (l *loggingT) putBuffer(b *buffer) {
	if b.Len() >= 256 {
		// 让大缓冲区自然消亡
		return
	}
	l.freeListMu.Lock()
	b.next = l.freeList
	l.freeList = b
	l.freeListMu.Unlock()
}

// timeNow 当前时间函数,可以在测试时 stub
var timeNow = time.Now // Stubbed out for testing.

/*
header 根据 C++ 实现定义格式化日志头
它返回包含格式化头部的缓冲区以及用户的文件名和行号
depth 参数指定要在日志消息中标识的源代码行位于多少个栈帧之上

日志行的格式如下:

	Lmmdd hh:mm:ss.uuuuuu threadid file:line] msg...

其中各字段定义如下:

	L                单个字符,表示日志级别(例如 'I' 表示 INFO)
	mm               月份(零填充;即 5 月是 '05')
	dd               日期(零填充)
	hh:mm:ss.uuuuuu  时间:小时、分钟和小数秒
	threadid         空格填充的线程 ID(由 GetTID() 返回)
	file             文件名
	line             行号
	msg              The user-supplied message
*/
func (l *loggingT) header(s severity, depth int) (*buffer, string, int) {
	_, file, line, ok := runtime.Caller(3 + depth)
	if !ok {
		file = "???"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		if slash >= 0 {
			file = file[slash+1:]
		}
	}
	return l.formatHeader(s, file, line), file, line
}

// formatHeader formats a log header using the provided file name and line number.
// formatHeader 使用提供的文件名和行号格式化日志头
// 返回包含格式化头部的缓冲区
func (l *loggingT) formatHeader(s severity, file string, line int) *buffer {
	now := timeNow()
	if line < 0 {
		line = 0 // not a real line number, but acceptable to someDigits
		// 不是真实的行号,但 someDigits 可以接受
	}
	if s > fatalLog {
		s = infoLog // for safety.
		// 为安全起见
	}
	buf := l.getBuffer()

	// Avoid Fprintf, for speed. The format is so simple that we can do it quickly by hand.
	// It's worth about 3X. Fprintf is hard.
	// 为提高速度避免使用 Fprintf。格式很简单,可以手动快速完成。
	// 这样做大约快 3 倍。Fprintf 很慢。
	_, month, day := now.Date()
	hour, minute, second := now.Clock()
	// Lmmdd hh:mm:ss.uuuuuu threadid file:line]
	buf.tmp[0] = severityChar[s]
	buf.twoDigits(1, int(month))
	buf.twoDigits(3, day)
	buf.tmp[5] = ' '
	buf.twoDigits(6, hour)
	buf.tmp[8] = ':'
	buf.twoDigits(9, minute)
	buf.tmp[11] = ':'
	buf.twoDigits(12, second)
	buf.tmp[14] = '.'
	buf.nDigits(6, 15, now.Nanosecond()/1000, '0')
	buf.tmp[21] = ' '
	buf.Write(buf.tmp[:22])
	buf.WriteString(file)
	buf.tmp[0] = ':'
	n := buf.someDigits(1, line)
	buf.tmp[n+1] = ' '
	buf.Write(buf.tmp[:n+2])
	return buf
}

// Some custom tiny helper functions to print the log header efficiently.
// 一些自定义的小型辅助函数,用于高效打印日志头

const digits = "0123456789"

// twoDigits formats a zero-prefixed two-digit integer at buf.tmp[i].
// twoDigits 在 buf.tmp[i] 处格式化一个零前缀的两位整数
func (buf *buffer) twoDigits(i, d int) {
	buf.tmp[i+1] = digits[d%10]
	d /= 10
	buf.tmp[i] = digits[d%10]
}

// nDigits formats an n-digit integer at buf.tmp[i],
// padding with pad on the left.
// It assumes d >= 0.
// nDigits 在 buf.tmp[i] 处格式化一个 n 位整数
// 左侧用 pad 填充
// 假设 d >= 0
func (buf *buffer) nDigits(n, i, d int, pad byte) {
	j := n - 1
	for ; j >= 0 && d > 0; j-- {
		buf.tmp[i+j] = digits[d%10]
		d /= 10
	}
	for ; j >= 0; j-- {
		buf.tmp[i+j] = pad
	}
}

// someDigits formats a zero-prefixed variable-width integer at buf.tmp[i].
// someDigits 在 buf.tmp[i] 处格式化一个零前缀的可变宽度整数
func (buf *buffer) someDigits(i, d int) int {
	// Print into the top, then copy down. We know there's space for at least
	// a 10-digit number.
	// 先打印到顶部,然后向下复制。我们知道至少有10位数字的空间。
	j := len(buf.tmp)
	for {
		j--
		buf.tmp[j] = digits[d%10]
		d /= 10
		if d == 0 {
			break
		}
	}
	return copy(buf.tmp[i:], buf.tmp[j:])
}

// println 打印一行日志,使用 fmt.Fprintln 格式化参数
func (l *loggingT) println(s severity, args ...interface{}) {
	buf, file, line := l.header(s, 0)
	fmt.Fprintln(buf, args...)
	l.output(s, buf, file, line, false)
}

// print 打印日志,使用 fmt.Fprint 格式化参数
func (l *loggingT) print(s severity, args ...interface{}) {
	l.printDepth(s, 1, args...)
}

// printDepth 打印日志,可指定栈深度以确定调用位置
func (l *loggingT) printDepth(s severity, depth int, args ...interface{}) {
	buf, file, line := l.header(s, depth)
	fmt.Fprint(buf, args...)
	if buf.Bytes()[buf.Len()-1] != '\n' {
		buf.WriteByte('\n')
	}
	l.output(s, buf, file, line, false)
}

// printf 打印格式化日志,使用 fmt.Fprintf 格式化参数
func (l *loggingT) printf(s severity, format string, args ...interface{}) {
	buf, file, line := l.header(s, 0)
	fmt.Fprintf(buf, format, args...)
	if buf.Bytes()[buf.Len()-1] != '\n' {
		buf.WriteByte('\n')
	}
	l.output(s, buf, file, line, false)
}

// printWithFileLine behaves like print but uses the provided file and line number.  If
// alsoLogToStderr is true, the log message always appears on standard error; it
// will also appear in the log file unless --logtostderr is set.
// printWithFileLine 行为类似 print,但使用提供的文件名和行号
// 如果 alsoLogToStderr 为 true,日志消息总是显示在标准错误输出;
// 除非设置了 --logtostderr,否则它也会出现在日志文件中
func (l *loggingT) printWithFileLine(s severity, file string, line int, alsoToStderr bool, args ...interface{}) {
	buf := l.formatHeader(s, file, line)
	fmt.Fprint(buf, args...)
	if buf.Bytes()[buf.Len()-1] != '\n' {
		buf.WriteByte('\n')
	}
	l.output(s, buf, file, line, alsoToStderr)
}

// output writes the data to the log files and releases the buffer.
// output 将数据写入日志文件并释放缓冲区
// 这是日志写入的核心函数,处理日志级联、标准错误输出和文件写入
func (l *loggingT) output(s severity, buf *buffer, file string, line int, alsoToStderr bool) {
	l.mu.Lock()
	if l.traceLocation.isSet() {
		if l.traceLocation.match(file, line) {
			buf.Write(stacks(false))
		}
	}
	data := buf.Bytes()
	if l.toStderr {
		os.Stderr.Write(data)
	} else {
		if alsoToStderr || l.alsoToStderr || s >= l.stderrThreshold.get() {
			os.Stderr.Write(data)
		}
		if l.file[s] == nil {
			if err := l.createFiles(s); err != nil {
				os.Stderr.Write(data) // Make sure the message appears somewhere.
				// 确保消息至少显示在某处
				l.exit(err)
			}
		}
		// After exit is called, don't try to write to files
		// 调用 exit 后,不要尝试写入文件
		if !l.exited {
			switch s {
			case fatalLog:
				if l.file[fatalLog] != nil {
					l.file[fatalLog].Write(data)
				}
				fallthrough
			case errorLog:
				if l.file[errorLog] != nil {
					l.file[errorLog].Write(data)
				}
				fallthrough
			case warningLog:
				if l.file[warningLog] != nil {
					l.file[warningLog].Write(data)
				}
				fallthrough
			case infoLog:
				if l.file[infoLog] != nil {
					l.file[infoLog].Write(data)
				}
			}
		}
	}
	if s == fatalLog {
		// If we got here via Exit rather than Fatal, print no stacks.
		// 如果通过 Exit 而非 Fatal 到达这里,不打印堆栈
		if atomic.LoadUint32(&fatalNoStacks) > 0 {
			l.mu.Unlock()
			timeoutFlush(10 * time.Second)
			os.Exit(1)
		}
		// Dump all goroutine stacks before exiting.
		// First, make sure we see the trace for the current goroutine on standard error.
		// If -logtostderr has been specified, the loop below will do that anyway
		// as the first stack in the full dump.
		// 退出前转储所有 goroutine 堆栈
		// 首先,确保在标准错误上看到当前 goroutine 的跟踪
		// 如果指定了 -logtostderr,下面的循环无论如何都会这样做
		// 作为完整转储中的第一个堆栈
		if !l.toStderr {
			os.Stderr.Write(stacks(false))
		}
		// Write the stack trace for all goroutines to the files.
		// 将所有 goroutine 的堆栈跟踪写入文件
		trace := stacks(true)
		logExitFunc = func(error) {} // If we get a write error, we'll still exit below.
		// 如果遇到写入错误,下面仍会退出
		for log := fatalLog; log >= infoLog; log-- {
			if f := l.file[log]; f != nil { // Can be nil if -logtostderr is set.
				// 如果设置了 -logtostderr 可以为 nil
				f.Write(trace)
			}
		}
		l.mu.Unlock()
		timeoutFlush(10 * time.Second)
		os.Exit(255) // C++ uses -1, which is silly because it's anded with 255 anyway.
		// C++ 使用 -1,这很愚蠢,因为无论如何都会与 255 进行与运算
	}
	l.putBuffer(buf)
	l.mu.Unlock()
	if stats := severityStats[s]; stats != nil {
		atomic.AddInt64(&stats.lines, 1)
		atomic.AddInt64(&stats.bytes, int64(len(data)))
	}
}

// timeoutFlush calls Flush and returns when it completes or after timeout
// elapses, whichever happens first.  This is needed because the hooks invoked
// by Flush may deadlock when glog.Fatal is called from a hook that holds
// a lock.
// timeoutFlush 调用 Flush 并在完成或超时后返回,以先发生者为准
// 这是必需的,因为当从持有锁的钩子调用 glog.Fatal 时,
// Flush 调用的钩子可能会死锁
func timeoutFlush(timeout time.Duration) {
	done := make(chan bool, 1)
	go func() {
		Flush() // calls logging.lockAndFlushAll()
		// 调用 logging.lockAndFlushAll()
		done <- true
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		fmt.Fprintln(os.Stderr, "glog: Flush took longer than", timeout)
	}
}

// stacks is a wrapper for runtime.Stack that attempts to recover the data for all goroutines.
// stacks 是 runtime.Stack 的包装器,尝试恢复所有 goroutine 的数据
func stacks(all bool) []byte {
	// We don't know how big the traces are, so grow a few times if they don't fit. Start large, though.
	// 我们不知道跟踪有多大,所以如果不合适就增长几次。不过要从大的开始。
	n := 10000
	if all {
		n = 100000
	}
	var trace []byte
	for i := 0; i < 5; i++ {
		trace = make([]byte, n)
		nbytes := runtime.Stack(trace, all)
		if nbytes < len(trace) {
			return trace[:nbytes]
		}
		n *= 2
	}
	return trace
}

// logExitFunc provides a simple mechanism to override the default behavior
// of exiting on error. Used in testing and to guarantee we reach a required exit
// for fatal logs. Instead, exit could be a function rather than a method but that
// would make its use clumsier.
// logExitFunc 提供了一种简单的机制来覆盖错误时退出的默认行为
// 用于测试并保证我们到达致命日志所需的退出点
// exit 可以是函数而不是方法,但那样使用起来会更笨拙
var logExitFunc func(error)

// exit is called if there is trouble creating or writing log files.
// It flushes the logs and exits the program; there's no point in hanging around.
// l.mu is held.
// exit 在创建或写入日志文件时遇到问题时调用
// 它刷新日志并退出程序;没有必要继续运行
// 需要持有 l.mu 锁
func (l *loggingT) exit(err error) {
	fmt.Fprintf(os.Stderr, "glog: exiting because of error: %s\n", err)
	// If logExitFunc is set, we do that instead of exiting.
	// 如果设置了 logExitFunc,我们执行它而不是退出
	if logExitFunc != nil {
		logExitFunc(err)
		return
	}
	l.flushAll()
	l.exited = true // os.Exit(2)
}

// syncBuffer joins a bufio.Writer to its underlying file, providing access to the
// file's Sync method and providing a wrapper for the Write method that provides log
// file rotation. There are conflicting methods, so the file cannot be embedded.
// l.mu is held for all its methods.
// syncBuffer 将 bufio.Writer 连接到其底层文件,提供对文件 Sync 方法的访问
// 并为 Write 方法提供包装,实现日志文件轮转
// 由于方法冲突,文件不能嵌入
// 所有方法都需要持有 l.mu 锁
type syncBuffer struct {
	logger *loggingT
	*bufio.Writer
	file   *os.File
	sev    severity
	nbytes uint64 // The number of bytes written to this file
	// 写入此文件的字节数
}

func (sb *syncBuffer) Sync() error {
	return sb.file.Sync()
}

func (sb *syncBuffer) Write(p []byte) (n int, err error) {
	if sb.logger.exited {
		return
	}
	// Check if Writer is nil (can happen if rotateFile failed)
	if sb.Writer == nil {
		return 0, errors.New("log writer is nil")
	}
	if sb.nbytes+uint64(len(p)) >= MaxSize {
		if err := sb.rotateFile(time.Now()); err != nil {
			sb.logger.exit(err)
			return 0, err
		}
	}
	n, err = sb.Writer.Write(p)
	sb.nbytes += uint64(n)
	if err != nil {
		sb.logger.exit(err)
	}
	return
}

// rotateFile closes the syncBuffer's file and starts a new one.
//
// rotateFile 关闭 syncBuffer 当前的日志文件并创建一个新文件
//
// 参数:
//   now: 当前时间,用于生成新文件名和写入文件头
//
// 返回值:
//   error: 创建文件或写入文件头时的错误
//
// 日志轮转流程:
// 1. 刷新并关闭当前日志文件
// 2. 根据时间戳和严重级别创建新日志文件
// 3. 重置已写入字节计数器
// 4. 创建新的带缓冲的写入器(bufferSize 大小)
// 5. 写入日志文件头信息
//
// 文件头内容:
// - 创建时间: Log file created at: 2006/01/02 15:04:05
// - 运行机器: Running on machine: hostname
// - 编译信息: Built with go1.x for linux/amd64
// - 日志格式: [IWEF]mmdd hh:mm:ss threadid file:line] msg
//
// 缓冲区配置:
// - 使用 256KB 缓冲区大小(bufferSize)
// - 减少磁盘 I/O,提高日志写入性能
// - flushDaemon 定期刷新缓冲区
//
// 触发时机:
// - 当前日志文件大小达到 MaxSize
// - 程序启动时创建初始日志文件
func (sb *syncBuffer) rotateFile(now time.Time) error {
	if sb.file != nil {
		sb.Flush()
		sb.file.Close()
	}
	var err error
	sb.file, _, err = create(severityName[sb.sev], now)
	sb.nbytes = 0
	if err != nil {
		return err
	}

	sb.Writer = bufio.NewWriterSize(sb.file, bufferSize)

	// Write header.
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "Log file created at: %s\n", now.Format("2006/01/02 15:04:05"))
	fmt.Fprintf(&buf, "Running on machine: %s\n", host)
	fmt.Fprintf(&buf, "Binary: Built with %s %s for %s/%s\n", runtime.Compiler, runtime.Version(), runtime.GOOS, runtime.GOARCH)
	fmt.Fprintf(&buf, "Log line format: [IWEF]mmdd hh:mm:ss threadid file:line] msg\n")
	n, err := sb.file.Write(buf.Bytes())
	sb.nbytes += uint64(n)
	return err
}

// bufferSize sizes the buffer associated with each log file. It's large
// so that log records can accumulate without the logging thread blocking
// on disk I/O. The flushDaemon will block instead.
//
// bufferSize 定义与每个日志文件关联的缓冲区大小(256KB)
//
// 设计考虑:
// - 缓冲区足够大,使日志记录可以累积而不会阻塞日志记录线程的磁盘 I/O
// - 将磁盘 I/O 阻塞转移到后台 flushDaemon 守护进程
// - 提高日志写入性能,减少对主业务逻辑的影响
//
// 工作机制:
// - 日志先写入内存缓冲区(快速操作)
// - flushDaemon 定期将缓冲区内容刷新到磁盘
// - 即使有大量日志,也不会影响应用程序性能
//
// 权衡:
// - 优点: 高性能,减少磁盘 I/O 频率
// - 缺点: 程序异常崩溃时可能丢失缓冲区中的日志
// - 通过 Flush() 和 flushDaemon 定期刷新来降低丢失风险
const bufferSize = 256 * 1024

// createFiles creates all the log files for severity from sev down to infoLog.
// l.mu is held.
//
// createFiles 创建从指定严重级别 sev 到 infoLog 的所有日志文件
//
// 参数:
//   sev: 要创建的日志文件的严重级别
//
// 返回值:
//   error: 创建文件失败时的错误
//
// 创建策略:
// - 按严重级别递减顺序创建文件(从 sev 到 infoLog)
// - 如果某个级别的文件已存在,则停止(因为更低级别的文件也已存在)
// - 例如:创建 ERROR 级别时,会同时创建 WARNING 和 INFO 级别
//
// 日志级联写入机制:
// - FATAL 日志会写入: FATAL、ERROR、WARNING、INFO 四个文件
// - ERROR 日志会写入: ERROR、WARNING、INFO 三个文件
// - WARNING 日志会写入: WARNING、INFO 两个文件
// - INFO 日志只写入: INFO 一个文件
//
// 初始化内容:
// - 为每个级别创建 syncBuffer 结构
// - 调用 rotateFile 创建实际文件并写入文件头
// - 设置缓冲写入器
//
// 线程安全:
// - 需要持有 l.mu 锁
// - 调用者必须在调用前获取锁
func (l *loggingT) createFiles(sev severity) error {
	now := time.Now()
	// Files are created in decreasing severity order, so as soon as we find one
	// has already been created, we can stop.
	for s := sev; s >= infoLog && l.file[s] == nil; s-- {
		sb := &syncBuffer{
			logger: l,
			sev:    s,
		}
		if err := sb.rotateFile(now); err != nil {
			return err
		}
		l.file[s] = sb
	}
	return nil
}

// flushInterval 日志缓冲区刷新间隔,设置为 30 秒
//
// 说明:
// - flushDaemon 守护进程每隔 30 秒自动刷新一次所有日志文件缓冲区
// - 确保日志及时写入磁盘,降低程序崩溃时的日志丢失风险
// - 平衡性能和数据安全性
const flushInterval = 30 * time.Second

// flushDaemon periodically flushes the log file buffers.
//
// flushDaemon 定期刷新日志文件缓冲区的后台守护进程
//
// 工作机制:
// - 在 init() 函数中作为 goroutine 启动
// - 使用 time.NewTicker 创建定时器,每 30 秒触发一次
// - 每次触发时调用 lockAndFlushAll() 刷新所有日志文件
//
// 刷新操作:
// - 将内存缓冲区中的日志数据写入文件系统
// - 调用 Sync() 确保数据持久化到磁盘
// - 覆盖所有严重级别(INFO、WARNING、ERROR、FATAL)
//
// 重要性:
// - 防止长时间运行的程序积累过多未刷新的日志
// - 在程序异常终止时减少日志丢失
// - 确保日志文件内容相对实时
//
// 注意:
// - 此守护进程会一直运行直到程序退出
// - 程序正常退出前应调用 Flush() 确保所有日志已写入
func (l *loggingT) flushDaemon() {
	for _ = range time.NewTicker(flushInterval).C {
		l.lockAndFlushAll()
	}
}

// lockAndFlushAll is like flushAll but locks l.mu first.
//
// lockAndFlushAll 与 flushAll 类似,但会先获取 l.mu 锁
//
// 功能:
// - 线程安全版本的 flushAll
// - 先获取互斥锁,确保并发安全
// - 调用 flushAll() 执行实际刷新操作
// - 最后释放锁
//
// 调用场景:
// - flushDaemon 守护进程定期调用
// - 用户手动调用 Flush() 函数
// - 需要从外部安全地刷新日志时
//
// 与 flushAll 的区别:
// - lockAndFlushAll: 公共接口,自动处理锁
// - flushAll: 内部接口,要求调用者已持有锁
func (l *loggingT) lockAndFlushAll() {
	l.mu.Lock()
	l.flushAll()
	l.mu.Unlock()
}

// flushAll flushes all the logs and attempts to "sync" their data to disk.
// l.mu is held.
//
// flushAll 刷新所有日志文件并尝试将数据同步到磁盘
//
// 刷新顺序:
// - 从 FATAL 级别向下刷新到 INFO 级别
// - 顺序: FATAL -> ERROR -> WARNING -> INFO
// - 如果高级别刷新出现问题,至少低级别的日志已安全
//
// 操作步骤(针对每个级别):
// 1. Flush(): 将缓冲区内容写入操作系统
// 2. Sync(): 强制操作系统将数据持久化到磁盘
//
// 错误处理:
// - 忽略 Flush 和 Sync 的错误
// - 尽力而为策略,不因个别文件失败而影响其他文件
//
// 线程安全:
// - 需要持有 l.mu 锁
// - 调用者必须先获取锁
//
// 使用场景:
// - flushDaemon 定期调用
// - 程序退出前调用
// - Fatal 日志记录前调用
func (l *loggingT) flushAll() {
	// Flush from fatal down, in case there's trouble flushing.
	for s := fatalLog; s >= infoLog; s-- {
		file := l.file[s]
		if file != nil {
			file.Flush() // ignore error
			file.Sync()  // ignore error
		}
	}
}

// CopyStandardLogTo arranges for messages written to the Go "log" package's
// default logs to also appear in the Google logs for the named and lower
// severities.  Subsequent changes to the standard log's default output location
// or format may break this behavior.
//
// Valid names are "INFO", "WARNING", "ERROR", and "FATAL".  If the name is not
// recognized, CopyStandardLogTo panics.
//
// CopyStandardLogTo 将 Go 标准库 "log" 包的默认日志消息重定向到 glog
// 使得标准日志输出也会出现在指定严重级别及更低级别的 Google 日志中
// 后续对标准日志默认输出位置或格式的更改可能会破坏此行为
//
// 参数:
//   name: 日志级别名称,有效值为 "INFO", "WARNING", "ERROR" 和 "FATAL"
//
// 功能说明:
// - 将标准日志的输出通过 logBridge 桥接到 glog
// - 设置标准日志格式为 Lshortfile(文件名:行号)
// - 自动解析标准日志的文件名和行号信息
// - 如果提供的名称无法识别,函数会 panic
//
// 使用示例:
//   glog.CopyStandardLogTo("ERROR") // 标准日志将输出到 ERROR 级别
//   log.Println("这条消息将同时出现在标准日志和 glog 的 ERROR 日志中")
func CopyStandardLogTo(name string) {
	sev, ok := severityByName(name)
	if !ok {
		panic(fmt.Sprintf("log.CopyStandardLogTo(%q): unrecognized severity name", name))
	}
	// Set a log format that captures the user's file and line:
	//   d.go:23: message
	stdLog.SetFlags(stdLog.Lshortfile)
	stdLog.SetOutput(logBridge(sev))
}

// logBridge provides the Write method that enables CopyStandardLogTo to connect
// Go's standard logs to the logs provided by this package.
//
// logBridge 提供 Write 方法,使 CopyStandardLogTo 能够将 Go 标准日志连接到本包提供的日志系统
//
// 设计说明:
// - logBridge 实际上是 severity 类型的别名,表示要桥接到的日志级别
// - 通过实现 io.Writer 接口,可以作为标准日志的输出目标
// - 它充当适配器,将标准日志格式转换为 glog 格式
//
// 工作原理:
// 1. 标准日志按照 "文件名:行号: 消息" 格式输出
// 2. logBridge.Write() 解析这个格式
// 3. 提取文件名、行号和消息内容
// 4. 调用 glog 的 printWithFileLine 输出到相应级别
//
// 类型定义:
// - 基于 severity 类型,值为 infoLog/warningLog/errorLog/fatalLog 之一
// - 使用类型别名而非结构体,保持简洁高效
type logBridge severity

// Write parses the standard logging line and passes its components to the
// logger for severity(lb).
//
// Write 实现 io.Writer 接口,解析标准日志行并将其组件传递给对应严重级别的日志记录器
//
// 参数:
//   b: 标准日志输出的原始字节数据,格式为 "文件名:行号: 消息"
//
// 返回值:
//   n: 写入的字节数(总是返回输入的长度)
//   err: 错误信息(本函数总是返回 nil)
//
// 解析过程:
// 1. 期望输入格式: "d.go:23: message"
// 2. 使用 ':' 分隔符分割成 3 部分:
//    - parts[0]: 文件名 "d.go"
//    - parts[1]: 行号 "23"
//    - parts[2]: ": message" (包含前导空格)
// 3. 提取并转换各部分数据
// 4. 调用 printWithFileLine 输出到 glog
//
// 错误处理:
// - 如果格式不正确,将整个输入作为错误消息记录
// - 如果行号无法解析,使用默认行号 1 并记录错误消息
// - 所有标准日志消息都会同时输出到标准错误(alsoToStderr=true)
//
// 注意事项:
// - 跳过消息部分的前导空格 (parts[2][1:])
// - 即使解析失败也会记录日志,保证消息不丢失
func (lb logBridge) Write(b []byte) (n int, err error) {
	var (
		file = "???"
		line = 1
		text string
	)
	// Split "d.go:23: message" into "d.go", "23", and "message".
	// 将 "d.go:23: message" 分割为 "d.go", "23" 和 "message"
	if parts := bytes.SplitN(b, []byte{':'}, 3); len(parts) != 3 || len(parts[0]) < 1 || len(parts[2]) < 1 {
		text = fmt.Sprintf("bad log format: %s", b)
	} else {
		file = string(parts[0])
		text = string(parts[2][1:]) // skip leading space
		// 跳过前导空格
		line, err = strconv.Atoi(string(parts[1]))
		if err != nil {
			text = fmt.Sprintf("bad line number: %s", b)
			line = 1
		}
	}
	// printWithFileLine with alsoToStderr=true, so standard log messages
	// always appear on standard error.
	// 使用 alsoToStderr=true 调用 printWithFileLine,因此标准日志消息总是显示在标准错误输出
	logging.printWithFileLine(severity(lb), file, line, true, text)
	return len(b), nil
}

// setV computes and remembers the V level for a given PC
// when vmodule is enabled.
// File pattern matching takes the basename of the file, stripped
// of its .go suffix, and uses filepath.Match, which is a little more
// general than the *? matching used in C++.
// l.mu is held.
//
// setV 在启用 vmodule 时,计算并记住给定程序计数器(PC)的 V 日志级别
//
// 参数:
//   pc: 程序计数器(program counter),标识代码中的特定位置
//
// 返回值:
//   Level: 该代码位置对应的日志级别
//
// 工作流程:
// 1. 通过 PC 获取函数信息和文件路径
// 2. 从文件路径提取基本文件名(去掉目录和 .go 后缀)
//    例如: /a/b/c/d.go -> d
// 3. 遍历 vmodule 过滤器列表,查找匹配的模式
// 4. 将匹配结果缓存到 l.vmap 中,避免重复计算
// 5. 如果没有匹配的模式,返回默认级别 0
//
// 模式匹配:
// - 文件模式匹配使用文件基本名称(不含 .go 后缀)
// - 使用 filepath.Match 进行匹配,支持通配符 * 和 ?
// - 比 C++ 实现中的 *? 匹配更通用
//
// 缓存机制:
// - 将 PC -> Level 的映射存储在 l.vmap 中
// - 相同位置的后续 V() 调用可以直接查表,无需重新计算
// - 当 vmodule 配置改变时,vmap 会被清空
//
// 线程安全:
// - 需要持有 l.mu 锁
// - 调用者必须在调用前获取锁
func (l *loggingT) setV(pc uintptr) Level {
	fn := runtime.FuncForPC(pc)
	file, _ := fn.FileLine(pc)
	// The file is something like /a/b/c/d.go. We want just the d.
	if strings.HasSuffix(file, ".go") {
		file = file[:len(file)-3]
	}
	if slash := strings.LastIndex(file, "/"); slash >= 0 {
		file = file[slash+1:]
	}
	for _, filter := range l.vmodule.filter {
		if filter.match(file) {
			l.vmap[pc] = filter.level
			return filter.level
		}
	}
	l.vmap[pc] = 0
	return 0
}

// Verbose 是一个布尔类型,实现 Infof(类似 Printf)等方法
// 有关更多信息,请参见 V 函数的文档
type Verbose bool

// V 报告调用点的详细程度是否至少达到请求的级别
// 返回值是一个 Verbose 类型的布尔值,它实现了 Info、Infoln 和 Infof 方法
// 如果调用这些方法,它们将写入 Info 日志
// 因此,可以写成以下两种形式之一:
//
//	if glog.V(2) { glog.Info("记录此内容") }
//
// 或
//
//	glog.V(2).Info("记录此内容")
//
// 第二种形式更短,但第一种形式在关闭日志时更便宜,因为它不会评估其参数
//
// 单个 V 调用是否生成日志记录取决于 -v 和 --vmodule 标志的设置
// 两者默认都是关闭的。如果 V 调用中的级别至少是 -v 的值,
// 或包含该调用的源文件的 -vmodule 值,则 V 调用将记录日志
func V(level Level) Verbose {
	// 此函数努力保持低成本,除非有工作要做
	// 快速路径是两次原子加载和比较

	// 这是一个便宜但安全的测试,看看 V 日志是否在全局启用
	if logging.verbosity.get() >= level {
		return Verbose(true)
	}

	// 全局关闭,但 vmodule 可能仍然设置
	// 这是另一个便宜但安全的测试,看看 vmodule 是否启用
	if atomic.LoadInt32(&logging.filterLength) > 0 {
		// 现在我们需要一个适当的锁来使用 logging 结构
		// pcs 字段是共享的,因此我们必须在访问它之前锁定
		// 这相当昂贵,但如果启用了 V 日志,我们本来就很慢
		logging.mu.Lock()
		defer logging.mu.Unlock()
		if runtime.Callers(2, logging.pcs[:]) == 0 {
			return Verbose(false)
		}
		v, ok := logging.vmap[logging.pcs[0]]
		if !ok {
			v = logging.setV(logging.pcs[0])
		}
		return Verbose(v >= level)
	}
	return Verbose(false)
}

// Info is equivalent to the global Info function, guarded by the value of v.
// Info 等同于全局 Info 函数,由 v 的值保护
// 有关用法,请参见 V 的文档
func (v Verbose) Info(args ...interface{}) {
	if v {
		logging.print(infoLog, args...)
	}
}

// Infoln is equivalent to the global Infoln function, guarded by the value of v.
// Infoln 等同于全局 Infoln 函数,由 v 的值保护
// 有关用法,请参见 V 的文档
func (v Verbose) Infoln(args ...interface{}) {
	if v {
		logging.println(infoLog, args...)
	}
}

// Infof is equivalent to the global Infof function, guarded by the value of v.
// Infof 等同于全局 Infof 函数,由 v 的值保护
// 有关用法,请参见 V 的文档
func (v Verbose) Infof(format string, args ...interface{}) {
	if v {
		logging.printf(infoLog, format, args...)
	}
}

// Info 记录到 INFO 日志
// 参数按 fmt.Print 的方式处理;如果缺少换行符则添加
func Info(args ...interface{}) {
	logging.print(infoLog, args...)
}

// InfoDepth 作为 Info 但使用 depth 确定要记录的调用帧
// InfoDepth(0, "msg") 与 Info("msg") 相同
func InfoDepth(depth int, args ...interface{}) {
	logging.printDepth(infoLog, depth, args...)
}

// Infoln 记录到 INFO 日志
// 参数按 fmt.Println 的方式处理;如果缺少换行符则添加
func Infoln(args ...interface{}) {
	logging.println(infoLog, args...)
}

// Infof 记录到 INFO 日志
// 参数按 fmt.Printf 的方式处理;如果缺少换行符则添加
func Infof(format string, args ...interface{}) {
	logging.printf(infoLog, format, args...)
}

// Warning 记录到 WARNING 和 INFO 日志
// 参数按 fmt.Print 的方式处理;如果缺少换行符则添加
func Warning(args ...interface{}) {
	logging.print(warningLog, args...)
}

// WarningDepth 作为 Warning 但使用 depth 确定要记录的调用帧
// WarningDepth(0, "msg") 与 Warning("msg") 相同
func WarningDepth(depth int, args ...interface{}) {
	logging.printDepth(warningLog, depth, args...)
}

// Warningln 记录到 WARNING 和 INFO 日志
// 参数按 fmt.Println 的方式处理;如果缺少换行符则添加
func Warningln(args ...interface{}) {
	logging.println(warningLog, args...)
}

// Warningf 记录到 WARNING 和 INFO 日志
// 参数按 fmt.Printf 的方式处理;如果缺少换行符则添加
func Warningf(format string, args ...interface{}) {
	logging.printf(warningLog, format, args...)
}

// Error 记录到 ERROR、WARNING 和 INFO 日志
// 参数按 fmt.Print 的方式处理;如果缺少换行符则添加
func Error(args ...interface{}) {
	logging.print(errorLog, args...)
}

// ErrorDepth 作为 Error 但使用 depth 确定要记录的调用帧
// ErrorDepth(0, "msg") 与 Error("msg") 相同
func ErrorDepth(depth int, args ...interface{}) {
	logging.printDepth(errorLog, depth, args...)
}

// Errorln 记录到 ERROR、WARNING 和 INFO 日志
// 参数按 fmt.Println 的方式处理;如果缺少换行符则添加
func Errorln(args ...interface{}) {
	logging.println(errorLog, args...)
}

// Errorf 记录到 ERROR、WARNING 和 INFO 日志
// 参数按 fmt.Printf 的方式处理;如果缺少换行符则添加
func Errorf(format string, args ...interface{}) {
	logging.printf(errorLog, format, args...)
}

// Fatal 记录到 FATAL、ERROR、WARNING 和 INFO 日志
// 包括所有运行的 goroutine 的堆栈跟踪,然后调用 os.Exit(255)
// 参数按 fmt.Print 的方式处理;如果缺少换行符则添加
func Fatal(args ...interface{}) {
	logging.print(fatalLog, args...)
}

// FatalDepth 作为 Fatal 但使用 depth 确定要记录的调用帧
// FatalDepth(0, "msg") 与 Fatal("msg") 相同
func FatalDepth(depth int, args ...interface{}) {
	logging.printDepth(fatalLog, depth, args...)
}

// Fatalln 记录到 FATAL、ERROR、WARNING 和 INFO 日志
// 包括所有运行的 goroutine 的堆栈跟踪,然后调用 os.Exit(255)
// 参数按 fmt.Println 的方式处理;如果缺少换行符则添加
func Fatalln(args ...interface{}) {
	logging.println(fatalLog, args...)
}

// Fatalf 记录到 FATAL、ERROR、WARNING 和 INFO 日志
// 包括所有运行的 goroutine 的堆栈跟踪,然后调用 os.Exit(255)
// 参数按 fmt.Printf 的方式处理;如果缺少换行符则添加
func Fatalf(format string, args ...interface{}) {
	logging.printf(fatalLog, format, args...)
}

// fatalNoStacks 如果非零,表示我们要在不转储 goroutine 堆栈的情况下退出
// 它允许 Exit 和相关函数使用 Fatal 日志
var fatalNoStacks uint32

// Exit 记录到 FATAL、ERROR、WARNING 和 INFO 日志,然后调用 os.Exit(1)
// 参数按 fmt.Print 的方式处理;如果缺少换行符则添加
func Exit(args ...interface{}) {
	atomic.StoreUint32(&fatalNoStacks, 1)
	logging.print(fatalLog, args...)
}

// ExitDepth 作为 Exit 但使用 depth 确定要记录的调用帧
// ExitDepth(0, "msg") 与 Exit("msg") 相同
func ExitDepth(depth int, args ...interface{}) {
	atomic.StoreUint32(&fatalNoStacks, 1)
	logging.printDepth(fatalLog, depth, args...)
}

// Exitln 记录到 FATAL、ERROR、WARNING 和 INFO 日志,然后调用 os.Exit(1)
func Exitln(args ...interface{}) {
	atomic.StoreUint32(&fatalNoStacks, 1)
	logging.println(fatalLog, args...)
}

// Exitf 记录到 FATAL、ERROR、WARNING 和 INFO 日志,然后调用 os.Exit(1)
// 参数按 fmt.Printf 的方式处理;如果缺少换行符则添加
func Exitf(format string, args ...interface{}) {
	atomic.StoreUint32(&fatalNoStacks, 1)
	logging.printf(fatalLog, format, args...)
}
