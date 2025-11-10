package security

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

var (
	// ErrUnauthorized JWT token 未授权错误
	// 当 token 验证失败时返回此错误
	ErrUnauthorized = errors.New("unauthorized token")
)

// Guard 访问控制守卫
//
// Guard 是 SeaweedFS 的安全组件，负责保护数据访问安全。
// 它提供了两层安全检查机制：
//
// 1. IP 白名单检查（第一道防线）
//   - 检查请求的来源 IP 地址
//   - 支持单个 IP 地址（如 "192.168.1.100"）
//   - 支持 CIDR 网段（如 "192.168.1.0/24"）
//   - 白名单为空时，此检查被跳过
//
// 2. JWT (JSON Web Token) 认证（第二道防线）
//   - 使用 HMAC-SHA256 算法签名
//   - 支持三种 token 传递方式：
//     a. URL 参数：?jwt=xxx
//     b. HTTP 头：Authorization: Bearer xxx
//     c. Cookie：AT=xxx
//
// JWT Claims 验证：
//   - "exp" (Expiration Time)：过期时间，Unix 时间戳
//   - "nbf" (Not Before)：生效时间，Unix 时间戳
//   - "fid" (File ID)：限制访问的文件 ID（可选）
//
// 认证流程：
//   1. 先检查 IP 白名单（快速失败）
//   2. 再验证 JWT token（如果配置了签名密钥）
//   3. 两者通过其一即可访问
//
// 使用场景：
//   - Volume Server：保护文件上传、下载、删除操作
//   - Filer Server：保护文件系统 API
//   - Master Server：保护管理接口
//
// 参考实现：
// https://github.com/pkieltyka/jwtauth/blob/master/jwtauth.go
type Guard struct {
	// whiteListIp 白名单中的单个 IP 地址映射表
	// 使用 map[string]struct{} 而不是 []string 是为了 O(1) 查找性能
	// 键是 IP 地址字符串，如 "192.168.1.100"
	whiteListIp map[string]struct{}

	// whiteListCIDR 白名单中的 CIDR 网段映射表
	// 键是 CIDR 字符串（如 "192.168.1.0/24"），值是解析后的网络对象
	whiteListCIDR map[string]*net.IPNet

	// SigningKey JWT 签名密钥（用于写操作）
	// 用于生成和验证上传、删除等写操作的 JWT token
	// 为空时表示不启用 JWT 认证
	SigningKey SigningKey

	// ExpiresAfterSec 写操作 JWT 的有效期（秒）
	// 默认 10 秒，较短的有效期提高安全性
	ExpiresAfterSec int

	// ReadSigningKey 只读操作的 JWT 签名密钥
	// 用于生成和验证下载等读操作的 JWT token
	// 可以与 SigningKey 不同，实现读写分离的权限控制
	ReadSigningKey SigningKey

	// ReadExpiresAfterSec 读操作 JWT 的有效期（秒）
	// 默认 60 秒，比写操作更长，因为读操作更频繁
	ReadExpiresAfterSec int

	// isWriteActive 是否启用写保护
	// true：需要进行安全检查（白名单或 JWT）
	// false：不需要安全检查，用于开发环境
	// 当白名单非空或配置了签名密钥时为 true
	isWriteActive bool

	// isEmptyWhiteList 白名单是否为空
	// true：跳过白名单检查
	// false：需要检查白名单
	isEmptyWhiteList bool
}

// NewGuard 创建一个新的访问控制守卫实例
//
// 参数：
//   whiteList: IP 白名单列表，可包含单个 IP 或 CIDR 网段
//     示例：[]string{"192.168.1.100", "10.0.0.0/8", "172.16.0.0/12"}
//   signingKey: JWT 签名密钥（用于写操作），空字符串表示不启用 JWT
//   expiresAfterSec: 写操作 JWT 有效期（秒），通常设为 10 秒
//   readSigningKey: 只读操作的 JWT 签名密钥，可以与 signingKey 不同
//   readExpiresAfterSec: 读操作 JWT 有效期（秒），通常设为 60 秒
//
// 返回：
//   初始化完成的 Guard 实例
//
// 使用示例：
//   guard := NewGuard(
//       []string{"192.168.1.0/24"},
//       "my-secret-key",
//       10,
//       "my-read-key",
//       60,
//   )
func NewGuard(whiteList []string, signingKey string, expiresAfterSec int, readSigningKey string, readExpiresAfterSec int) *Guard {
	g := &Guard{
		SigningKey:          SigningKey(signingKey),
		ExpiresAfterSec:     expiresAfterSec,
		ReadSigningKey:      SigningKey(readSigningKey),
		ReadExpiresAfterSec: readExpiresAfterSec,
	}
	// 解析并更新白名单
	g.UpdateWhiteList(whiteList)
	return g
}

// WhiteList HTTP 中间件，用于保护需要白名单验证的端点
//
// 此中间件包装一个 HTTP 处理函数，在执行前先检查客户端 IP 是否在白名单中。
//
// 工作流程：
//   1. 检查是否需要安全验证（isWriteActive）
//   2. 如果不需要，直接执行原处理函数（开发模式）
//   3. 如果需要，先检查白名单
//   4. 白名单通过后执行原处理函数
//   5. 白名单检查失败返回 401 Unauthorized
//
// 参数：
//   f: 需要保护的 HTTP 处理函数
//
// 返回：
//   包装后的 HTTP 处理函数
//
// 使用场景：
//   - 保护管理接口（如压缩、删除卷等）
//   - 保护敏感统计信息端点
//   - 限制只允许内网访问的操作
//
// 使用示例：
//   adminMux.HandleFunc("/admin/compact", guard.WhiteList(compactHandler))
func (g *Guard) WhiteList(f http.HandlerFunc) http.HandlerFunc {
	if !g.isWriteActive {
		// 如果没有启用安全检查，直接返回原处理函数
		// 这种情况发生在：白名单为空且未配置签名密钥
		return f
	}
	return func(w http.ResponseWriter, r *http.Request) {
		// 检查客户端 IP 是否在白名单中
		if err := g.checkWhiteList(w, r); err != nil {
			// 白名单检查失败，返回 401 未授权
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		// 白名单检查通过，执行原处理函数
		f(w, r)
	}
}

// GetActualRemoteHost 获取请求的真实客户端 IP 地址
//
// 安全设计：
// 出于安全考虑，此函数只使用 r.RemoteAddr 来确定客户端 IP 地址，
// 不信任像 X-Forwarded-For 这样的 HTTP 头，因为客户端可以轻易伪造这些头。
//
// 处理的情况：
//   1. 标准格式："192.168.1.100:12345" -> "192.168.1.100"
//   2. IPv6 格式："[::1]:8080" -> "::1"
//   3. 无端口格式："192.168.1.100" -> "192.168.1.100"
//   4. IPv6 无端口："[::1]" -> "::1"
//
// 参数：
//   r: HTTP 请求对象
//
// 返回：
//   客户端的 IP 地址或主机名字符串
//
// 重要提示：
// 如果在反向代理（如 Nginx）后面部署，需要确保代理不允许客户端
// 直接连接到 SeaweedFS，否则白名单检查可能被绕过。
func GetActualRemoteHost(r *http.Request) string {
	// 尝试从 RemoteAddr 中分离出主机和端口
	// RemoteAddr 格式通常是 "IP:Port" 或 "[IPv6]:Port"
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}

	// 如果 SplitHostPort 失败，可能是因为缺少端口号
	// 尝试将 RemoteAddr 作为原始主机（IP 或主机名）解析
	host = strings.TrimSpace(r.RemoteAddr)

	// 可能是没有端口的 IPv6 地址，但带有方括号
	// 例如 "[::1]"，需要去掉方括号
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}

	// 返回主机（可以是 IP 或主机名，就像在 HTTP 头中一样）
	return host
}

// checkWhiteList 检查请求的客户端 IP 是否在白名单中
//
// 检查逻辑分为三步：
//   1. 如果白名单为空，直接通过（开发模式）
//   2. 检查精确匹配（单个 IP 地址或主机名）
//   3. 检查 CIDR 网段匹配（仅适用于 IP 地址）
//
// 参数：
//   w: HTTP 响应写入器（未使用，保留用于未来扩展）
//   r: HTTP 请求对象
//
// 返回：
//   nil: 客户端在白名单中，允许访问
//   error: 客户端不在白名单中，拒绝访问
//
// 日志记录：
// 当检查失败时，会记录客户端 IP 和原始 RemoteAddr，便于调试
func (g *Guard) checkWhiteList(w http.ResponseWriter, r *http.Request) error {
	// 如果白名单为空，跳过检查
	if g.isEmptyWhiteList {
		return nil
	}

	// 获取客户端的真实 IP 地址
	host := GetActualRemoteHost(r)

	// 第一步：检查精确匹配
	// 适用于单个 IP 地址和主机名
	// 例如："192.168.1.100" 或 "localhost"
	if _, ok := g.whiteListIp[host]; ok {
		return nil
	}

	// 第二步：检查 CIDR 网段匹配
	// 仅适用于有效的 IP 地址
	remote := net.ParseIP(host)
	if remote != nil {
		// 遍历所有 CIDR 网段
		for _, cidrnet := range g.whiteListCIDR {
			// 如果白名单条目包含 "/"，说明是 CIDR 网段
			// 检查客户端 IP 是否在该网段内
			if cidrnet.Contains(remote) {
				return nil
			}
		}
	}

	// 白名单检查失败，记录日志并返回错误
	glog.V(0).Infof("Not in whitelist: %s (original RemoteAddr: %s)", host, r.RemoteAddr)
	return fmt.Errorf("Not in whitelist: %s", host)
}

// UpdateWhiteList 更新 IP 白名单配置
//
// 此方法支持热更新白名单，不需要重启服务器。
// 白名单条目可以是：
//   1. 单个 IP 地址：如 "192.168.1.100"、"::1"
//   2. CIDR 网段：如 "192.168.1.0/24"、"10.0.0.0/8"
//   3. 主机名：如 "localhost"
//
// 工作流程：
//   1. 解析白名单条目，区分单个 IP 和 CIDR 网段
//   2. 将单个 IP/主机名存入 whiteListIp map
//   3. 将 CIDR 网段解析并存入 whiteListCIDR map
//   4. 更新安全检查状态标志
//
// 参数：
//   whiteList: IP 白名单字符串数组
//
// 状态更新：
//   - isEmptyWhiteList: 白名单是否为空
//   - isWriteActive: 是否需要进行安全检查
//     (白名单非空或配置了签名密钥时为 true)
//
// 错误处理：
// 如果 CIDR 解析失败，会记录错误日志但不会中断处理，
// 其他有效的白名单条目仍会生效
//
// 使用示例：
//   guard.UpdateWhiteList([]string{
//       "127.0.0.1",           // localhost
//       "192.168.1.0/24",      // 内网网段
//       "10.0.0.0/8",          // 私有网段
//   })
func (g *Guard) UpdateWhiteList(whiteList []string) {
	// 创建新的白名单映射表
	whiteListIp := make(map[string]struct{})
	whiteListCIDR := make(map[string]*net.IPNet)

	// 解析白名单条目
	for _, ip := range whiteList {
		if strings.Contains(ip, "/") {
			// 包含 "/"，是 CIDR 网段格式
			_, cidrnet, err := net.ParseCIDR(ip)
			if err != nil {
				// CIDR 解析失败，记录错误但继续处理其他条目
				glog.Errorf("Parse CIDR %s in whitelist failed: %v", ip, err)
			}
			whiteListCIDR[ip] = cidrnet
		} else {
			// 单个 IP 地址或主机名
			// 使用空结构体 struct{} 节省内存
			whiteListIp[ip] = struct{}{}
		}
	}

	// 更新状态标志
	// 只有当 IP 列表和 CIDR 列表都为空时，白名单才算空
	g.isEmptyWhiteList = len(whiteListIp) == 0 && len(whiteListCIDR) == 0

	// 如果白名单非空或配置了签名密钥，就需要进行安全检查
	g.isWriteActive = !g.isEmptyWhiteList || len(g.SigningKey) != 0

	// 原子性地更新白名单（Go 的 map 赋值是原子的）
	g.whiteListIp = whiteListIp
	g.whiteListCIDR = whiteListCIDR
}
