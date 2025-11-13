package operation

import (
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// ErrorNotFound 表示在缓存中未找到对应的 volume ID
var ErrorNotFound = errors.New("not found")

// VidInfo Volume ID 信息结构
// 存储一个 volume 的位置信息和缓存过期时间
type VidInfo struct {
	// Locations volume 所在的服务器位置列表
	// 一个 volume 可能存在于多个服务器上(副本)
	Locations []Location

	// NextRefreshTime 下次刷新时间
	// 超过此时间后,缓存条目被认为已过期,需要重新查询
	NextRefreshTime time.Time
}

// VidCache Volume ID 缓存结构
// 提供线程安全的 volume 位置信息缓存
// 使用数组索引作为 volume ID,实现 O(1) 的查询性能
//
// 设计思路:
// - volume ID 从 1 开始递增
// - 使用数组索引 (id-1) 存储对应 volume 的信息
// - 读写锁保证并发安全
// - 支持缓存过期机制,避免使用过时的位置信息
type VidCache struct {
	// sync.RWMutex 读写锁
	// 允许多个并发读取,但写入时独占
	sync.RWMutex

	// cache volume 信息缓存数组
	// 索引 = volume ID - 1
	cache []VidInfo
}

// Get 从缓存中获取指定 volume 的位置信息
// 使用读锁保证并发安全,多个 goroutine 可以同时读取
//
// 参数:
//   vid: volume ID 字符串(如 "1", "2", "100")
//
// 返回:
//   []Location: volume 所在的服务器位置列表
//   error: 错误信息
//     - 解析失败: vid 不是有效的数字
//     - "not found": vid 超出缓存范围
//     - "not set": 该 vid 的缓存槽位存在但未设置
//     - "expired": 缓存已过期
//
// 注意:
//   - volume ID 从 1 开始,使用 id-1 作为数组索引
//   - 过期的缓存会返回错误,调用方应重新查询
func (vc *VidCache) Get(vid string) ([]Location, error) {
	// 解析 volume ID 字符串为整数
	id, err := strconv.Atoi(vid)
	if err != nil {
		glog.V(1).Infof("Unknown volume id %s", vid)
		return nil, err
	}

	// 加读锁,允许并发读取
	vc.RLock()
	defer vc.RUnlock()

	// 检查 ID 是否在有效范围内
	// volume ID 从 1 开始,所以需要检查 0 < id <= len
	if 0 < id && id <= len(vc.cache) {
		// 检查缓存是否已设置
		if vc.cache[id-1].Locations == nil {
			return nil, errors.New("not set")
		}

		// 检查缓存是否已过期
		if vc.cache[id-1].NextRefreshTime.Before(time.Now()) {
			return nil, errors.New("expired")
		}

		// 返回有效的缓存数据
		return vc.cache[id-1].Locations, nil
	}

	// ID 超出缓存范围
	return nil, ErrorNotFound
}
// Set 设置指定 volume 的位置信息到缓存
// 使用写锁保证并发安全,写入时会阻塞其他读写操作
//
// 参数:
//   vid: volume ID 字符串
//   locations: volume 所在的服务器位置列表
//   duration: 缓存有效期,超过此时间后缓存过期
//
// 行为:
//   - 如果 vid 超出当前缓存数组大小,会自动扩展数组
//   - 数组扩展时,新增的空位会用零值 VidInfo 填充
//   - 设置过期时间为 当前时间 + duration
//
// 注意:
//   - volume ID 从 1 开始,0 是无效的
//   - 数组会按需增长,但不会缩减
func (vc *VidCache) Set(vid string, locations []Location, duration time.Duration) {
	// 解析 volume ID
	id, err := strconv.Atoi(vid)
	if err != nil {
		glog.V(1).Infof("Unknown volume id %s", vid)
		return
	}

	// 加写锁,独占访问
	vc.Lock()
	defer vc.Unlock()

	// 如果 ID 超出当前数组大小,扩展数组
	if id > len(vc.cache) {
		// 计算需要扩展的空位数量
		// 使用零值 VidInfo 填充新增的空位
		for i := id - len(vc.cache); i > 0; i-- {
			vc.cache = append(vc.cache, VidInfo{})
		}
	}

	// 设置缓存数据(volume ID > 0 才有效)
	if id > 0 {
		vc.cache[id-1].Locations = locations
		// 计算过期时间:当前时间 + 缓存有效期
		vc.cache[id-1].NextRefreshTime = time.Now().Add(duration)
	}
}
