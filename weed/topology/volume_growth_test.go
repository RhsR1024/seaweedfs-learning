// Package topology 的测试文件
// 本文件测试卷增长（VolumeGrowth）的核心功能，包括：
//   1. findEmptySlotsForOneVolume: 查找空闲槽位
//   2. 副本放置策略的正确性
//   3. 加权调度算法
//   4. PickForWrite: 选择可写卷
package topology

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"

	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

// topologyLayout 定义测试用的拓扑结构
// JSON 格式：数据中心 → 机架 → 服务器 → 卷列表
//
// 拓扑结构：
//   dc1:
//     rack1: 2 台服务器（server111 满载，server112 有空间）
//     rack2: 3 台服务器（server121 接近满载，server122 空闲，server123 有空间）
//   dc2: 空数据中心（用于测试失败场景）
//   dc3:
//     rack2: 1 台服务器（server321 有空间）
//
// 注意：
//   - "limit" 表示服务器的最大卷数量
//   - "volumes" 表示服务器当前的卷列表
//   - 可用空间 = limit - len(volumes)
var topologyLayout = `
{
  "dc1":{
    "rack1":{
      "server111":{
        "volumes":[
          {"id":1, "size":12312},
          {"id":2, "size":12312},
          {"id":3, "size":12312}
        ],
        "limit":3
      },
      "server112":{
        "volumes":[
          {"id":4, "size":12312},
          {"id":5, "size":12312},
          {"id":6, "size":12312}
        ],
        "limit":10
      }
    },
    "rack2":{
      "server121":{
        "volumes":[
          {"id":4, "size":12312},
          {"id":5, "size":12312},
          {"id":6, "size":12312}
        ],
        "limit":4
      },
      "server122":{
        "volumes":[],
        "limit":4
      },
      "server123":{
        "volumes":[
          {"id":2, "size":12312},
          {"id":3, "size":12312},
          {"id":4, "size":12312}
        ],
        "limit":5
      }
    }
  },
  "dc2":{
  },
  "dc3":{
    "rack2":{
      "server321":{
        "volumes":[
          {"id":1, "size":12312},
          {"id":3, "size":12312},
          {"id":5, "size":12312}
        ],
        "limit":4
      }
    }
  }
}
`

// setup 根据 JSON 配置构建测试用的拓扑结构
// 执行流程：
//   1. 解析 JSON 配置
//   2. 创建拓扑树：Topology → DataCenter → Rack → DataNode
//   3. 为每个服务器添加现有的卷
//   4. 设置服务器的容量限制
//
// 参数:
//   - topologyLayout: JSON 格式的拓扑配置
// 返回:
//   - *Topology: 构建好的拓扑对象
func setup(topologyLayout string) *Topology {
	// 【步骤 1：解析 JSON 配置】
	var data interface{}
	err := json.Unmarshal([]byte(topologyLayout), &data)
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Println("data:", data)

	// 【步骤 2：创建根拓扑节点】
	// 参数说明：
	//   - "weedfs": 拓扑名称
	//   - sequence.NewMemorySequencer(): 内存序列生成器（用于分配 Volume ID）
	//   - 32*1024: 卷大小限制（32KB，测试用）
	//   - 5: 心跳间隔（秒）
	//   - false: 不使用默认副本策略
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)

	// 【步骤 3：构建拓扑树结构】
	mTopology := data.(map[string]interface{})

	// 遍历数据中心
	for dcKey, dcValue := range mTopology {
		dc := NewDataCenter(dcKey)
		dcMap := dcValue.(map[string]interface{})
		topo.LinkChildNode(dc) // 将数据中心链接到拓扑根节点

		// 遍历机架
		for rackKey, rackValue := range dcMap {
			dcRack := NewRack(rackKey)
			rackMap := rackValue.(map[string]interface{})
			dc.LinkChildNode(dcRack) // 将机架链接到数据中心

			// 遍历服务器
			for serverKey, serverValue := range rackMap {
				server := NewDataNode(serverKey)
				serverMap := serverValue.(map[string]interface{})

				// 设置服务器 IP（可选）
				if ip, ok := serverMap["ip"]; ok {
					server.Ip = ip.(string)
				}
				dcRack.LinkChildNode(server) // 将服务器链接到机架

				// 【步骤 4：为服务器添加现有的卷】
				for _, v := range serverMap["volumes"].([]interface{}) {
					m := v.(map[string]interface{})
					// 创建卷信息对象
					vi := storage.VolumeInfo{
						Id:      needle.VolumeId(int64(m["id"].(float64))),     // Volume ID
						Size:    uint64(m["size"].(float64)),                   // 卷大小
						Version: needle.GetCurrentVersion(),                     // 卷版本
					}

					// 设置集合名称（可选）
					if mVal, ok := m["collection"]; ok {
						vi.Collection = mVal.(string)
					}

					// 设置副本策略（可选）
					if mVal, ok := m["replication"]; ok {
						rp, _ := super_block.NewReplicaPlacementFromString(mVal.(string))
						vi.ReplicaPlacement = rp
					}

					// 如果有副本策略，注册到 VolumeLayout
					if vi.ReplicaPlacement != nil {
						vl := topo.GetVolumeLayout(vi.Collection, vi.ReplicaPlacement, needle.EMPTY_TTL, types.HardDriveType)
						vl.RegisterVolume(&vi, server)    // 注册卷位置
						vl.setVolumeWritable(vi.Id)       // 标记为可写
					}

					// 在服务器上添加卷信息
					server.AddOrUpdateVolume(vi)
				}

				// 【步骤 5：设置服务器的容量限制】
				// 获取或创建磁盘对象
				disk := server.getOrCreateDisk("")
				// 调整磁盘使用统计（设置最大卷数量）
				disk.UpAdjustDiskUsageDelta("", &DiskUsageCounts{
					maxVolumeCount: int64(serverMap["limit"].(float64)),
				})
			}
		}
	}

	return topo
}

// TestFindEmptySlotsForOneVolume 测试查找空闲槽位的核心功能
// 测试场景：在 dc1 数据中心内创建一个需要 3 个副本的卷
//
// 副本策略 "002" 解析：
//   - 0 个跨数据中心副本
//   - 0 个跨机架副本（同数据中心）
//   - 2 个同机架副本（同机架不同服务器）
//   - 总共需要 3 个副本：1 个主副本 + 2 个同机架副本
//
// 预期结果：
//   - 从 dc1 的某个机架中选择 3 台不同的服务器
//   - 所有服务器都在同一个机架（因为 DiffRackCount=0）
//   - 打印选中的服务器列表
func TestFindEmptySlotsForOneVolume(t *testing.T) {
	// 【准备测试环境】
	topo := setup(topologyLayout)
	vg := NewDefaultVolumeGrowth()

	// 【定义副本策略】
	// "002": 同机架 2 个副本（共 3 个副本）
	rp, _ := super_block.NewReplicaPlacementFromString("002")

	// 【定义卷增长选项】
	volumeGrowOption := &VolumeGrowOption{
		Collection:       "",      // 不指定集合
		ReplicaPlacement: rp,      // 副本策略
		DataCenter:       "dc1",   // 指定数据中心（dc1）
		Rack:             "",      // 不指定机架（自动选择）
		DataNode:         "",      // 不指定节点（自动选择）
	}

	// 【执行查找】
	// useReservations=false：不使用容量预留机制
	servers, _, err := vg.findEmptySlotsForOneVolume(topo, volumeGrowOption, false)

	// 【验证结果】
	if err != nil {
		fmt.Println("finding empty slots error :", err)
		t.Fail()
	}

	// 打印选中的服务器（用于调试）
	for _, server := range servers {
		fmt.Println("assigned node :", server.Id())
	}
}

var topologyLayout2 = `
{
  "dc1":{
    "rack1":{
      "server111":{
        "volumes":[
          {"id":1, "size":12312},
          {"id":2, "size":12312},
          {"id":3, "size":12312}
        ],
        "limit":300
      },
      "server112":{
        "volumes":[
          {"id":4, "size":12312},
          {"id":5, "size":12312},
          {"id":6, "size":12312}
        ],
        "limit":300
      },
      "server113":{
        "volumes":[],
        "limit":300
      },
      "server114":{
        "volumes":[],
        "limit":300
      },
      "server115":{
        "volumes":[],
        "limit":300
      },
      "server116":{
        "volumes":[],
        "limit":300
      }
    },
    "rack2":{
      "server121":{
        "volumes":[
          {"id":4, "size":12312},
          {"id":5, "size":12312},
          {"id":6, "size":12312}
        ],
        "limit":300
      },
      "server122":{
        "volumes":[],
        "limit":300
      },
      "server123":{
        "volumes":[
          {"id":2, "size":12312},
          {"id":3, "size":12312},
          {"id":4, "size":12312}
        ],
        "limit":300
      },
      "server124":{
        "volumes":[],
        "limit":300
      },
      "server125":{
        "volumes":[],
        "limit":300
      },
      "server126":{
        "volumes":[],
        "limit":300
      }
    },
    "rack3":{
      "server131":{
        "volumes":[],
        "limit":300
      },
      "server132":{
        "volumes":[],
        "limit":300
      },
      "server133":{
        "volumes":[],
        "limit":300
      },
      "server134":{
        "volumes":[],
        "limit":300
      },
      "server135":{
        "volumes":[],
        "limit":300
      },
      "server136":{
        "volumes":[],
        "limit":300
      }
    }
  }
}
`

func TestReplication011(t *testing.T) {
	topo := setup(topologyLayout2)
	vg := NewDefaultVolumeGrowth()
	rp, _ := super_block.NewReplicaPlacementFromString("011")
	volumeGrowOption := &VolumeGrowOption{
		Collection:       "MAIL",
		ReplicaPlacement: rp,
		DataCenter:       "dc1",
		Rack:             "",
		DataNode:         "",
	}
	servers, _, err := vg.findEmptySlotsForOneVolume(topo, volumeGrowOption, false)
	if err != nil {
		fmt.Println("finding empty slots error :", err)
		t.Fail()
	}
	for _, server := range servers {
		fmt.Println("assigned node :", server.Id())
	}
}

var topologyLayout3 = `
{
  "dc1":{
    "rack1":{
      "server111":{
        "volumes":[],
        "limit":2000
      }
    }
  },
  "dc2":{
    "rack2":{
      "server222":{
        "volumes":[],
        "limit":2000
      }
    }
  },
  "dc3":{
    "rack3":{
      "server333":{
        "volumes":[],
        "limit":1000
      }
    }
  },
  "dc4":{
    "rack4":{
      "server444":{
        "volumes":[],
        "limit":1000
      }
    }
  },
  "dc5":{
    "rack5":{
      "server555":{
        "volumes":[],
        "limit":500
      }
    }
  },
  "dc6":{
    "rack6":{
      "server666":{
        "volumes":[],
        "limit":500
      }
    }
  }
}
`

func TestFindEmptySlotsForOneVolumeScheduleByWeight(t *testing.T) {
	topo := setup(topologyLayout3)
	vg := NewDefaultVolumeGrowth()
	rp, _ := super_block.NewReplicaPlacementFromString("100")
	volumeGrowOption := &VolumeGrowOption{
		Collection:       "Weight",
		ReplicaPlacement: rp,
		DataCenter:       "",
		Rack:             "",
		DataNode:         "",
	}

	distribution := map[NodeId]int{}
	// assign 1000 volumes
	for i := 0; i < 1000; i++ {
		servers, _, err := vg.findEmptySlotsForOneVolume(topo, volumeGrowOption, false)
		if err != nil {
			fmt.Println("finding empty slots error :", err)
			t.Fail()
		}
		for _, server := range servers {
			// fmt.Println("assigned node :", server.Id())
			if _, ok := distribution[server.id]; !ok {
				distribution[server.id] = 0
			}
			distribution[server.id] += 1
		}
	}

	for k, v := range distribution {
		fmt.Printf("%s : %d\n", k, v)
	}
}

var topologyLayout4 = `
{
  "dc1":{
    "rack1":{
      "serverdc111":{
		"ip": "127.0.0.1",
        "volumes":[
          {"id":1, "size":12312, "collection":"test", "replication":"001"},
          {"id":2, "size":12312, "collection":"test", "replication":"100"},
          {"id":4, "size":12312, "collection":"test", "replication":"100"},
          {"id":6, "size":12312, "collection":"test", "replication":"010"}
        ],
        "limit":100
      }
    }
  },
  "dc2":{
    "rack1":{
      "serverdc211":{
		"ip": "127.0.0.2",
        "volumes":[
          {"id":2, "size":12312, "collection":"test", "replication":"100"},
          {"id":3, "size":12312, "collection":"test", "replication":"010"},
          {"id":5, "size":12312, "collection":"test", "replication":"001"},
          {"id":6, "size":12312, "collection":"test", "replication":"010"}
		],
        "limit":100
      }
    }
  },
  "dc3":{
    "rack1":{
      "serverdc311":{
		"ip": "127.0.0.3",
        "volumes":[
          {"id":1, "size":12312, "collection":"test", "replication":"001"},
          {"id":3, "size":12312, "collection":"test", "replication":"010"},
          {"id":4, "size":12312, "collection":"test", "replication":"100"},
          {"id":5, "size":12312, "collection":"test", "replication":"001"}
		],
        "limit":100
      }
    }
  }
}
`

func TestPickForWrite(t *testing.T) {
	topo := setup(topologyLayout4)
	volumeGrowOption := &VolumeGrowOption{
		Collection: "test",
		DataCenter: "",
		Rack:       "",
		DataNode:   "",
	}
	VolumeGrowStrategy.Threshold = 0.9
	for _, rpStr := range []string{"001", "010", "100"} {
		rp, _ := super_block.NewReplicaPlacementFromString(rpStr)
		vl := topo.GetVolumeLayout("test", rp, needle.EMPTY_TTL, types.HardDriveType)
		volumeGrowOption.ReplicaPlacement = rp
		for _, dc := range []string{"", "dc1", "dc2", "dc3", "dc0"} {
			volumeGrowOption.DataCenter = dc
			for _, r := range []string{""} {
				volumeGrowOption.Rack = r
				for _, dn := range []string{""} {
					if dc == "" && dn != "" {
						continue
					}
					volumeGrowOption.DataNode = dn
					fileId, count, _, shouldGrow, err := topo.PickForWrite(1, volumeGrowOption, vl)
					if dc == "dc0" {
						if err == nil || count != 0 || !shouldGrow {
							fmt.Println(dc, r, dn, "pick for write should be with error")
							t.Fail()
						}
					} else if err != nil {
						fmt.Println(dc, r, dn, "pick for write error :", err)
						t.Fail()
					} else if count == 0 {
						fmt.Println(dc, r, dn, "pick for write count is zero")
						t.Fail()
					} else if len(fileId) == 0 {
						fmt.Println(dc, r, dn, "pick for write file id is empty")
						t.Fail()
					} else if shouldGrow {
						fmt.Println(dc, r, dn, "pick for write error : not should grow")
						t.Fail()
					}
				}
			}
		}
	}
}
