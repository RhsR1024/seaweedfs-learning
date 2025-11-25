// Package main 提供 Volume .dat 文件修复工具
//
// 用途：
// 修复因删除操作导致的 .dat 和 .idx 文件不一致问题。
// 在某些情况下，删除操作可能导致 .dat 文件中的偏移量不正确，
// 但 .idx 文件中的偏移量是正确的。
//
// 使用场景：
// 这是一个一次性修复工具，用于解决历史数据不一致问题。
// 在正常情况下不应该需要使用此工具。
//
// 使用步骤：
//  1. 运行此工具修复 .dat 文件（生成 .dat_fixed 文件）：
//     go run fix_dat.go -volumeId=9 -dir=/Users/chrislu/Downloads
//  2. 备份原始文件并重命名修复后的文件：
//     mv 9.dat 9.dat.bak
//     mv 9.dat_fixed 9.dat
//  3. 使用 "weed fix" 命令修复 .idx 文件：
//     weed fix -volumeId=9 -dir=/Users/chrislu/Downloads
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

var (
	// fixVolumePath Volume 文件所在目录
	fixVolumePath = flag.String("dir", "/tmp", "data directory to store files")
	// fixVolumeCollection Volume 所属的 Collection
	fixVolumeCollection = flag.String("collection", "", "the volume collection name")
	// fixVolumeId 要修复的 Volume ID
	fixVolumeId = flag.Int("volumeId", -1, "a volume id. The volume should already exist in the dir. The volume index file should not exist.")
)

/*
问题描述：
这是一个一次性修复工具，用于解决 .dat 和 .idx 文件不一致的问题。
在某些情况下，.dat 文件包含所有数据，但某些删除操作导致偏移量不正确。
而 .idx 文件中的偏移量是正确的。

修复步骤：
 1. 修复 .dat 文件，生成新的 .dat_fixed 文件：
    go run fix_dat.go -volumeId=9 -dir=/Users/chrislu/Downloads
 2. 备份原文件并重命名修复后的文件：
    mv 9.dat 9.dat.bak
    mv 9.dat_fixed 9.dat
 3. 使用 "weed fix" 命令修复 .idx 文件：
    weed fix -volumeId=9 -dir=/Users/chrislu/Downloads
*/

// main 主函数：修复 Volume .dat 文件
//
// 工作流程：
// 1. 解析命令行参数（volumeId、dir、collection）
// 2. 打开原始的 .idx 和 .dat 文件
// 3. 创建新的 .dat_fixed 文件
// 4. 读取并复制 SuperBlock
// 5. 遍历 .idx 文件中的所有条目
// 6. 从原 .dat 文件读取 Needle 数据
// 7. 将 Needle 写入新的 .dat_fixed 文件
func main() {
	// 解析命令行参数
	flag.Parse()
	// 初始化全局 HTTP 客户端
	util_http.InitGlobalHttpClient()

	// 构建文件名（格式：collection_volumeId 或 volumeId）
	fileName := strconv.Itoa(*fixVolumeId)
	if *fixVolumeCollection != "" {
		fileName = *fixVolumeCollection + "_" + fileName
	}

	// 打开索引文件（.idx）- 只读模式
	indexFile, err := os.OpenFile(path.Join(*fixVolumePath, fileName+".idx"), os.O_RDONLY, 0644)
	if err != nil {
		glog.Fatalf("Read Volume Index %v", err)
	}
	defer indexFile.Close()

	// 打开原数据文件（.dat）- 只读模式
	datFileName := path.Join(*fixVolumePath, fileName+".dat")
	datFile, err := os.OpenFile(datFileName, os.O_RDONLY, 0644)
	if err != nil {
		glog.Fatalf("Read Volume Data %v", err)
	}
	// 将文件包装为 Backend 接口
	datBackend := backend.NewDiskFile(datFile)
	defer datBackend.Close()

	// 创建新的数据文件（.dat_fixed）
	newDatFile, err := os.Create(path.Join(*fixVolumePath, fileName+".dat_fixed"))
	if err != nil {
		glog.Fatalf("Write New Volume Data %v", err)
	}
	defer newDatFile.Close()

	// 读取 SuperBlock（Volume 元数据）
	superBlock, err := super_block.ReadSuperBlock(datBackend)
	if err != nil {
		glog.Fatalf("Read Volume Data superblock %v", err)
	}
	// 将 SuperBlock 写入新文件
	newDatFile.Write(superBlock.Bytes())

	// 遍历索引文件，修复每个 Needle
	// visitNeedle 函数会将 Needle 追加到 newDatFile
	iterateEntries(datBackend, indexFile, func(n *needle.Needle, offset int64) {
		fmt.Printf("needle id=%v name=%s size=%d dataSize=%d\n", n.Id, string(n.Name), n.Size, n.DataSize)
		// 将 Needle 追加到新数据文件
		_, s, _, e := n.Append(datBackend, superBlock.Version)
		fmt.Printf("size %d error %v\n", s, e)
	})

}

// iterateEntries 遍历索引文件的所有条目，并从数据文件读取对应的 Needle
//
// 工作原理：
// 1. 同时读取 .idx 索引文件和 .dat 数据文件
// 2. .idx 文件提供正确的 Needle 位置信息
// 3. 根据索引信息从 .dat 文件读取 Needle 数据
// 4. 对每个有效的 Needle 调用 visitNeedle 回调函数
//
// 算法流程：
// - 从 .idx 文件顺序读取索引条目（每条 16 字节）
// - 解析出 key、offset 和 size
// - 如果索引中的 offset 与当前 .dat 偏移量不一致，调整到索引指定的位置
// - 从 .dat 文件读取 Needle Header 和 Body
// - 调用 visitNeedle 处理这个 Needle
//
// 参数：
//   - datBackend: 数据文件的 Backend 接口
//   - idxFile: 索引文件句柄
//   - visitNeedle: 回调函数，处理每个有效的 Needle
func iterateEntries(datBackend backend.BackendStorageFile, idxFile *os.File, visitNeedle func(n *needle.Needle, offset int64)) {
	// start to read index file
	// 开始读取索引文件
	var readerOffset int64
	// 读取索引文件的前 16 字节（第一个索引条目）
	bytes := make([]byte, 16)
	count, _ := idxFile.ReadAt(bytes, readerOffset)
	readerOffset += int64(count)

	// start to read dat file
	// 开始读取数据文件
	// 读取 SuperBlock（Volume 元数据）
	superBlock, err := super_block.ReadSuperBlock(datBackend)
	if err != nil {
		fmt.Printf("cannot read dat file super block: %v", err)
		return
	}
	// 数据文件的当前偏移量（跳过 SuperBlock）
	offset := int64(superBlock.BlockSize())
	// Needle 版本号
	version := superBlock.Version

	// 读取第一个 Needle 的 Header
	n, _, rest, err := needle.ReadNeedleHeader(datBackend, version, offset)
	if err != nil {
		fmt.Printf("cannot read needle header: %v", err)
		return
	}
	fmt.Printf("Needle %+v, rest %d\n", n, rest)

	// 循环读取索引和数据文件，直到处理完所有条目
	for n != nil && count > 0 {
		// parse index file entry
		// 解析索引条目（16 字节）
		// key: NeedleId (8 字节)
		key := util.BytesToUint64(bytes[0:8])
		// offsetFromIndex: Needle 偏移量（4 字节，单位：8 字节块）
		offsetFromIndex := util.BytesToUint32(bytes[8:12])
		// sizeFromIndex: Needle 大小（4 字节）
		sizeFromIndex := types.BytesToSize(bytes[12:16])

		// 读取下一个索引条目
		count, _ = idxFile.ReadAt(bytes, readerOffset)
		readerOffset += int64(count)

		// 检查偏移量是否一致
		// 如果索引中的偏移量与当前位置不一致，调整到索引指定的位置
		// 这是修复的核心：使用 .idx 中正确的偏移量
		if offsetFromIndex != 0 && offset != int64(offsetFromIndex)*8 {
			//t := offset
			offset = int64(offsetFromIndex) * 8
			//fmt.Printf("Offset change %d => %d\n", t, offset)
		}

		fmt.Printf("key: %d offsetFromIndex %d n.Size %d sizeFromIndex:%d\n", key, offsetFromIndex, n.Size, sizeFromIndex)

		// 计算 Needle Body 的长度（基于索引中的 size）
		rest = needle.NeedleBodyLength(sizeFromIndex, version)

		// 使用 defer + recover 捕获可能的 panic
		// 因为数据可能损坏，读取时可能出现异常
		func() {
			defer func() {
				if r := recover(); r != nil {
					fmt.Println("Recovered in f", r)
				}
			}()
			// 读取 Needle Body（实际数据）
			if _, err = n.ReadNeedleBody(datBackend, version, offset+int64(types.NeedleHeaderSize), rest); err != nil {
				fmt.Printf("cannot read needle body: offset %d body %d %v\n", offset, rest, err)
			}
		}()

		// 验证 Needle 是否有效（size 应该大于等于 dataSize）
		if n.Size <= types.Size(n.DataSize) {
			continue
		}

		// 调用回调函数处理这个 Needle
		visitNeedle(n, offset)

		// 移动到下一个 Needle 的位置
		offset += types.NeedleHeaderSize + rest
		//fmt.Printf("==> new entry offset %d\n", offset)

		// 读取下一个 Needle 的 Header
		if n, _, rest, err = needle.ReadNeedleHeader(datBackend, version, offset); err != nil {
			if err == io.EOF {
				// 到达文件末尾，正常退出
				return
			}

			fmt.Printf("cannot read needle header: %v\n", err)
			return
		}
		//fmt.Printf("new entry needle size:%d rest:%d\n", n.Size, rest)
	}

}
