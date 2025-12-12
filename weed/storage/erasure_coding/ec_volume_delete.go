package erasure_coding

import (
	"fmt"
	"io"
	"os"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	// MarkNeedleDeleted 是一个函数，用于在 .ecx 索引文件中标记 Needle 为已删除
	// 通过将 Size 字段设置为 TombstoneFileSize 来标记删除
	//
	// 参数:
	//   - file: .ecx 索引文件
	//   - offset: Needle 条目在文件中的偏移量
	// 返回值:
	//   - error: 错误信息
	// 工作原理:
	//   .ecx 文件格式: NeedleId(8字节) + Offset(4字节) + Size(4字节)
	//   此函数修改 Size 字段为 TombstoneFileSize 以标记删除
	MarkNeedleDeleted = func(file *os.File, offset int64) error {
		b := make([]byte, types.SizeSize)
		types.SizeToBytes(b, types.TombstoneFileSize)
		// 写入位置 = offset + NeedleId 大小 + Offset 大小
		// 即跳过 NeedleId 和 Offset 字段，直接修改 Size 字段
		n, err := file.WriteAt(b, offset+types.NeedleIdSize+types.OffsetSize)
		if err != nil {
			return fmt.Errorf("sorted needle write error: %w", err)
		}
		if n != types.SizeSize {
			return fmt.Errorf("sorted needle written %d bytes, expecting %d", n, types.SizeSize)
		}
		return nil
	}
)

// DeleteNeedleFromEcx 从 EC Volume 删除一个 Needle
// 执行两个操作:
//   1. 在 .ecx 文件中标记 Needle 为已删除
//   2. 将 Needle ID 追加到 .ecj 日志文件
//
// 参数:
//   - needleId: 要删除的 Needle ID
// 返回值:
//   - error: 错误信息
// 说明:
//   .ecx: 排序的索引文件，包含所有 Needle 的元数据
//   .ecj: 日志文件，记录增量删除的 Needle ID
func (ev *EcVolume) DeleteNeedleFromEcx(needleId types.NeedleId) (err error) {

	// 步骤1：在 .ecx 文件中查找并标记 Needle 为已删除
	_, _, err = SearchNeedleFromSortedIndex(ev.ecxFile, ev.ecxFileSize, needleId, MarkNeedleDeleted)

	if err != nil {
		if err == NotFoundError {
			// Needle 不存在，视为成功删除
			return nil
		}
		return err
	}

	// 步骤2：将 Needle ID 追加到 .ecj 日志文件
	b := make([]byte, types.NeedleIdSize)
	types.NeedleIdToBytes(b, needleId)

	// 使用锁保护 .ecj 文件的并发访问
	ev.ecjFileAccessLock.Lock()

	// 定位到文件末尾并写入 Needle ID
	ev.ecjFile.Seek(0, io.SeekEnd)
	ev.ecjFile.Write(b)

	ev.ecjFileAccessLock.Unlock()

	return
}

// RebuildEcxFile 重建 .ecx 索引文件
// 将 .ecj 日志文件中的所有删除操作应用到 .ecx 文件，然后删除 .ecj 文件
//
// 参数:
//   - baseFileName: 文件基础名称（不含扩展名）
// 返回值:
//   - error: 错误信息
// 工作流程:
//   1. 读取 .ecj 文件中的所有 Needle ID
//   2. 在 .ecx 文件中标记这些 Needle 为已删除
//   3. 删除 .ecj 文件（压缩完成）
// 使用场景:
//   当 .ecj 文件变得很大时，通过此函数压缩索引
func RebuildEcxFile(baseFileName string) error {

	// 如果 .ecj 文件不存在，无需重建
	if !util.FileExists(baseFileName + ".ecj") {
		return nil
	}

	// 打开 .ecx 索引文件（读写模式）
	ecxFile, err := os.OpenFile(baseFileName+".ecx", os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("rebuild: failed to open ecx file: %w", err)
	}
	defer ecxFile.Close()

	// 获取 .ecx 文件大小
	fstat, err := ecxFile.Stat()
	if err != nil {
		return err
	}
	ecxFileSize := fstat.Size()

	// 打开 .ecj 日志文件
	ecjFile, err := os.OpenFile(baseFileName+".ecj", os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("rebuild: failed to open ecj file: %w", err)
	}

	// 读取 .ecj 文件中的所有 Needle ID，并在 .ecx 中标记为已删除
	buf := make([]byte, types.NeedleIdSize)
	for {
		n, _ := ecjFile.Read(buf)
		if n != types.NeedleIdSize {
			// 读取完毕
			break
		}

		needleId := types.BytesToNeedleId(buf)

		// 在 .ecx 文件中标记为已删除
		_, _, err = SearchNeedleFromSortedIndex(ecxFile, ecxFileSize, needleId, MarkNeedleDeleted)

		if err != nil && err != NotFoundError {
			ecxFile.Close()
			return err
		}

	}

	ecxFile.Close()

	// 删除 .ecj 文件（所有删除操作已应用到 .ecx）
	os.Remove(baseFileName + ".ecj")

	return nil
}
