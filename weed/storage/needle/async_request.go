package needle

// AsyncRequest 异步请求结构
// 用于处理 needle 的异步读写操作,实现了非阻塞的 I/O 模式
// 通过 channel 机制实现请求的异步执行和结果返回
// 主要应用场景:
// 1. 批量写入操作的异步处理,提高写入吞吐量
// 2. 读取操作的并发执行,减少请求延迟
type AsyncRequest struct {
	// N 要读写的 needle 对象
	// 包含了文件的完整数据和元数据
	N *Needle

	// IsWriteRequest 标记是写入请求还是读取请求
	// true 表示写入,false 表示读取
	IsWriteRequest bool

	// ActualSize 实际写入或读取的数据大小(字节)
	// 用于统计和验证实际传输的数据量
	ActualSize int64

	// offset needle 在 volume 文件中的偏移量
	// 记录数据在 .dat 文件中的物理位置
	offset uint64

	// size needle 的大小(字节)
	// 包含 header、data、footer 等所有部分的总大小
	size uint64

	// doneChan 完成信号 channel
	// 当请求处理完成时关闭此 channel,通知等待的调用方
	doneChan chan interface{}

	// isUnchanged 标记数据是否未改变
	// 在写入时如果数据与现有数据相同,则设置为 true
	// 可用于优化,避免重复写入相同的数据
	isUnchanged bool

	// err 请求执行过程中的错误
	// nil 表示执行成功,非 nil 表示执行失败
	err error
}

// NewAsyncRequest 创建一个新的异步请求对象
// n: 要操作的 needle 对象
// isWriteRequest: true 表示写入请求,false 表示读取请求
// 返回初始化好的 AsyncRequest 实例
func NewAsyncRequest(n *Needle, isWriteRequest bool) *AsyncRequest {
	return &AsyncRequest{
		offset:         0,
		size:           0,
		ActualSize:     0,
		doneChan:       make(chan interface{}), // 创建无缓冲 channel,用于同步
		N:              n,
		isUnchanged:    false,
		IsWriteRequest: isWriteRequest,
		err:            nil,
	}
}

// WaitComplete 等待异步请求完成并返回结果
// 此方法会阻塞当前 goroutine,直到请求处理完成
// 返回值:
//   offset: needle 在 volume 中的偏移量
//   size: needle 的大小
//   isUnchanged: 数据是否未改变
//   error: 错误信息,nil 表示成功
func (r *AsyncRequest) WaitComplete() (uint64, uint64, bool, error) {
	// 阻塞等待 doneChan 被关闭
	// 当处理完成时,Complete() 或 Submit() 会关闭此 channel
	<-r.doneChan
	return r.offset, r.size, r.isUnchanged, r.err
}

// Complete 标记请求完成并设置结果
// 此方法由请求处理器调用,用于返回处理结果
// offset: needle 在 volume 中的偏移量
// size: needle 的大小
// isUnchanged: 数据是否未改变
// err: 错误信息,nil 表示成功
// 调用此方法后会关闭 doneChan,唤醒所有等待的 goroutine
func (r *AsyncRequest) Complete(offset uint64, size uint64, isUnchanged bool, err error) {
	r.offset = offset
	r.size = size
	r.isUnchanged = isUnchanged
	r.err = err
	// 关闭 channel,通知等待方请求已完成
	close(r.doneChan)
}

// UpdateResult 更新请求结果但不关闭 channel
// 此方法仅更新结果字段,不通知等待方
// 适用于需要多次更新结果的场景,最后再调用 Submit() 提交
// offset: needle 在 volume 中的偏移量
// size: needle 的大小
// isUnchanged: 数据是否未改变
// err: 错误信息,nil 表示成功
func (r *AsyncRequest) UpdateResult(offset uint64, size uint64, isUnchanged bool, err error) {
	r.offset = offset
	r.size = size
	r.isUnchanged = isUnchanged
	r.err = err
}

// Submit 提交请求结果
// 关闭 doneChan,通知等待方请求已处理完成
// 通常与 UpdateResult() 配合使用:先更新结果,再调用 Submit() 提交
func (r *AsyncRequest) Submit() {
	close(r.doneChan)
}

// IsSucceed 判断请求是否执行成功
// 返回 true 表示成功(err == nil),false 表示失败
func (r *AsyncRequest) IsSucceed() bool {
	return r.err == nil
}
