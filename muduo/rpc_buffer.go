package muduo

import (
	"encoding/binary"
	"errors"
	"github.com/panjf2000/gnet/v2"
	"strconv"
	"sync"
)

// 全局 sync.Pool：复用 4 字节缓冲区，减少内存分配开销
var uint32BufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 4) // 固定 4 字节，用于存储 uint32 类型（长度、校验和）
	},
}

// 通用缓冲区池：复用读取固定长度数据的缓冲区
var bufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 4096) // 初始大小4KB，可根据业务包大小调整
	},
}

// -----------------------------------------------------------------------------
// 工具函数：读取固定长度数据（适配gnet Conn.Read接口）
// -----------------------------------------------------------------------------
func readFixedLength(conn gnet.Conn, length int) ([]byte, error) {
	if length <= 0 {
		return nil, errors.New("invalid read length (must be > 0)")
	}

	// 1. 从池获取缓冲区（复用避免频繁分配）
	buf, ok := bufPool.Get().([]byte)
	if !ok || cap(buf) < length {
		buf = make([]byte, length) // 缓冲区不足时重新创建
	} else {
		buf = buf[:length] // 缩容到目标长度，清除旧数据残留
	}
	defer bufPool.Put(buf) // 读取完成后归还池

	// 2. 循环读取：确保读满目标长度（处理一次读不完的情况）
	totalRead := 0
	for totalRead < length {
		n, err := conn.Read(buf[totalRead:]) // 读取剩余需要的字节
		if n > 0 {
			totalRead += n // 累计已读取长度
		}

		// 3. 错误处理（遵循标准Reader逻辑）
		if err != nil {
			// 已读取部分数据：返回已读数据+错误（上层可决定是否重试）
			if totalRead > 0 {
				return append([]byte(nil), buf[:totalRead]...), err // 拷贝避免池复用污染
			}
			return nil, err
		}

		// 4. 无数据可用（gnet非阻塞特性）：返回数据不足
		if n == 0 {
			return append([]byte(nil), buf[:totalRead]...), ErrInsufficientData
		}
	}

	// 5. 读取完整：返回数据拷贝（避免池缓冲区后续被覆盖）
	return append([]byte(nil), buf...), nil
}

// -----------------------------------------------------------------------------
// RpcCodec: RPC0标签编解码器（基于gnet.Peek接口）
// -----------------------------------------------------------------------------

// rpcConnContext: RpcCodec的连接上下文

// uint32ToBytes: 复用切片池，将uint32转为大端字节流
func uint32ToBytes(n uint32) []byte {
	buf := uint32BufPool.Get().([]byte)
	binary.BigEndian.PutUint32(buf, n)
	return buf
}

// consumePacket：消费指定长度的数据包（推进 gnet 缓冲区指针，标记数据已处理）
// 内部辅助函数，不对外暴露
func consumePacket(conn gnet.Conn, length int) error {
	if length <= 0 {
		return errors.New("invalid consume length (must be positive)")
	}

	buf := make([]byte, length)
	n, err := conn.Read(buf)
	if err != nil {
		return errors.Join(errors.New("read packet from conn failed"), err)
	}
	if n != length {
		return errors.New("consume length mismatch: read " + strconv.Itoa(n) +
			" bytes, expected " + strconv.Itoa(length) + " bytes")
	}
	return nil
}

// ReleaseUint32Buf：归还 uint32 缓冲区到池（配合 uint32ToBytes 使用）
// 对外暴露，避免内存泄漏
func ReleaseUint32Buf(buf []byte) {
	if len(buf) == 4 { // 仅接受 4 字节缓冲区（避免非法数据污染池）
		uint32BufPool.Put(buf)
	}
}
