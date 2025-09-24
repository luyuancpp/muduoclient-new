package muduocodec

import (
	"encoding/binary"
	"errors"
	"github.com/panjf2000/gnet/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"hash/adler32"
	"io"
	"log"
	"sync"
)

// 自定义错误类型：明确区分不同解码状态
var (
	ErrInsufficientData = errors.New("insufficient data for decoding")    // 数据不足
	ErrInvalidChecksum  = errors.New("checksum mismatch")                 // 校验和不匹配
	ErrInvalidTag       = errors.New("invalid RPC tag (expected 'RPC0')") // RPC标签错误
	ErrInvalidProtoType = errors.New("protobuf message type not found")   // 找不到Proto类型
)

// 4字节切片池：复用临时缓冲区（用于uint32转字节）
var uint32BufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 4)
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
// TcpCodec: 通用Protobuf编解码器（基于gnet.Peek接口）
// -----------------------------------------------------------------------------

// connContext: TcpCodec的连接上下文（缓存未消费数据+解码后的消息）
type connContext struct {
	cachedData []byte // 暂存未消费的Peek数据
	msg        proto.Message
}

type TcpCodec struct{}

func NewTcpCodec() *TcpCodec {
	return &TcpCodec{}
}

// uint32ToBytes: 复用切片池，将uint32转为大端字节流
func uint32ToBytes(n uint32) []byte {
	buf := uint32BufPool.Get().([]byte)
	binary.BigEndian.PutUint32(buf, n)
	return buf
}

// Encode: 编码Proto消息为TCP协议格式
func (c *TcpCodec) Encode(conn gnet.Conn, _ []byte) ([]byte, error) {
	ctx, ok := conn.Context().(*connContext)
	if !ok || ctx.msg == nil {
		return nil, nil // 无待发送消息
	}

	// 1. 获取Proto消息描述符和类型名
	desc := ctx.msg.ProtoReflect().Descriptor()
	if desc == nil {
		return nil, errors.New("empty message descriptor")
	}
	msgTypeName := desc.Name() + " " // 保留原格式的空格
	msgTypeNameData := []byte(msgTypeName)
	nameLen := uint32(len(msgTypeNameData))

	// 2. 序列化Proto消息体（v2接口）
	msgBody, err := proto.MarshalOptions{}.Marshal(ctx.msg)
	if err != nil {
		return nil, err
	}

	// 3. 构建Payload（nameLen + typeName + body）
	payloadLen := 4 + len(msgTypeNameData) + len(msgBody)
	payload := make([]byte, payloadLen)
	// 写入nameLen（复用切片池）
	nameLenBuf := uint32ToBytes(nameLen)
	copy(payload[0:4], nameLenBuf)
	uint32BufPool.Put(nameLenBuf) // 归还切片
	// 写入typeName和body
	copy(payload[4:4+len(msgTypeNameData)], msgTypeNameData)
	copy(payload[4+len(msgTypeNameData):], msgBody)

	// 4. 计算校验和并拼接
	checksum := adler32.Checksum(payload)
	checksumBuf := uint32ToBytes(checksum)
	defer uint32BufPool.Put(checksumBuf) // 延迟归还切片
	payload = append(payload, checksumBuf...)

	// 5. 拼接总长度前缀（4字节）
	totalLen := uint32(len(payload))
	totalLenBuf := uint32ToBytes(totalLen)
	defer uint32BufPool.Put(totalLenBuf)
	fullPacket := append(totalLenBuf, payload...)

	return fullPacket, nil
}

// Decode: 基于Peek接口解码TCP协议包
func (c *TcpCodec) Decode(conn gnet.Conn) ([]byte, error) {
	// 1. 获取/初始化连接上下文
	ctx, ok := conn.Context().(*connContext)
	if !ok {
		ctx = &connContext{cachedData: make([]byte, 0, 4096)} // 预分配缓存
		conn.SetContext(ctx)
	}

	// 2. 循环解析：处理粘包和拆包
	for {
		// 2.1 合并缓存数据和新Peek数据
		newData, err := conn.Peek(0)     // Peek所有可用新数据（n=0表示全部）
		if err != nil && err != io.EOF { // EOF暂时忽略（对端关闭时后续会处理）
			log.Println("Peek new data error:", err)
			return nil, err
		}
		fullData := append(ctx.cachedData, newData...)
		ctx.cachedData = ctx.cachedData[:0] // 清空缓存

		// 2.2 第一步：解析总长度前缀（4字节）
		if len(fullData) < 4 {
			ctx.cachedData = append(ctx.cachedData, fullData...) // 缓存不足数据
			return nil, ErrInsufficientData
		}
		totalPayloadLen := binary.BigEndian.Uint32(fullData[:4])
		requiredTotalLen := 4 + int(totalPayloadLen) // 完整包总长度（4字节长度+payload+checksum）

		// 2.3 第二步：检查是否有完整包
		if len(fullData) < requiredTotalLen {
			ctx.cachedData = append(ctx.cachedData, fullData...) // 缓存不足数据
			return nil, ErrInsufficientData
		}

		// 2.4 第三步：校验和验证
		payload := fullData[4 : requiredTotalLen-4] // payload = nameLen+typeName+body
		checksumExpected := binary.BigEndian.Uint32(fullData[requiredTotalLen-4 : requiredTotalLen])
		if adler32.Checksum(payload) != checksumExpected {
			log.Println("TcpCodec:", ErrInvalidChecksum)
			// 消费错误包（读取并丢弃）
			if _, readErr := readFixedLength(conn, requiredTotalLen); readErr != nil && readErr != ErrInsufficientData {
				log.Println("Read invalid packet error:", readErr)
			}
			fullData = fullData[requiredTotalLen:] // 跳过错误包
			continue
		}

		// 2.5 第四步：解析类型名长度和类型名
		if len(payload) < 4 { // 至少需要4字节nameLen
			log.Println("TcpCodec: insufficient data for nameLen")
			if _, readErr := readFixedLength(conn, requiredTotalLen); readErr != nil && readErr != ErrInsufficientData {
				log.Println("Read invalid nameLen packet error:", readErr)
			}
			fullData = fullData[requiredTotalLen:]
			continue
		}
		nameLen := binary.BigEndian.Uint32(payload[:4])
		if nameLen < 1 || int(nameLen+4) > len(payload) { // 校验类型名长度合法性
			log.Println("TcpCodec: invalid nameLen:", nameLen)
			if _, readErr := readFixedLength(conn, requiredTotalLen); readErr != nil && readErr != ErrInsufficientData {
				log.Println("Read invalid nameLen packet error:", readErr)
			}
			fullData = fullData[requiredTotalLen:]
			continue
		}

		// 2.6 第五步：解析Proto消息并反序列化
		typeNameStr := string(payload[4 : 4+nameLen-1]) // 去掉末尾空格
		msgType, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(typeNameStr))
		if err != nil {
			log.Printf("TcpCodec: %v, typeName=%s", ErrInvalidProtoType, typeNameStr)
			if _, readErr := readFixedLength(conn, requiredTotalLen); readErr != nil && readErr != ErrInsufficientData {
				log.Println("Read invalid proto type packet error:", readErr)
			}
			fullData = fullData[requiredTotalLen:]
			continue
		}

		// 反序列化（使用v2标准接口，替代废弃的proto.MessageV1）
		bodyStart := 4 + nameLen
		msg := msgType.New().Interface()
		unmarshalOptions := proto.UnmarshalOptions{}
		if err := unmarshalOptions.Unmarshal(payload[bodyStart:], msg); err != nil {
			log.Println("TcpCodec: unmarshal error:", err)
			if _, readErr := readFixedLength(conn, requiredTotalLen); readErr != nil && readErr != ErrInsufficientData {
				log.Println("Read unmarshal error packet error:", readErr)
			}
			fullData = fullData[requiredTotalLen:]
			continue
		}

		// 2.7 第六步：消费完整包+缓存剩余数据
		if _, readErr := readFixedLength(conn, requiredTotalLen); readErr != nil {
			log.Println("Read valid packet error:", readErr)
			return nil, readErr
		}
		// 缓存剩余未处理数据（供下次解码）
		if len(fullData) > requiredTotalLen {
			ctx.cachedData = append(ctx.cachedData, fullData[requiredTotalLen:]...)
		}
		// 保存解码结果
		ctx.msg = msg
		break // 解析成功一个包，退出循环
	}

	return nil, nil
}

// -----------------------------------------------------------------------------
// RpcCodec: RPC0标签编解码器（基于gnet.Peek接口）
// -----------------------------------------------------------------------------

// rpcConnContext: RpcCodec的连接上下文
type rpcConnContext struct {
	cachedData []byte // 暂存未消费的Peek数据
	msg        proto.Message
}

type RpcCodec struct {
	MsgType proto.Message // 提前指定的Proto消息类型（如&YourRPCMsg{}）
}

func NewRpcCodec(msgType proto.Message) *RpcCodec {
	if msgType == nil {
		log.Panic("RpcCodec: MsgType cannot be nil")
	}
	return &RpcCodec{MsgType: msgType}
}

// Encode: 编码RPC消息（带RPC0标签）
func (c *RpcCodec) Encode(conn gnet.Conn, _ []byte) ([]byte, error) {
	ctx, ok := conn.Context().(*rpcConnContext)
	if !ok || ctx.msg == nil {
		return nil, nil // 无待发送消息
	}

	// 1. 固定RPC标签
	tag := []byte("RPC0")
	// 2. 序列化消息体（v2接口）
	body, err := proto.MarshalOptions{}.Marshal(ctx.msg)
	if err != nil {
		return nil, err
	}

	// 3. 构建Payload（tag + body）
	payload := make([]byte, len(tag)+len(body))
	copy(payload[:len(tag)], tag)
	copy(payload[len(tag):], body)

	// 4. 计算校验和
	checksum := adler32.Checksum(payload)
	checksumBuf := uint32ToBytes(checksum)
	defer uint32BufPool.Put(checksumBuf)

	// 5. 拼接总长度（4字节）+ payload + checksum
	totalDataLen := len(payload) + 4 // payload长度 + 校验和长度
	totalLenBuf := uint32ToBytes(uint32(totalDataLen))
	defer uint32BufPool.Put(totalLenBuf)

	// 组装完整包
	fullPacket := make([]byte, 4+totalDataLen)
	copy(fullPacket[:4], totalLenBuf)
	copy(fullPacket[4:4+len(payload)], payload)
	copy(fullPacket[4+len(payload):], checksumBuf)

	return fullPacket, nil
}

// Decode: 基于Peek接口解码RPC包（带RPC0标签）
func (c *RpcCodec) Decode(conn gnet.Conn) ([]byte, error) {
	// 1. 获取/初始化连接上下文（缓存未消费数据 + 解码后的消息）
	ctx, ok := conn.Context().(*rpcConnContext)
	if !ok {
		// 预分配4KB缓存，减少后续扩容开销
		ctx = &rpcConnContext{cachedData: make([]byte, 0, 4096)}
		conn.SetContext(ctx)
	}

	// 2. 循环解析：处理TCP粘包/拆包，一次只解析一个完整包
	for {
		// 2.1 合并「历史缓存数据」和「新Peek数据」（非消费式读取，不推进缓冲区指针）
		newData, err := conn.Peek(0)     // Peek(0) 获取所有可用新数据
		if err != nil && err != io.EOF { // EOF暂忽略（对端关闭时后续会触发断开逻辑）
			log.Printf("RpcCodec Peek failed: %v", err)
			return nil, err
		}
		fullData := append(ctx.cachedData, newData...) // 合并数据确保连续性
		ctx.cachedData = ctx.cachedData[:0]            // 清空旧缓存，避免重复处理

		// 2.2 第一步：解析「总长度前缀」（4字节大端序，标识完整包长度）
		if len(fullData) < 4 {
			// 数据不足4字节（连长度都读不完），缓存后返回
			ctx.cachedData = append(ctx.cachedData, fullData...)
			return nil, ErrInsufficientData
		}
		// totalDataLen = payload（RPC0+body） + checksum（4字节） 的总长度
		totalDataLen := binary.BigEndian.Uint32(fullData[:4])
		// requiredTotalLen = 4字节长度前缀 + totalDataLen（完整包的总字节数）
		requiredTotalLen := 4 + int(totalDataLen)

		// 2.3 第二步：检查是否有完整的包（避免拆包场景）
		if len(fullData) < requiredTotalLen {
			// 数据不足完整包长度，缓存后等待下次数据到来
			ctx.cachedData = append(ctx.cachedData, fullData...)
			return nil, ErrInsufficientData
		}

		// 2.4 第三步：校验和验证（确保数据传输未篡改）
		payload := fullData[4 : requiredTotalLen-4] // 提取 payload（RPC0标签 + 消息体）
		// 提取预期校验和（包末尾4字节）
		checksumExpected := binary.BigEndian.Uint32(fullData[requiredTotalLen-4 : requiredTotalLen])
		// 计算实际校验和
		checksumActual := adler32.Checksum(payload)

		if checksumActual != checksumExpected {
			log.Println("RpcCodec:", ErrInvalidChecksum)
			// 消费错误包（必须推进缓冲区指针，避免错误数据残留）
			if _, readErr := readFixedLength(conn, requiredTotalLen); readErr != nil && readErr != ErrInsufficientData {
				log.Printf("RpcCodec failed to read invalid checksum packet: %v", readErr)
			}
			// 跳过错误包，继续解析后续数据（若有）
			fullData = fullData[requiredTotalLen:]
			continue
		}

		// 2.5 第四步：校验RPC固定标签（确保是合法的RPC包）
		if len(payload) < 4 || string(payload[:4]) != "RPC0" {
			log.Println("RpcCodec:", ErrInvalidTag)
			// 消费错误标签的包
			if _, readErr := readFixedLength(conn, requiredTotalLen); readErr != nil && readErr != ErrInsufficientData {
				log.Printf("RpcCodec failed to read invalid tag packet: %v", readErr)
			}
			// 跳过错误包
			fullData = fullData[requiredTotalLen:]
			continue
		}

		// 2.6 第五步：解析Proto消息类型并反序列化
		body := payload[4:] // 去掉"RPC0"标签，提取纯消息体
		// 获取提前指定的消息描述符（从c.MsgType）
		desc := c.MsgType.ProtoReflect().Descriptor()
		if desc == nil {
			log.Println("RpcCodec: empty message descriptor (invalid MsgType)")
			// 消费无效描述符的包
			if _, readErr := readFixedLength(conn, requiredTotalLen); readErr != nil && readErr != ErrInsufficientData {
				log.Printf("RpcCodec failed to read empty descriptor packet: %v", readErr)
			}
			fullData = fullData[requiredTotalLen:]
			continue
		}

		// 从Protobuf全局注册表查找消息类型（v2标准接口）
		msgType, err := protoregistry.GlobalTypes.FindMessageByName(desc.FullName())
		if err != nil {
			log.Printf("RpcCodec: %v, typeName=%s", ErrInvalidProtoType, desc.FullName())
			// 消费未知类型的包
			if _, readErr := readFixedLength(conn, requiredTotalLen); readErr != nil && readErr != ErrInsufficientData {
				log.Printf("RpcCodec failed to read unknown proto type packet: %v", readErr)
			}
			fullData = fullData[requiredTotalLen:]
			continue
		}

		// 创建消息实例（v2标准用法，替代废弃的proto.MessageV1）
		msg := msgType.New().Interface()
		// 反序列化消息体（用v2接口确保兼容性）
		unmarshalOptions := proto.UnmarshalOptions{}
		if err := unmarshalOptions.Unmarshal(body, msg); err != nil {
			log.Printf("RpcCodec unmarshal error: %v", err)
			// 消费反序列化失败的包
			if _, readErr := readFixedLength(conn, requiredTotalLen); readErr != nil && readErr != ErrInsufficientData {
				log.Printf("RpcCodec failed to read unmarshal error packet: %v", readErr)
			}
			fullData = fullData[requiredTotalLen:]
			continue
		}

		// 2.7 第六步：消费正确包 + 缓存剩余数据（关键步骤，避免重复解析）
		// 消费当前完整的正确包（推进gnet缓冲区指针，标记数据已处理）
		if _, readErr := readFixedLength(conn, requiredTotalLen); readErr != nil {
			log.Printf("RpcCodec failed to read valid packet: %v", readErr)
			return nil, readErr
		}

		// 若fullData有剩余数据（粘包场景），缓存到上下文供下次解析
		if len(fullData) > requiredTotalLen {
			ctx.cachedData = append(ctx.cachedData, fullData[requiredTotalLen:]...)
		}

		// 保存解码成功的消息到上下文，供业务层或Encode使用
		ctx.msg = msg
		break // 解析成功一个包后退出循环（避免一次处理过多包占用资源）
	}

	// 解码成功：返回nil（实际结果存储在conn.Context()的ctx.msg中）
	return nil, nil
}
