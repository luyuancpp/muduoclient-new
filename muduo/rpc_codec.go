package muduo

import (
	"encoding/binary"
	"errors"
	"github.com/panjf2000/gnet/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"
	"hash/adler32"
	"io"
	"log"
)

// ------------------------------ 连接上下文与编解码器结构体 ------------------------------
// rpcConnContext：gnet连接上下文，存储缓存数据和解码结果
type rpcConnContext struct {
	cachedData []byte        // 暂存未完整解码的数据（处理拆包）
	msg        proto.Message // 解码成功后的Protobuf消息
}

// RpcCodec：RPC协议编解码器（基于Protobuf+TCP）
type RpcCodec struct {
	MsgType proto.Message // 预定义的RPC消息类型（如&pb.UserRequest{}）
}

// ------------------------------ 编解码器构造函数 ------------------------------
func NewRpcCodec(msgType proto.Message) *RpcCodec {
	if msgType == nil {
		log.Panic("rpc codec: message type cannot be nil") // Panic：构造函数不允许空类型
	}
	return &RpcCodec{MsgType: msgType}
}

// ------------------------------ 编码逻辑（Encode） ------------------------------
func (c *RpcCodec) Encode(msg proto.Message) ([]byte, error) {
	// 1. 校验消息合法性
	if msg == nil {
		return nil, errors.New("rpc codec: encode nil message")
	}

	// 2. 固定RPC标签（4字节"RPC0"，用于解码时识别RPC包）
	tag := []byte("RPC0")

	// 3. 序列化Protobuf消息体（确定性序列化，确保跨语言/版本兼容性）
	body, err := proto.MarshalOptions{
		AllowPartial:  false, // 不允许部分序列化（确保消息完整）
		Deterministic: true,  // 相同消息生成相同字节流（避免哈希不一致）
	}.Marshal(msg)
	if err != nil {
		return nil, errors.Join(errors.New("rpc codec: marshal body failed"), err)
	}

	// 4. 构建Payload（标签+消息体）：校验和仅计算此部分，确保数据完整性
	payload := make([]byte, 0, len(tag)+len(body))
	payload = append(payload, tag...)
	payload = append(payload, body...)

	// 5. 计算Adler32校验和（轻量、高效，适合TCP数据完整性校验）
	checksum := adler32.Checksum(payload)
	checksumBuf := uint32BufPool.Get().([]byte)
	defer uint32BufPool.Put(checksumBuf) // 延迟归还内存池
	binary.BigEndian.PutUint32(checksumBuf, checksum)

	// 6. 计算总长度（Payload长度 + 4字节校验和）
	totalDataLen := uint32(len(payload) + 4)
	totalLenBuf := uint32BufPool.Get().([]byte)
	defer uint32BufPool.Put(totalLenBuf)
	binary.BigEndian.PutUint32(totalLenBuf, totalDataLen)

	// 7. 组装完整TCP数据包：[4字节总长度] + [Payload] + [4字节校验和]
	fullPacket := make([]byte, 0, 4+len(payload)+4)
	fullPacket = append(fullPacket, totalLenBuf...)
	fullPacket = append(fullPacket, payload...)
	fullPacket = append(fullPacket, checksumBuf...)

	return fullPacket, nil
}

// ------------------------------ 解码逻辑（Decode） ------------------------------
// Decode：从gnet连接解码RPC消息，结果存储在rpcConnContext.msg中
func (c *RpcCodec) Decode(conn gnet.Conn) error {
	// 1. 初始化/获取连接上下文（确保每个连接有独立缓存）
	ctx, ok := conn.Context().(*rpcConnContext)
	if !ok {
		ctx = &rpcConnContext{
			cachedData: make([]byte, 0, 4096), // 预分配4KB缓存，减少扩容开销
		}
		conn.SetContext(ctx)
	}

	// 2. 校验预定义消息类型（避免解码时无目标类型）
	if c.MsgType == nil {
		return ErrEmptyMsgType
	}

	// 3. 循环解析：处理粘包/拆包（一次解析一个完整包，避免阻塞事件循环）
	for {
		// 3.1 合并历史缓存与新数据（Peek非消费式，不修改内核缓冲区指针）
		newData, err := conn.Peek(0)
		if err != nil && err != io.EOF {
			return errors.Join(errors.New("rpc codec: peek new data failed"), err)
		}
		fullData := append(ctx.cachedData, newData...)
		ctx.cachedData = ctx.cachedData[:0] // 清空旧缓存，避免重复处理

		// 3.2 第一步：校验总长度前缀（至少4字节）
		if len(fullData) < 4 {
			ctx.cachedData = append(ctx.cachedData, fullData...) // 缓存不足数据
			return ErrInsufficientData
		}

		// 3.3 第二步：解析总长度，判断是否有完整包
		totalDataLen := binary.BigEndian.Uint32(fullData[:4]) // Payload + 校验和长度
		requiredTotalLen := 4 + int(totalDataLen)             // 完整包总长度（4字节长度+数据）
		if len(fullData) < requiredTotalLen {
			ctx.cachedData = append(ctx.cachedData, fullData...) // 缓存不足数据
			return ErrInsufficientData
		}

		// -------------------------- 核心优化：统一用defer Discard消费数据 --------------------------
		// 仅在确认有完整包后，绑定defer Discard（避免重复消费）
		defer func() {
			discarded, discardErr := conn.Discard(requiredTotalLen)
			if discardErr != nil {
				log.Printf("rpc codec: discard failed (required %d, discarded %d): %v", requiredTotalLen, discarded, discardErr)
			} else if discarded != requiredTotalLen {
				log.Printf("rpc codec: discard length mismatch (required %d, discarded %d)", requiredTotalLen, discarded)
			}
		}()

		// 3.4 第三步：提取Payload和校验和（与编码逻辑严格对齐）
		payloadEnd := 4 + int(totalDataLen) - 4 // Payload结束位置（排除4字节校验和）
		payload := fullData[4:payloadEnd]
		checksumExpected := binary.BigEndian.Uint32(fullData[payloadEnd:requiredTotalLen])

		// 3.5 第四步：校验和验证（检测数据篡改或传输错误）
		if adler32.Checksum(payload) != checksumExpected {
			log.Println("rpc codec:", ErrInvalidChecksum)
			return ErrInvalidChecksum // 退出后defer会自动消费错误包
		}

		// 3.6 第五步：校验RPC标签（确保是目标RPC包，过滤无效数据）
		if len(payload) < 4 || string(payload[:4]) != "RPC0" {
			log.Println("rpc codec:", ErrInvalidTag)
			return ErrInvalidTag // 退出后defer会自动消费错误包
		}

		// 3.7 第六步：提取消息体并反序列化
		body := payload[4:] // 去掉"RPC0"标签，获取纯消息体
		// 从Protobuf全局注册表查找消息类型（基于预定义的MsgType）
		msgTypeName := c.MsgType.ProtoReflect().Descriptor().FullName()
		msgType, err := protoregistry.GlobalTypes.FindMessageByName(msgTypeName)
		if err != nil {
			log.Printf("rpc codec: find message type failed (name=%s): %v", msgTypeName, err)
			return errors.Join(errors.New("rpc codec: find message type failed"), err)
		}

		// 创建消息实例并反序列化（忽略未知字段，提高版本兼容性）
		msg := msgType.New().Interface()
		unmarshalOptions := proto.UnmarshalOptions{
			AllowPartial:   false, // 不允许部分反序列化（确保消息完整）
			DiscardUnknown: true,  // 忽略未知字段（提高兼容性）
		}
		if err := unmarshalOptions.Unmarshal(body, msg); err != nil {
			log.Printf("rpc codec: unmarshal body failed: %v", err)
			return errors.Join(errors.New("rpc codec: unmarshal body failed"), err)
		}

		// 3.8 第七步：缓存剩余数据（粘包场景，供下次解码）
		if len(fullData) > requiredTotalLen {
			ctx.cachedData = append(ctx.cachedData, fullData[requiredTotalLen:]...)
		}

		// 3.9 保存解码结果到上下文
		ctx.msg = msg
		break // 解析成功一个包，退出循环（避免单次处理过多包导致阻塞）
	}

	return nil
}

// ------------------------------ 辅助函数：移除重复的consumePacket ------------------------------
// 说明：原代码中的consumePacket已被defer Discard替代，故移除；若需保留，需确保与Discard不重复调用
