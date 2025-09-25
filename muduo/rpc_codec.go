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

func NewRpcCodec(msgType proto.Message) *RpcCodec {
	if msgType == nil {
		log.Panic("RpcCodec: MsgType cannot be nil")
	}
	return &RpcCodec{MsgType: msgType}
}

// rpcConnContext：gnet 连接上下文，存储缓存数据和解码后的消息
// 需在连接初始化时绑定到 conn.Context()
type rpcConnContext struct {
	cachedData []byte        // 缓存未处理的 TCP 数据（用于解决粘包/拆包）
	msg        proto.Message // 解码成功后的 Protobuf 消息
}

type RpcCodec struct {
	MsgType proto.Message // 预定义的 RPC 消息类型（如 &pb.UserRequest{}）
}

func (c *RpcCodec) Encode(msg proto.Message) ([]byte, error) {

	// 1. 固定 RPC 标签（4 字节 "RPC0"，用于解码时识别 RPC 包）
	tag := []byte("RPC0")

	// 4. 序列化 Protobuf 消息体（使用 v2 接口，确保兼容性）
	body, err := proto.MarshalOptions{
		AllowPartial:  false, // 不允许部分序列化（确保消息完整）
		Deterministic: true,  // 确定性序列化（相同消息生成相同字节流）
	}.Marshal(msg)
	if err != nil {
		return nil, errors.Join(errors.New("marshal message body failed"), err)
	}

	// 2. 构建 Payload（标签 + 消息体）：校验和仅计算此部分，确保数据完整性
	payload := make([]byte, 0, len(tag)+len(body))
	payload = append(payload, tag...)
	payload = append(payload, body...)

	// 3. 计算 Adler32 校验和（用于检测数据传输篡改）
	checksum := adler32.Checksum(payload)
	checksumBuf := uint32BufPool.Get().([]byte)
	defer uint32BufPool.Put(checksumBuf) // 延迟归还缓冲区到池
	binary.BigEndian.PutUint32(checksumBuf, checksum)

	// 4. 计算总长度（Payload 长度 + 校验和长度）
	totalDataLen := uint32(len(payload) + 4) // 4 = 校验和字节数
	totalLenBuf := uint32BufPool.Get().([]byte)
	defer uint32BufPool.Put(totalLenBuf)
	binary.BigEndian.PutUint32(totalLenBuf, totalDataLen)

	// 5. 组装完整 TCP 数据包：[4字节总长度] + [Payload] + [4字节校验和]
	fullPacket := make([]byte, 0, 4+len(payload)+4)
	fullPacket = append(fullPacket, totalLenBuf...)
	fullPacket = append(fullPacket, payload...)
	fullPacket = append(fullPacket, checksumBuf...)

	return fullPacket, nil
}

// Decode：解码 TCP 数据为 RPC 消息（基于 gnet Peek 接口，非消费式读取）
// 输入：gnet.Conn（从连接读取数据，缓存未处理数据）
// 输出：nil（解码结果存储在 conn.Context().(*rpcConnContext).Msg 中）、错误信息
func (c *RpcCodec) Decode(conn gnet.Conn) error {
	// 1. 初始化/获取连接上下文
	ctx, ok := conn.Context().(*rpcConnContext)
	if !ok {
		// 首次解码：创建上下文并绑定到连接（预分配 4KB 缓存，减少扩容）
		ctx = &rpcConnContext{
			cachedData: make([]byte, 0, 4096),
		}
		conn.SetContext(ctx)
	}

	// 2. 校验预定义消息类型是否初始化
	if c.MsgType == nil {
		return ErrEmptyMsgType
	}

	// 3. 循环解析：处理粘包/拆包（一次仅解析一个完整包，避免资源占用）
	for {
		// 3.1 合并「历史缓存数据」和「新读取数据」（Peek 非消费式，不推进缓冲区指针）
		newData, err := conn.Peek(0) // Peek(0) 获取所有可用新数据
		if err != nil && err != io.EOF {
			return errors.Join(errors.New("peek new data failed"), err)
		}
		fullData := append(ctx.cachedData, newData...)
		ctx.cachedData = ctx.cachedData[:0] // 清空旧缓存，避免重复处理

		// 3.2 第一步：校验「总长度前缀」是否足够（至少 4 字节）
		if len(fullData) < 4 {
			ctx.cachedData = append(ctx.cachedData, fullData...) // 缓存不足数据，等待下次
			return ErrInsufficientData
		}

		// 3.3 第二步：解析总长度，判断是否有完整包
		totalDataLen := binary.BigEndian.Uint32(fullData[:4]) // Payload + 校验和 长度
		requiredTotalLen := 4 + int(totalDataLen)             // 完整包总长度：4字节长度 + totalDataLen
		if len(fullData) < requiredTotalLen {
			ctx.cachedData = append(ctx.cachedData, fullData...) // 数据不足，缓存后退出
			return ErrInsufficientData
		}

		// 3.4 第三步：提取 Payload 和 校验和（修正边界，与编码逻辑对齐）
		payload := fullData[4 : 4+int(totalDataLen)-4]                            // Payload = 标签 + 消息体
		checksumExpectedBuf := fullData[4+int(totalDataLen)-4 : requiredTotalLen] // 校验和（4字节）
		if len(checksumExpectedBuf) != 4 {
			log.Println("rpc codec: invalid checksum length (expected 4 bytes)")
			if err := consumePacket(conn, requiredTotalLen); err != nil {
				return errors.Join(ErrConsumeFailed, err)
			}
			fullData = fullData[requiredTotalLen:] // 跳过错误包，继续解析后续数据
			continue
		}
		checksumExpected := binary.BigEndian.Uint32(checksumExpectedBuf)

		// 3.5 第四步：校验和验证（检测数据是否篡改）
		checksumActual := adler32.Checksum(payload)
		if checksumActual != checksumExpected {
			log.Println("rpc codec:", ErrInvalidChecksum)
			if err := consumePacket(conn, requiredTotalLen); err != nil {
				return errors.Join(ErrConsumeFailed, err)
			}
			fullData = fullData[requiredTotalLen:]
			continue
		}

		// 3.6 第五步：校验 RPC 固定标签（确保是目标 RPC 包，避免解析其他 TCP 数据）
		if len(payload) < 4 || string(payload[:4]) != "RPC0" {
			log.Println("rpc codec:", ErrInvalidTag)
			if err := consumePacket(conn, requiredTotalLen); err != nil {
				return errors.Join(ErrConsumeFailed, err)
			}
			fullData = fullData[requiredTotalLen:]
			continue
		}

		// 3.7 第六步：提取消息体并反序列化
		body := payload[4:] // 去掉 "RPC0" 标签，获取纯消息体
		// 从 Protobuf 全局注册表查找消息类型（基于预定义的 MsgType）
		msgTypeName := c.MsgType.ProtoReflect().Descriptor().FullName()
		msgType, err := protoregistry.GlobalTypes.FindMessageByName(msgTypeName)
		if err != nil {
			log.Printf("rpc codec: find message type failed (name=%s): %v", msgTypeName, err)
			if err := consumePacket(conn, requiredTotalLen); err != nil {
				return errors.Join(ErrConsumeFailed, err)
			}
			fullData = fullData[requiredTotalLen:]
			continue
		}

		// 创建消息实例并反序列化（v2 标准接口，替代废弃的 proto.MessageV1）
		msg := msgType.New().Interface()
		unmarshalOptions := proto.UnmarshalOptions{
			AllowPartial:   false, // 不允许部分反序列化（确保消息完整）
			DiscardUnknown: true,  // 忽略未知字段（提高兼容性）
		}
		if err := unmarshalOptions.Unmarshal(body, msg); err != nil {
			log.Printf("rpc codec: unmarshal message body failed: %v", err)
			if err := consumePacket(conn, requiredTotalLen); err != nil {
				return errors.Join(ErrConsumeFailed, err)
			}
			fullData = fullData[requiredTotalLen:]
			continue
		}

		// 3.8 第七步：消费完整包 + 缓存剩余数据（关键步骤，避免重复解析）
		if err := consumePacket(conn, requiredTotalLen); err != nil {
			return errors.Join(ErrConsumeFailed, err)
		}
		// 粘包场景：缓存剩余未处理数据，供下次解码
		if len(fullData) > requiredTotalLen {
			ctx.cachedData = append(ctx.cachedData, fullData[requiredTotalLen:]...)
		}

		// -------------------------- 核心修改：用 Discard 替代 readFixedLength --------------------------
		// 无论解析成功/失败，最终都要 Discard 已处理的 requiredTotalLen 字节
		defer func() {
			// 调用 Discard 消费数据（从缓冲区移除 requiredTotalLen 字节）
			discarded, discardErr := conn.Discard(requiredTotalLen)
			if discardErr != nil {
				log.Printf("Discard failed: required %d, discarded %d, err: %v", requiredTotalLen, discarded, discardErr)
			} else if discarded != requiredTotalLen {
				// 极端情况：缓冲区数据被意外修改，Discard 长度不匹配（需告警）
				log.Printf("Discard length mismatch: required %d, discarded %d", requiredTotalLen, discarded)
			}
		}()

		// 3.9 保存解码结果到上下文，供业务层使用
		ctx.msg = msg
		break // 解析成功一个包，退出循环（避免单次处理过多包）
	}

	return nil // 解码成功，结果通过上下文传递
}
