package net

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/luyuancpp/muduo-client-go/muduocodec"
	"github.com/panjf2000/gnet/v2"
	"google.golang.org/protobuf/proto"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// ------------------------------ 核心接口与结构体定义 ------------------------------
// Codec: 编解码器接口（适配 muduocodec.TcpCodec）
type Codec interface {
	Encode(msg proto.Message) ([]byte, error) // 编码消息为字节流
	Decode(conn gnet.Conn) error              // 从连接解码消息到 ConnContext
}

// ConnMeta: 连接元数据（与 gnet.Conn 强绑定，存储连接唯一标识及生命周期信息）
type ConnMeta struct {
	ConnID   string    // 自定义唯一连接ID（UUID生成）
	CreateAt time.Time // 连接建立时间
}

// TcpClient: gnet 异步 TCP 客户端（线程安全版）
type TcpClient struct {
	codec       Codec              // 编解码器实例
	closed      atomic.Bool        // 客户端关闭状态（原子变量，线程安全）
	ctx         context.Context    // 客户端生命周期上下文
	cancel      context.CancelFunc // 上下文取消函数
	incoming    chan proto.Message // 业务层接收消息通道（带缓冲）
	outgoing    chan []byte        // 业务层发送数据通道（带缓冲）
	conn        gnet.Conn          // 当前活跃连接
	connMutex   sync.Mutex         // 保护 conn 的互斥锁（多协程安全访问）
	connID      string             // 当前连接ID（需通过锁保护）
	connIDMutex sync.Mutex         // 保护 connID 的互斥锁（线程安全读写）
	wg          sync.WaitGroup     // 协程等待组（确保优雅关闭）
	client      *gnet.Client       // gnet 客户端实例
	network     string             // 网络类型（固定为 "tcp"）
	addr        string             // 服务器地址（如 "127.0.0.1:8080"）
	multicore   bool               // 是否启用 gnet 多核模式
	connected   atomic.Bool        // 连接状态（true=已连接，原子变量）
}

// tcpClientEvents: 实现 gnet 事件接口，处理连接/数据/定时等事件
type tcpClientEvents struct {
	*gnet.BuiltinEventEngine            // 嵌入 gnet 内置事件引擎（减少重复实现）
	client                   *TcpClient // 关联的 TcpClient 实例
}

// ------------------------------ 事件回调实现（线程安全+元数据绑定） ------------------------------
// OnOpen: 连接建立时触发（绑定元数据、更新连接状态）
func (ev *tcpClientEvents) OnOpen(conn gnet.Conn) (out []byte, action gnet.Action) {
	tcpClient := ev.client

	// 1. 生成连接元数据并绑定到 gnet.Conn（与连接强关联，支持多连接扩展）
	connID := uuid.NewString()
	connMeta := &ConnMeta{
		ConnID:   connID,
		CreateAt: time.Now(),
	}
	conn.SetContext(connMeta) // 元数据绑定到连接，后续可通过 conn.Context() 读取

	// 2. 线程安全更新 TcpClient 的 conn 和 connID（避免数据竞争）
	tcpClient.connMutex.Lock()
	tcpClient.conn = conn
	tcpClient.connMutex.Unlock()

	tcpClient.connIDMutex.Lock()
	tcpClient.connID = connID
	tcpClient.connIDMutex.Unlock()

	// 3. 更新连接状态并打印日志（明确当前连接ID）
	tcpClient.connected.Store(true)
	log.Printf("successfully connected to server: %s (connID: %s, connObjID: %d)",
		tcpClient.addr, connID, conn.ID())

	// 可选：连接建立时发送握手/认证消息（示例）
	// if handshakeMsg != nil {
	// 	handshakeData, err := tcpClient.codec.Encode(handshakeMsg)
	// 	if err == nil {
	// 		out = handshakeData // gnet 会自动发送该数据
	// 	} else {
	// 		log.Printf("OnOpen: encode handshake msg failed (connID: %s): %v", connID, err)
	// 	}
	// }

	return out, gnet.None
}

// OnClose: 连接关闭时触发（清理连接、判断重连）
func (ev *tcpClientEvents) OnClose(conn gnet.Conn, err error) gnet.Action {
	tcpClient := ev.client

	// 1. 从连接读取元数据（避免依赖 TcpClient 全局状态，更安全）
	connMeta, ok := conn.Context().(*ConnMeta)
	if !ok {
		connMeta = &ConnMeta{ConnID: "unknown-conn-id"} // 降级处理，避免日志报错
	}
	connID := connMeta.ConnID

	// 2. 线程安全清理当前连接（仅清理匹配的连接，避免误删新连接）
	tcpClient.connMutex.Lock()
	if tcpClient.conn == conn {
		tcpClient.conn = nil
	}
	tcpClient.connMutex.Unlock()

	// 3. 更新连接状态
	tcpClient.connected.Store(false)

	// 4. 判断是否需要重连（排除主动关闭场景）
	if tcpClient.closed.Load() {
		log.Printf("conn %s closed (active client shutdown), no reconnect", connID)
		return gnet.Shutdown // 主动关闭时，通知 gnet 停止事件循环
	}
	if err == nil {
		log.Printf("conn %s closed by server (normal close), no reconnect", connID)
		return gnet.None
	}

	// 5. 意外断连：启动独立协程执行重连（避免阻塞 gnet 事件循环）
	go tcpClient.reconnectLoop(connID)

	return gnet.None
}

// OnTraffic: 数据到达时触发（解码+投递消息到业务层）
func (ev *tcpClientEvents) OnTraffic(conn gnet.Conn) (action gnet.Action) {
	tcpClient := ev.client

	// 1. 获取 muduocodec 上下文（用于存储解码结果）
	codecCtx, ok := conn.Context().(*muduocodec.ConnContext)
	if !ok {
		log.Printf("OnTraffic: invalid muduocodec.ConnContext type (connID: %s)",
			conn.Context().(*ConnMeta).ConnID)
		return gnet.None
	}

	// 2. 调用编解码器解码（结果存储在 codecCtx.Msg 中）
	decodeErr := tcpClient.codec.Decode(conn)
	if decodeErr != nil {
		// 仅打印非“数据不足”的错误（数据不足是 TCP 拆包正常场景）
		if !errors.Is(decodeErr, muduocodec.ErrInsufficientData) {
			log.Printf("OnTraffic: decode failed (connID: %s): %v",
				conn.Context().(*ConnMeta).ConnID, decodeErr)
		}
		return gnet.None
	}

	// 3. 解码成功：非阻塞投递消息到业务层（避免阻塞 gnet 事件循环）
	if codecCtx.Msg != nil {
		select {
		case tcpClient.incoming <- codecCtx.Msg:
			codecCtx.Msg = nil // 清空上下文，供下次解码使用
		default:
			log.Printf("OnTraffic: incoming channel full (connID: %s), drop message",
				conn.Context().(*ConnMeta).ConnID)
		}
	}

	return gnet.None
}

// OnTick: 定时回调（用于心跳检测、连接保活等）
func (ev *tcpClientEvents) OnTick() (delay time.Duration, action gnet.Action) {
	delay = 10 * time.Second // 每10秒触发一次定时任务
	tcpClient := ev.client

	// 仅在已连接状态下执行心跳
	if tcpClient.connected.Load() {
		// 线程安全获取当前连接（避免空指针）
		tcpClient.connMutex.Lock()
		currentConn := tcpClient.conn
		tcpClient.connMutex.Unlock()

		if currentConn != nil {
			connID := currentConn.Context().(*ConnMeta).ConnID
			// 示例：发送 Protobuf 心跳消息（替换为业务实际心跳结构体）
			// heartbeatMsg := &pb.Heartbeat{
			// 	ConnID:    connID,
			// 	Timestamp: time.Now().Unix(),
			// }
			// if err := tcpClient.Send(heartbeatMsg); err != nil {
			// 	log.Printf("OnTick: send heartbeat failed (connID: %s): %v", connID, err)
			// }
		}
	}

	return delay, gnet.None
}

// ------------------------------ 重连逻辑（独立抽离，便于维护） ------------------------------
// reconnectLoop: 退避重连逻辑（失败后间隔翻倍，上限30秒）
func (c *TcpClient) reconnectLoop(closedConnID string) {
	reconnectDelay := 3 * time.Second     // 初始重连间隔
	maxReconnectDelay := 30 * time.Second // 最大重连间隔
	retryCount := 0                       // 重连重试次数

	for {
		// 检查客户端是否已主动关闭（避免无效重连）
		if c.closed.Load() {
			log.Printf("reconnect canceled (closed connID: %s): client is shutdown", closedConnID)
			return
		}

		// 打印重连日志（明确基于哪个连接的重连）
		log.Printf("reconnecting to server %s (closed connID: %s, retry: %d, delay: %v)",
			c.addr, closedConnID, retryCount+1, reconnectDelay)

		// 等待重连间隔
		time.Sleep(reconnectDelay)

		// 尝试建立新连接（gnet.Dial 会触发 OnOpen 回调，自动绑定新元数据）
		newConn, dialErr := c.client.Dial(c.network, c.addr)
		if dialErr == nil {
			// 重连成功：通过新连接的元数据获取新 connID
			newConnID := newConn.Context().(*ConnMeta).ConnID
			log.Printf("reconnect success (closed connID: %s, new connID: %s, new connObjID: %d)",
				closedConnID, newConnID, newConn.ID())
			return
		}

		// 重连失败：更新重试次数和退避间隔（翻倍，不超过最大值）
		retryCount++
		reconnectDelay *= 2
		if reconnectDelay > maxReconnectDelay {
			reconnectDelay = maxReconnectDelay
		}

		log.Printf("reconnect failed (closed connID: %s, retry: %d): %v, next delay: %v",
			closedConnID, retryCount, dialErr, reconnectDelay)
	}
}

// ------------------------------ 客户端初始化与启动（配置化+错误处理） ------------------------------
// ClientConfig: TCP 客户端配置结构体（支持自定义核心参数）
type ClientConfig struct {
	Multicore bool // 是否启用 gnet 多核模式（多事件循环）
	Async     bool // 是否启用 gnet 异步 I/O（内部优化）
	Writev    bool // 是否启用 scatter/gather I/O（提升大文件发送性能）
}

// DefaultClientConfig: 默认客户端配置（适配多数场景，避免过度配置）
func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		Multicore: false, // 单核心足够应对多数场景，减少多核调度开销
		Async:     false,
		Writev:    false,
	}
}

// NewTcpClient: 简化版客户端创建接口（使用默认配置）
func NewTcpClient(addr string, codec Codec) *TcpClient {
	return NewTcpClientWithConfig(addr, codec, DefaultClientConfig())
}

// NewTcpClientWithConfig: 全配置客户端创建接口（支持自定义配置）
func NewTcpClientWithConfig(addr string, codec Codec, conf *ClientConfig) *TcpClient {
	// 初始化客户端生命周期上下文（用于优雅关闭）
	ctx, cancel := context.WithCancel(context.Background())

	client := &TcpClient{
		codec:     codec,
		ctx:       ctx,
		cancel:    cancel,
		incoming:  make(chan proto.Message, 200), // 200 缓冲：避免业务层消费不及时导致阻塞
		outgoing:  make(chan []byte, 200),        // 200 缓冲：避免发送端突发流量导致阻塞
		network:   "tcp",
		addr:      addr,
		multicore: conf.Multicore,
	}

	// 启动核心协程（异步发送+gnet 客户端）
	client.wg.Add(2)
	go client.asyncWriteLoop()      // 协程1：处理发送通道数据
	go client.startGnetClient(conf) // 协程2：启动 gnet 引擎并管理连接

	return client
}

// startGnetClient: 启动 gnet 客户端引擎（初始化+启动+错误处理）
func (c *TcpClient) startGnetClient(conf *ClientConfig) {
	defer c.wg.Done() // 协程退出时通知等待组

	// 1. 初始化 gnet 事件处理器（绑定当前 TcpClient）
	gnetEvents := &tcpClientEvents{
		BuiltinEventEngine: &gnet.BuiltinEventEngine{},
		client:             c,
	}

	// 2. 创建 gnet 客户端实例（配置核心参数）
	gnetClient, createErr := gnet.NewClient(
		gnetEvents,
		gnet.WithMulticore(conf.Multicore),   // 多核模式开关
		gnet.WithTCPNoDelay(gnet.TCPNoDelay), // 禁用 Nagle 算法：减少小数据包延迟
		gnet.WithLockOSThread(true),          // 绑定线程：减少线程切换开销
		gnet.WithTicker(true),                // 启用定时回调：支持心跳功能
		// 可选：启用 TCP 保活（根据业务需求开启）
		// gnet.WithTCPKeepAlive(time.Minute*5),
		// gnet.WithTCPKeepAlivePeriod(time.Second*30),
	)
	if createErr != nil {
		log.Fatalf("failed to create gnet client: %v", createErr)
	}
	c.client = gnetClient

	// 3. 启动 gnet 引擎（非阻塞，内部启动事件循环）
	if startErr := c.client.Start(); startErr != nil {
		log.Fatalf("failed to start gnet client engine: %v", startErr)
	}

	// 4. 延迟关闭 gnet 引擎（确保客户端退出时资源清理）
	defer func() {
		if stopErr := c.client.Stop(); stopErr != nil {
			log.Printf("failed to stop gnet client engine: %v", stopErr)
		} else {
			log.Println("gnet client engine stopped successfully")
		}
	}()

	// 5. 建立初始连接（失败则退出，后续重连由 OnClose 触发）
	initialConn, dialErr := c.client.Dial(c.network, c.addr)
	if dialErr != nil {
		log.Fatalf("failed to establish initial connection to %s: %v", c.addr, dialErr)
	}
	log.Printf("initial connection established (connID: %s, connObjID: %d)",
		initialConn.Context().(*ConnMeta).ConnID, initialConn.ID())

	// 6. 阻塞等待客户端关闭（通过 ctx 控制生命周期）
	<-c.ctx.Done()
	log.Println("gnet client coroutine exiting (context canceled)")
}

// ------------------------------ 异步发送实现（修复连接引用与线程安全） ------------------------------
// asyncWriteLoop: 异步发送协程（从 outgoing 通道取数据，线程安全发送）
func (c *TcpClient) asyncWriteLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done(): // 客户端主动关闭
			log.Println("asyncWriteLoop: exiting (context canceled)")
			return

		case data, ok := <-c.outgoing: // 从业务层接收待发送数据
			if !ok { // 发送通道被关闭
				log.Println("asyncWriteLoop: exiting (outgoing channel closed)")
				return
			}

			// 检查客户端与连接状态（双重校验，确保线程安全）
			if c.closed.Load() || !c.connected.Load() {
				log.Println("asyncWriteLoop: skip send (client closed or not connected)")
				continue
			}

			// 线程安全获取连接引用（加锁保护）
			c.connMutex.Lock()
			conn := c.conn
			c.connMutex.Unlock()
			if conn == nil {
				log.Println("asyncWriteLoop: skip send (no active connection)")
				continue
			}

			// 调用 gnet 异步写（不阻塞当前协程）
			err := conn.AsyncWrite(data, func(conn gnet.Conn, err error) error {
				if err != nil {
					// 从连接元数据获取 connID，便于定位问题
					connMeta, ok := conn.Context().(*ConnMeta)
					connID := "unknown"
					if ok {
						connID = connMeta.ConnID
					}
					log.Printf("asyncWrite callback (conn %s): failed to send data (len=%d): %v", connID, len(data), err)
				}
				return err
			})
			if err != nil {
				log.Printf("asyncWrite init failed: len=%d: %v", len(data), err)
			}
		}
	}
}

// Send: 业务层发送接口（非阻塞，线程安全）
func (c *TcpClient) Send(msg proto.Message) error {
	// 检查客户端状态（原子操作，线程安全）
	if c.closed.Load() {
		return errors.New("tcp client: client is closed")
	}
	if !c.connected.Load() {
		return errors.New("tcp client: not connected to server")
	}

	// 编码消息
	data, err := c.codec.Encode(msg)
	if err != nil {
		return errors.Join(errors.New("tcp client: encode message failed"), err)
	}

	// 非阻塞发送到通道（避免业务层阻塞）
	select {
	case c.outgoing <- data:
		return nil
	default:
		return errors.New("tcp client: send buffer full (outgoing channel is full)")
	}
}

// ------------------------------ 业务层接收与优雅关闭 ------------------------------
// Recv: 业务层接收接口（阻塞直到有消息或客户端关闭）
func (c *TcpClient) Recv() (proto.Message, error) {
	select {
	case msg, ok := <-c.incoming:
		if !ok {
			return nil, errors.New("tcp client: incoming channel closed (client is closed)")
		}
		return msg, nil
	case <-c.ctx.Done():
		return nil, errors.New("tcp client: recv canceled (client is closed)")
	}
}

// Close: 优雅关闭客户端（线程安全，可重复调用，修复错误处理）
func (c *TcpClient) Close() {
	// 原子操作：确保只关闭一次
	if !c.closed.CompareAndSwap(false, true) {
		log.Println("tcp client: already closed")
		return
	}

	log.Println("tcp client: starting clean shutdown")
	// 1. 取消上下文（通知所有协程退出）
	c.cancel()

	// 2. 关闭发送通道（通知 asyncWriteLoop 退出）
	close(c.outgoing)

	// 3. 关闭连接（若存在）- 修复：即使关闭失败也继续后续清理
	c.connMutex.Lock()
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			log.Printf("Close: failed to close connection: %v", err)
			// 不 return，继续执行后续清理
		}
		c.conn = nil // 显式置空，避免重复关闭
	}
	c.connMutex.Unlock()

	// 4. 等待所有协程退出（确保资源清理完成）
	c.wg.Wait()

	// 5. 关闭接收通道（通知业务层 Recv 退出）
	close(c.incoming)

	log.Println("tcp client: closed completely")
}

// IsConnected: 检查是否已连接（线程安全）
func (c *TcpClient) IsConnected() bool {
	return c.connected.Load()
}

// IsClosed: 检查是否已关闭（线程安全）
func (c *TcpClient) IsClosed() bool {
	return c.closed.Load()
}
