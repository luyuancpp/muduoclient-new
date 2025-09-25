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

// TcpClient: gnet 异步 TCP 客户端（修复连接管理、解码逻辑）
type TcpClient struct {
	codec     Codec              // 编解码器
	closed    atomic.Bool        // 客户端关闭状态（原子变量）
	ctx       context.Context    // 生命周期上下文
	cancel    context.CancelFunc // 关闭上下文的函数
	incoming  chan proto.Message // 业务层接收消息通道
	outgoing  chan []byte        // 业务层发送数据通道
	conn      gnet.Conn          // 当前活跃连接（关键：保存连接引用）
	connMutex sync.Mutex         // 保护 conn 的线程安全（多协程访问）
	wg        sync.WaitGroup     // 协程等待组（确保优雅关闭）
	client    *gnet.Client       // gnet 客户端实例
	network   string             // 网络类型（固定为 "tcp"）
	addr      string             // 服务器地址（如 "127.0.0.1:8080"）
	multicore bool               // 是否启用多核模式
	connected atomic.Bool        // 连接状态（true=已连接）
	connID    string
}

// tcpClientEvents: 实现 gnet 事件接口，处理连接/数据/关闭等事件
type tcpClientEvents struct {
	*gnet.BuiltinEventEngine            // 嵌入 gnet 内置事件引擎（减少重复实现）
	client                   *TcpClient // 关联的 TcpClient 实例
}

// ------------------------------ 事件回调实现（修复连接管理与解码） ------------------------------
// OnOpen: 连接建立时触发（保存连接引用+更新连接状态）
func (ev *tcpClientEvents) OnOpen(conn gnet.Conn) (out []byte, action gnet.Action) {
	tcpClient := ev.client
	// 保存连接引用（线程安全）
	tcpClient.connMutex.Lock()
	tcpClient.conn = conn
	tcpClient.connMutex.Unlock()
	tcpClient.connID = uuid.NewString()
	// 更新连接状态
	tcpClient.connected.Store(true)
	log.Printf("connected to server: %s (conn ID: %s)", tcpClient.addr, tcpClient.connID)

	// 可选：连接建立时发送握手消息（如认证包）
	// if handshakeMsg != nil {
	// 	if data, err := tcpClient.codec.Encode(handshakeMsg); err == nil {
	// 		out = data // gnet 会自动发送该数据
	// 	}
	// }

	return out, gnet.None
}

// OnClose: 连接关闭时触发（清理连接引用+更新状态）
// OnClose: 连接关闭时触发（判断是否需要重连）
func (ev *tcpClientEvents) OnClose(conn gnet.Conn, err error) gnet.Action {
	tcpClient := ev.client

	// 1. 清理连接引用+更新状态
	tcpClient.connMutex.Lock()
	if tcpClient.conn == conn {
		tcpClient.conn = nil
	}
	tcpClient.connMutex.Unlock()
	tcpClient.connected.Store(false)

	// 2. 判断是否需要重连：排除“主动关闭”场景
	if tcpClient.closed.Load() { // 客户端已主动调用 Close()
		log.Printf("conn %s closed (active shutdown), no reconnect", tcpClient.connID)
		return gnet.Shutdown
	}
	if err == nil { // 连接被正常关闭（如服务器主动断连且无错误）
		log.Printf("conn %s closed by server (no error), no reconnect", tcpClient.connID)
		return gnet.None
	}

	// 3. 意外断连：启动重连（用独立协程避免阻塞 gnet 事件循环）
	go func() {
		// 退避间隔配置：每次重连失败后，间隔翻倍（上限 30 秒）
		reconnectDelay := 3 * time.Second
		maxDelay := 30 * time.Second
		retryCount := 0

		for {
			// 检查客户端是否已主动关闭（避免重连过程中客户端退出）
			if tcpClient.closed.Load() {
				log.Println("reconnect canceled: client is closed")
				return
			}

			log.Printf("reconnecting to %s (retry %d, delay %v)...", tcpClient.addr, retryCount+1, reconnectDelay)
			time.Sleep(reconnectDelay)

			// 调用 gnet.Client.Dial 重新建立连接
			newConn, dialErr := tcpClient.client.Dial(tcpClient.network, tcpClient.addr)
			if dialErr == nil {
				// 重连成功：更新连接引用+状态，重置退避间隔
				tcpClient.connMutex.Lock()
				tcpClient.conn = newConn
				tcpClient.connMutex.Unlock()
				tcpClient.connected.Store(true)
				log.Printf("reconnected to %s successfully (new conn ID: %d)", tcpClient.addr, tcpClient.connID)
				return
			}

			// 重连失败：增加重试次数，翻倍退避间隔（不超过上限）
			retryCount++
			reconnectDelay *= 2
			if reconnectDelay > maxDelay {
				reconnectDelay = maxDelay
			}
			log.Printf("reconnect failed (retry %d): %v, next retry in %v", retryCount, dialErr, reconnectDelay)
		}
	}()

	return gnet.None
}

// OnTraffic: 数据到达时触发（修复解码逻辑，正确传递 ConnContext）
func (ev *tcpClientEvents) OnTraffic(conn gnet.Conn) (action gnet.Action) {
	tcpClient := ev.client

	// 1. 获取连接上下文（避免重复创建，确保 Decode 能修改）
	ctx, ok := conn.Context().(*muduocodec.ConnContext)
	if !ok {
		log.Println("OnTraffic: invalid ConnContext type")
		return gnet.None
	}

	// 2. 调用编解码器解码（解码结果会保存在 ctx.Msg 中）
	err := tcpClient.codec.Decode(conn)
	if err != nil {
		// 仅打印非“数据不足”的错误（数据不足是正常拆包场景）
		if !errors.Is(err, muduocodec.ErrInsufficientData) {
			log.Printf("OnTraffic: decode error: %v", err)
		}
		return gnet.None
	}

	// 3. 解码成功：将消息发送到业务层（非阻塞，避免阻塞事件循环）
	if ctx.Msg != nil {
		select {
		case tcpClient.incoming <- ctx.Msg:
			// 消息成功投递，清空 ctx.Msg 供下次使用
			ctx.Msg = nil
		default:
			log.Println("OnTraffic: incoming channel full, drop message")
		}
	}

	return gnet.None
}

// OnTick: 定时回调（可选，用于心跳检测）
func (ev *tcpClientEvents) OnTick() (delay time.Duration, action gnet.Action) {
	delay = 10 * time.Second // 每10秒触发一次
	tcpClient := ev.client

	// 仅在已连接状态下发送心跳
	if tcpClient.connected.Load() {
		// 示例：发送 Protobuf 心跳消息（需替换为你的实际心跳结构体）
		// heartbeatMsg := &pb.Heartbeat{Timestamp: time.Now().Unix()}
		// if err := tcpClient.Send(heartbeatMsg); err != nil {
		// 	log.Printf("OnTick: send heartbeat failed: %v", err)
		// }
	}

	return delay, gnet.None
}

// ------------------------------ 客户端初始化与启动（修复配置与连接逻辑） ------------------------------
// ClientConfig: 客户端配置结构体（替代未定义的 testConf）
type ClientConfig struct {
	Multicore bool // 是否启用多核模式（gnet 多事件循环）
	Async     bool // 是否启用异步 I/O（gnet 内部优化）
	Writev    bool // 是否启用 scatter/gather I/O（提升大文件发送性能）
}

// DefaultClientConfig: 默认客户端配置（简化用户使用）
func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		Multicore: false, // 单核心足够应对多数场景，避免多核调度开销
		Async:     false,
		Writev:    false,
	}
}

// NewTcpClient: 创建 TCP 客户端实例（修复连接引用、配置初始化）
func NewTcpClient(addr string, codec Codec) *TcpClient {
	return NewTcpClientWithConfig(addr, codec, DefaultClientConfig())
}

// NewTcpClientWithConfig: 带配置的客户端创建方法（支持自定义配置）
func NewTcpClientWithConfig(addr string, codec Codec, conf *ClientConfig) *TcpClient {
	// 初始化生命周期上下文
	ctx, cancel := context.WithCancel(context.Background())

	c := &TcpClient{
		codec:     codec,
		ctx:       ctx,
		cancel:    cancel,
		incoming:  make(chan proto.Message, 200), // 接收通道缓冲（避免业务层阻塞）
		outgoing:  make(chan []byte, 200),        // 发送通道缓冲（避免业务层阻塞）
		network:   "tcp",
		addr:      addr,
		multicore: conf.Multicore,
	}

	// 启动核心协程：1. 异步发送协程 2. gnet 客户端协程
	c.wg.Add(2)
	go c.asyncWriteLoop()      // 协程1：处理发送通道数据
	go c.startGnetClient(conf) // 协程2：启动 gnet 客户端并管理连接

	return c
}

// startGnetClient: 启动 gnet 客户端（修复连接创建与错误处理）
func (c *TcpClient) startGnetClient(conf *ClientConfig) {
	defer c.wg.Done()

	// 初始化 gnet 事件处理器
	clientEV := &tcpClientEvents{
		BuiltinEventEngine: &gnet.BuiltinEventEngine{},
		client:             c,
	}

	// 创建 gnet 客户端实例
	gnetClient, err := gnet.NewClient(
		clientEV,
		gnet.WithMulticore(conf.Multicore),   // 多核模式
		gnet.WithTCPNoDelay(gnet.TCPNoDelay), // 禁用 Nagle 算法（减少延迟）
		gnet.WithLockOSThread(true),          // 绑定线程（提升性能）
		gnet.WithTicker(true),                // 启用定时回调（支持心跳）
	)
	if err != nil {
		log.Fatalf("failed to create gnet client: %v", err)
	}
	c.client = gnetClient

	// 启动 gnet 客户端引擎
	if err := c.client.Start(); err != nil {
		log.Fatalf("failed to start gnet client: %v", err)
	}
	defer func() {
		// 协程退出时停止 gnet 客户端
		if err := c.client.Stop(); err != nil {
			log.Printf("failed to stop gnet client: %v", err)
		}
	}()

	// 连接服务器（支持自动重连）
	_, err = c.client.Dial(c.network, c.addr)
	if err != nil {
		log.Fatalf("failed to dial server %s: %v", c.addr, err)
	}
	log.Printf("gnet client started, connecting to %s", c.addr)

	// 阻塞等待客户端关闭（通过 ctx 控制）
	<-c.ctx.Done()
	log.Println("gnet client exiting...")
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

			// 检查客户端与连接状态
			if c.closed.Load() || !c.connected.Load() {
				log.Println("asyncWriteLoop: skip send (client closed or not connected)")
				continue
			}

			// 线程安全获取连接引用
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
					log.Printf("asyncWrite callback: failed to send data (len=%d): %v", len(data), err)
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
	// 检查客户端状态
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

// Close: 优雅关闭客户端（线程安全，可重复调用）
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
	// 3. 关闭连接（若存在）
	c.connMutex.Lock()
	if c.conn != nil {
		err := c.conn.Close()
		if err != nil {
			return
		}
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
