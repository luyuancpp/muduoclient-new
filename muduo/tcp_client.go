package muduo

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/panjf2000/gnet/v2"
	"google.golang.org/protobuf/proto"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// ------------------------------ 核心接口与精简结构体 ------------------------------
// Codec: 编解码器接口（不变）
type Codec interface {
	Encode(msg proto.Message) ([]byte, error) // 编码Protobuf消息
	Decode(conn gnet.Conn) error              // 解码到muduocodec上下文
}

// ConnMeta: 连接元数据（仅绑定到conn，不冗余到TcpClient）
type ConnMeta struct {
	ConnID   string    // 唯一连接标识（UUID）
	CreateAt time.Time // 连接建立时间
}

// ConnContext: TcpCodec的连接上下文（缓存未消费数据+解码后的消息）
type ConnContext struct {
	ConnMeta                 // 嵌入连接元数据（继承 ConnID、CreateAt）
	Msg        proto.Message // 解码后的 Protobuf 消息
	cachedData []byte        // 拆包剩余的缓冲数据
	// ... 其他编解码器需要的字段
}

// TcpClient: 单连接精简版结构体（删除冗余锁和字段）
type TcpClient struct {
	codec     Codec              // 编解码器实例
	closed    atomic.Bool        // 客户端关闭状态（原子变量）
	ctx       context.Context    // 生命周期上下文
	cancel    context.CancelFunc // 上下文取消函数
	incoming  chan proto.Message // 业务层接收通道（带缓冲）
	outgoing  chan []byte        // 业务层发送通道（带缓冲）
	conn      atomic.Value       // 活跃连接（原子变量替代互斥锁）
	wg        sync.WaitGroup     // 协程等待组（优雅关闭）
	client    *gnet.Client       // gnet客户端实例
	network   string             // 网络类型（固定tcp）
	addr      string             // 服务器地址（如127.0.0.1:8080）
	multicore bool               // gnet多核模式开关
	connected atomic.Bool        // 连接状态（原子变量）
}

// tcpClientEvents: gnet事件处理器（绑定TcpClient）
type tcpClientEvents struct {
	*gnet.BuiltinEventEngine // 嵌入默认事件实现（减少重复代码）
	client                   *TcpClient
}

// ------------------------------ 事件回调（简化版） ------------------------------
func (ev *tcpClientEvents) OnOpen(conn gnet.Conn) (out []byte, action gnet.Action) {
	tcpClient := ev.client
	if tcpClient.closed.Load() {
		return nil, gnet.Close
	}

	// 1. 初始化 ConnContext（同时包含元数据和编解码器状态）
	connCtx := &ConnContext{
		ConnMeta: ConnMeta{ // 初始化嵌入的元数据
			ConnID:   uuid.NewString(),
			CreateAt: time.Now(),
		},
		cachedData: make([]byte, 0, 4096), // 初始化缓冲（大小根据协议调整）
		Msg:        nil,                   // 初始化为空消息
		// ... 其他编解码器字段的初始化（如包长度计数器等）
	}

	// 2. 将 ConnContext 绑定到 conn（关键步骤！）
	conn.SetContext(connCtx)

	// 3. 原子存储连接和状态（原有逻辑不变）
	tcpClient.conn.Store(conn)
	tcpClient.connected.Store(true)
	log.Printf("已连接服务器: %s (connID: %s)", tcpClient.addr, connCtx.ConnID) // 直接从 connCtx 取 ConnID

	return out, gnet.None
}

// OnClose: 连接关闭（原子清空conn，判断重连）
func (ev *tcpClientEvents) OnClose(conn gnet.Conn, err error) gnet.Action {
	tcpClient := ev.client
	// 从conn读取元数据（无需依赖TcpClient字段）
	connMeta, _ := conn.Context().(*ConnContext)
	connID := "unknown"
	if connMeta != nil {
		connID = connMeta.ConnID
	}

	// 仅清空当前活跃连接（单连接场景无并发冲突）
	if currentConn := tcpClient.conn.Load(); currentConn == conn {
		tcpClient.conn.Store(nil)
		tcpClient.connected.Store(false)
	}

	// 主动关闭场景：不重连
	if tcpClient.closed.Load() {
		log.Printf("连接关闭: %s (主动关闭)", connID)
		return gnet.Shutdown
	}

	// 服务器正常关闭：不重连
	if err == nil {
		log.Printf("连接关闭: %s (服务器正常关闭)", connID)
		return gnet.None
	}

	// 意外断连：启动重连（独立协程避免阻塞gnet）
	log.Printf("连接异常关闭: %s (err: %v)，启动重连", connID, err)
	go tcpClient.reconnectLoop()

	return gnet.None
}

// OnTraffic: 数据接收（简化解码与消息投递）
func (ev *tcpClientEvents) OnTraffic(conn gnet.Conn) (action gnet.Action) {
	tcpClient := ev.client
	// 1. 读取 ConnContext（此时已包含 ConnMeta，不会为空）
	connCtx, ok := conn.Context().(*ConnContext)
	if !ok {
		log.Printf("数据接收异常: 无效的 ConnContext")
		return gnet.None
	}

	// 2. 解码（使用 connCtx 中的缓冲和状态，原有逻辑不变）
	decodeErr := tcpClient.codec.Decode(conn) // 注意：Codec.Decode 需适配 ConnContext
	if decodeErr != nil && !errors.Is(decodeErr, ErrInsufficientData) {
		log.Printf("解码失败 (connID: %s): %v", connCtx.ConnID, decodeErr) // 从 connCtx 取 ConnID
		return gnet.None
	}

	// 3. 投递消息（使用 connCtx 中的 Msg，原有逻辑不变）
	if connCtx.Msg != nil {
		select {
		case tcpClient.incoming <- connCtx.Msg:
			connCtx.Msg = nil // 清空消息供下次使用
		default:
			log.Printf("接收通道满 (connID: %s)，丢弃消息", connCtx.ConnID)
		}
	}

	return gnet.None
}

// OnTick: 定时心跳（简化连接读取与状态判断）
func (ev *tcpClientEvents) OnTick() (delay time.Duration, action gnet.Action) {
	//delay = 10 * time.Second // 10秒心跳间隔
	//tcpClient := ev.client

	// 未连接则跳过心跳
	//if !tcpClient.connected.Load() {
	//return delay, gnet.None
	//}

	// 原子读取连接（无锁）
	//conn, ok := tcpClient.conn.Load().(gnet.Conn)
	//if !ok || conn == nil {
	//return delay, gnet.None
	//}
	//connMeta := conn.Context().(*ConnMeta)

	// 示例：发送心跳（替换为实际Protobuf结构体）
	// heartbeatMsg := &pb.Heartbeat{
	// 	ConnID:    connMeta.ConnID,
	// 	Timestamp: time.Now().Unix(),
	// }
	// if err := tcpClient.Send(heartbeatMsg); err != nil {
	// 	log.Printf("心跳发送失败 (connID: %s): %v", connMeta.ConnID, err)
	// }

	return delay, gnet.None
}

// ------------------------------ 重连逻辑（简化版） ------------------------------
// reconnectLoop: 退避重连（无冗余参数，单连接场景无需跟踪旧connID）
func (c *TcpClient) reconnectLoop() {
	reconnectDelay := 3 * time.Second // 初始重连间隔
	maxDelay := 30 * time.Second      // 最大重连间隔（避免频繁重试）
	retryCount := 0                   // 重试次数

	for {
		// 客户端已关闭则终止重连
		if c.closed.Load() {
			log.Println("重连终止：客户端已关闭")
			return
		}

		// 打印简洁重连日志
		log.Printf("重连尝试 %d (间隔: %v) - 目标服务器: %s", retryCount+1, reconnectDelay, c.addr)
		time.Sleep(reconnectDelay)

		// 尝试建立新连接（gnet.Dial自动触发OnOpen）
		newConn, err := c.client.Dial(c.network, c.addr)
		if err == nil {
			// 重连成功：从新conn读取元数据
			newConnMeta := newConn.Context().(*ConnContext)
			log.Printf("重连成功 (新connID: %s)", newConnMeta.ConnID)
			return
		}

		// 重连失败：更新退避间隔（翻倍，不超过最大值）
		retryCount++
		reconnectDelay *= 2
		if reconnectDelay > maxDelay {
			reconnectDelay = maxDelay
		}
		log.Printf("重连失败 (尝试 %d): %v，下次间隔: %v", retryCount, err, reconnectDelay)
	}
}

// ------------------------------ 客户端初始化（简化配置） ------------------------------
// ClientConfig: 精简配置（仅保留核心参数）
type ClientConfig struct {
	Multicore bool // 是否启用gnet多核模式（单连接场景建议关闭）
}

// DefaultClientConfig: 默认配置（单连接场景最优）
func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{Multicore: false}
}

// NewTcpClient: 简化创建接口（默认配置）
func NewTcpClient(addr string, codec Codec) *TcpClient {
	return NewTcpClientWithConfig(addr, codec, DefaultClientConfig())
}

// NewTcpClientWithConfig: 全配置创建（支持自定义多核模式）
func NewTcpClientWithConfig(addr string, codec Codec, conf *ClientConfig) *TcpClient {
	// 初始化生命周期上下文
	ctx, cancel := context.WithCancel(context.Background())

	client := &TcpClient{
		codec:     codec,
		ctx:       ctx,
		cancel:    cancel,
		incoming:  make(chan proto.Message, 200), // 200缓冲避免业务层阻塞
		outgoing:  make(chan []byte, 200),        // 200缓冲应对突发发送
		network:   "tcp",
		addr:      addr,
		multicore: conf.Multicore,
	}

	// 启动核心协程（仅2个：异步发送+gnet引擎）
	client.wg.Add(2)
	go client.asyncWriteLoop()
	go client.startGnetClient()

	return client
}

// startGnetClient: 启动gnet引擎（简化错误处理）
func (c *TcpClient) startGnetClient() {
	defer c.wg.Done()

	// 初始化gnet事件处理器
	gnetEvents := &tcpClientEvents{
		BuiltinEventEngine: &gnet.BuiltinEventEngine{},
		client:             c,
	}

	// 创建gnet客户端（核心参数配置）
	gnetClient, err := gnet.NewClient(
		gnetEvents,
		gnet.WithMulticore(c.multicore),      // 多核模式（按配置）
		gnet.WithTCPNoDelay(gnet.TCPNoDelay), // 禁用Nagle算法（减少延迟）
		gnet.WithLockOSThread(true),          // 绑定线程（提升性能）
		gnet.WithTicker(true),                // 启用定时回调（支持心跳）
	)
	if err != nil {
		log.Fatalf("gnet客户端创建失败: %v", err)
	}
	c.client = gnetClient

	// 启动gnet（非阻塞）
	if err := c.client.Start(); err != nil {
		log.Fatalf("gnet引擎启动失败: %v", err)
	}

	// 延迟关闭gnet（确保退出时清理）
	defer func() {
		if stopErr := c.client.Stop(); stopErr != nil {
			log.Printf("gnet引擎关闭失败: %v", stopErr)
		} else {
			log.Println("gnet引擎正常关闭")
		}
	}()

	// 建立初始连接（失败则退出）
	initialConn, err := c.client.Dial(c.network, c.addr)
	if err != nil {
		log.Fatalf("初始连接失败: %v", err)
	}
	initialConnMeta := initialConn.Context().(*ConnContext)
	log.Printf("初始连接成功 (connID: %s)", initialConnMeta.ConnID)

	// 阻塞等待客户端关闭
	<-c.ctx.Done()
	log.Println("gnet协程退出")
}

// ------------------------------ Send及后续核心函数（优化版） ------------------------------
// asyncWriteLoop: 异步发送协程（无锁，原子读取conn）
func (c *TcpClient) asyncWriteLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done(): // 客户端关闭信号
			log.Println("异步发送协程退出：上下文关闭")
			return

		case data, ok := <-c.outgoing: // 从业务层接收待发送数据
			if !ok { // 发送通道被关闭（Close函数触发）
				log.Println("异步发送协程退出：发送通道关闭")
				return
			}

			// 双重状态校验（原子操作，无锁）
			if c.closed.Load() || !c.connected.Load() {
				log.Printf("跳过发送（数据长度: %d）：客户端关闭或未连接", len(data))
				continue
			}

			// 原子读取连接（无锁，避免并发冲突）
			conn, ok := c.conn.Load().(gnet.Conn)
			if !ok || conn == nil {
				log.Printf("跳过发送（数据长度: %d）：无活跃连接", len(data))
				continue
			}
			connMeta := conn.Context().(*ConnContext)

			// 调用gnet异步写（不阻塞当前协程）
			err := conn.AsyncWrite(data, func(_ gnet.Conn, err error) error {
				if err != nil {
					log.Printf("发送失败 (connID: %s, 数据长度: %d): %v", connMeta.ConnID, len(data), err)
				}
				return err
			})
			if err != nil {
				log.Printf("发送初始化失败 (connID: %s, 数据长度: %d): %v", connMeta.ConnID, len(data), err)
			}
		}
	}
}

// Send: 业务层发送接口（非阻塞，原子状态判断）
func (c *TcpClient) Send(msg proto.Message) error {
	// 1. 快速状态校验（原子操作，无锁）
	if c.closed.Load() {
		return errors.New("发送失败：客户端已关闭")
	}
	if !c.connected.Load() {
		return errors.New("发送失败：未连接服务器")
	}

	// 2. 编码Protobuf消息
	data, err := c.codec.Encode(msg)
	if err != nil {
		return errors.Join(errors.New("消息编码失败"), err)
	}

	// 3. 非阻塞投递到发送通道（避免阻塞业务层）
	select {
	case c.outgoing <- data:
		return nil
	default:
		return errors.New("发送失败：发送缓冲区已满（业务层发送过快）")
	}
}

// Recv: 业务层接收接口（阻塞，支持优雅退出）
func (c *TcpClient) Recv() (proto.Message, error) {
	select {
	case msg, ok := <-c.incoming:
		// 接收通道关闭（Close函数触发）
		if !ok {
			return nil, errors.New("接收失败：客户端已关闭")
		}
		return msg, nil

	case <-c.ctx.Done():
		// 上下文关闭（主动退出信号）
		return nil, errors.New("接收失败：客户端退出中")
	}
}

// Close: 优雅关闭（原子控制，无冗余清理）
func (c *TcpClient) Close() {
	// 1. 原子判断：确保只关闭一次（避免重复调用）
	if !c.closed.CompareAndSwap(false, true) {
		log.Println("客户端已关闭，跳过重复操作")
		return
	}
	log.Println("开始关闭客户端")

	// 2. 触发所有协程退出信号
	c.cancel()

	// 3. 关闭发送通道（通知asyncWriteLoop退出）
	close(c.outgoing)

	// 4. 关闭活跃连接（仅日志，不中断后续清理）
	if conn, ok := c.conn.Load().(gnet.Conn); ok && conn != nil {
		if err := conn.Close(); err != nil {
			log.Printf("连接关闭异常: %v", err)
		}
	}

	// 5. 等待所有协程退出（确保资源清理完成）
	c.wg.Wait()

	// 6. 关闭接收通道（通知业务层Recv退出）
	close(c.incoming)

	log.Println("客户端完全关闭")
}

// IsConnected: 连接状态查询（原子操作，无锁）
func (c *TcpClient) IsConnected() bool {
	return c.connected.Load()
}

// IsClosed: 检查是否已关闭（线程安全）
func (c *TcpClient) IsClosed() bool {
	return c.closed.Load()
}
