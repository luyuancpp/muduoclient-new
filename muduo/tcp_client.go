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

// 补充全局错误定义（确保编解码器的 ErrInsufficientData 在此可见）
var (
	ErrInsufficientData = errors.New("insufficient data for packet")
)

// Codec: 编解码器接口（业务层实现，不变）
type Codec interface {
	Encode(msg proto.Message) ([]byte, error) // 编码Protobuf消息
	Decode(conn gnet.Conn) error              // 解码到ConnContext（需设置 connCtx.Msg）
}

// MessageCallback: 消息接收回调（业务层注册，接收完整消息后触发）
type MessageCallback func(msg proto.Message, connCtx *ConnContext)

// ------------------------------ 核心结构体 ------------------------------
// ConnMeta: 连接元数据（嵌入到ConnContext）
type ConnMeta struct {
	ConnID   string    // 唯一连接标识（UUID）
	CreateAt time.Time // 连接建立时间
}

// ConnContext: 连接上下文（绑定到gnet.Conn，仅存储元数据和解码结果）
type ConnContext struct {
	ConnMeta               // 嵌入元数据
	Msg      proto.Message // 解码后的完整消息（Decode成功后设置）
	// 核心优化：移除 cachedData 字段（编解码器已不依赖本地缓存）
}

// TcpClient: 精简TCP客户端（无chan，基于回调）
type TcpClient struct {
	codec       Codec              // 编解码器
	closed      atomic.Bool        // 关闭状态（原子变量）
	ctx         context.Context    // 生命周期上下文
	cancel      context.CancelFunc // 上下文取消函数
	conn        atomic.Value       // 活跃连接（gnet.Conn）
	wg          sync.WaitGroup     // 协程等待组（优雅关闭）
	client      *gnet.Client       // gnet客户端实例
	network     string             // 网络类型（固定tcp）
	addr        string             // 服务器地址（ip:port）
	multicore   bool               // gnet多核模式
	connected   atomic.Bool        // 连接状态（原子变量）
	msgCallback MessageCallback    // 消息回调（业务层注册）
}

// tcpClientEvents: gnet事件处理器（绑定TcpClient）
type tcpClientEvents struct {
	*gnet.BuiltinEventEngine // 嵌入默认事件实现
	client                   *TcpClient
}

// ------------------------------ gnet事件回调（同步优化） ------------------------------
// OnOpen: 连接建立时初始化上下文（移除cachedData初始化）
func (ev *tcpClientEvents) OnOpen(conn gnet.Conn) (out []byte, action gnet.Action) {
	tcpClient := ev.client
	if tcpClient.closed.Load() {
		return nil, gnet.Close
	}

	// 初始化连接上下文（仅元数据，无cachedData）
	connCtx := &ConnContext{
		ConnMeta: ConnMeta{
			ConnID:   uuid.NewString(),
			CreateAt: time.Now(),
		},
	}
	conn.SetContext(connCtx)

	// 原子更新连接状态
	tcpClient.conn.Store(conn)
	tcpClient.connected.Store(true)
	log.Printf("connected to server: %s (connID: %s)", tcpClient.addr, connCtx.ConnID)

	return out, gnet.None
}

// OnClose: 连接关闭时处理（重连/资源清理）
func (ev *tcpClientEvents) OnClose(conn gnet.Conn, err error) gnet.Action {
	tcpClient := ev.client
	connCtx, _ := conn.Context().(*ConnContext)
	connID := "unknown"
	if connCtx != nil {
		connID = connCtx.ConnID
	}

	// 更新连接状态（清空无效连接）
	tcpClient.connected.Store(false)
	tcpClient.conn.Store(nil)

	// 主动关闭：不重连
	if tcpClient.closed.Load() {
		log.Printf("conn closed: %s (active close)", connID)
		return gnet.Shutdown
	}

	// 服务器正常关闭：不重连
	if err == nil {
		log.Printf("conn closed: %s (server normal close)", connID)
		return gnet.None
	}

	// 异常断连：启动重连（提交到独立协程，避免阻塞gnet事件循环）
	log.Printf("conn closed abnormally: %s (err: %v) - starting reconnect", connID, err)
	go tcpClient.reconnectLoop()

	return gnet.None
}

// OnTraffic: 数据接收（解码成功直接触发回调，无chan中转）
func (ev *tcpClientEvents) OnTraffic(conn gnet.Conn) (action gnet.Action) {
	tcpClient := ev.client
	connCtx, ok := conn.Context().(*ConnContext)
	if !ok {
		// 双重保证：创建无cachedData的默认上下文
		connCtx = &ConnContext{}
		conn.SetContext(connCtx)
		return gnet.None
	}

	// 循环解码：处理所有可读的完整包
	for {
		// 调用编解码器解码（编解码器已基于gnet缓冲区，无本地缓存依赖）
		decodeErr := tcpClient.codec.Decode(conn)

		switch {
		case decodeErr == nil:
			// 解码成功且有消息：触发回调
			if connCtx.Msg != nil {
				if tcpClient.msgCallback != nil {
					// 注意：此处运行在gnet IO线程，回调不能耗时
					tcpClient.msgCallback(connCtx.Msg, connCtx)
				}
				connCtx.Msg = nil // 清空消息，准备下一次解码
			}
			continue // 继续解码下一个包

		case errors.Is(decodeErr, ErrInsufficientData):
			// 数据不足（半包）：退出循环，等待下一次数据到达
			return gnet.None

		default:
			// 解码错误（格式错误/校验失败等）：记录日志后退出
			log.Printf("decode error: %s (connID: %s, err: %v)", tcpClient.addr, connCtx.ConnID, decodeErr)
			return gnet.None
		}
	}
}

// OnTick: 定时回调（支持心跳，默认关闭）
func (ev *tcpClientEvents) OnTick() (delay time.Duration, action gnet.Action) {
	// 如需心跳，解开注释并配置间隔（示例10秒）
	// delay = 10 * time.Second
	// tcpClient := ev.client
	// if !tcpClient.connected.Load() {
	// 	return delay, gnet.None
	// }
	// conn, ok := tcpClient.conn.Load().(gnet.Conn)
	// if ok && conn != nil {
	// 	// 发送心跳消息（业务层替换为实际Protobuf）
	// 	heartbeat := &pb.Heartbeat{ConnID: conn.Context().(*ConnContext).ConnID}
	// 	_ = tcpClient.Send(heartbeat)
	// }
	return delay, gnet.None
}

// ------------------------------ 核心拨号方法（适配gnet原生接口） ------------------------------
// dial: 自定义拨号（使用gnet.DialContext，适配原生接口）
func (c *TcpClient) dial() (gnet.Conn, error) {
	// 5秒拨号超时（绑定客户端生命周期上下文）
	ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()

	// 调用gnet原生DialContext，建立TCP连接
	// 关键：连接建立后会触发OnOpen回调，自动绑定ConnContext
	return c.client.DialContext(c.network, c.addr, ctx)
}

// ------------------------------ 重连逻辑（修复） ------------------------------
// reconnectLoop: 退避重连（避免频繁重试）
func (c *TcpClient) reconnectLoop() {
	reconnectDelay := 3 * time.Second // 初始间隔
	maxDelay := 30 * time.Second      // 最大间隔
	retryCount := 0

	for {
		// 客户端已关闭：终止重连
		if c.closed.Load() {
			log.Println("reconnect stopped: client closed")
			return
		}

		// 退避等待
		log.Printf("reconnect attempt %d (delay: %v) - target: %s", retryCount+1, reconnectDelay, c.addr)
		time.Sleep(reconnectDelay)

		// 调用自定义dial()，通过gnet原生接口建立连接
		newConn, err := c.dial()
		if err == nil {
			// 连接建立成功：OnOpen已绑定上下文，直接更新原子变量
			log.Printf("reconnect success (connID: %s)", newConn.Context().(*ConnContext).ConnID)
			return
		}

		// 重连失败：更新退避间隔
		retryCount++
		reconnectDelay *= 2
		if reconnectDelay > maxDelay {
			reconnectDelay = maxDelay
		}
		log.Printf("reconnect failed (attempt %d): %v", retryCount, err)
	}
}

// ------------------------------ 客户端配置与初始化 ------------------------------
// ClientConfig: 客户端配置（精简）
type ClientConfig struct {
	Multicore bool // 是否启用gnet多核模式（单连接建议关闭）
}

// DefaultClientConfig: 默认配置（单连接最优）
func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{Multicore: false}
}

// NewTcpClient: 快速创建客户端（使用默认配置）
func NewTcpClient(addr string, codec Codec) *TcpClient {
	return NewTcpClientWithConfig(addr, codec, DefaultClientConfig())
}

// NewTcpClientWithConfig: 全配置创建客户端
func NewTcpClientWithConfig(addr string, codec Codec, conf *ClientConfig) *TcpClient {
	ctx, cancel := context.WithCancel(context.Background())
	client := &TcpClient{
		codec:     codec,
		ctx:       ctx,
		cancel:    cancel,
		network:   "tcp",
		addr:      addr,
		multicore: conf.Multicore,
	}

	// 启动gnet客户端协程
	client.wg.Add(1)
	go client.startGnetClient()

	return client
}

// startGnetClient: 启动gnet引擎（核心修复，适配gnet原生接口）
func (c *TcpClient) startGnetClient() {
	defer c.wg.Done()

	// 初始化gnet事件处理器
	gnetEvents := &tcpClientEvents{
		BuiltinEventEngine: &gnet.BuiltinEventEngine{},
		client:             c,
	}

	// 创建gnet客户端（优化网络参数）
	gnetClient, err := gnet.NewClient(
		gnetEvents,
		gnet.WithMulticore(c.multicore),
		gnet.WithTCPNoDelay(gnet.TCPNoDelay), // 禁用Nagle，减少延迟
		gnet.WithLockOSThread(true),          // 绑定线程，提升性能
		gnet.WithTicker(true),                // 启用定时回调（支持心跳）
	)
	if err != nil {
		log.Fatalf("failed to create gnet client: %v", err)
	}
	c.client = gnetClient

	// 延迟关闭gnet引擎
	defer func() {
		if err := c.client.Stop(); err != nil {
			log.Printf("failed to stop gnet client: %v", err)
		} else {
			log.Println("gnet client stopped normally")
		}
	}()

	// 启动gnet引擎（非阻塞）
	if err := c.client.Start(); err != nil {
		log.Fatalf("failed to start gnet engine: %v", err)
	}

	// 建立初始连接（调用自定义dial()，适配gnet原生接口）
	if _, err := c.dial(); err != nil {
		log.Fatalf("failed to establish initial connection: %v", err)
	}

	// 阻塞等待客户端关闭信号
	<-c.ctx.Done()
	log.Println("gnet client coroutine exited")
}

// ------------------------------ 业务层核心接口 ------------------------------
// SetMessageCallback: 注册消息回调（业务层调用）
func (c *TcpClient) SetMessageCallback(callback MessageCallback) {
	c.msgCallback = callback
}

// Send: 发送消息（非阻塞，线程安全）
func (c *TcpClient) Send(msg proto.Message) error {
	// 快速状态校验
	if c.closed.Load() {
		return errors.New("client closed")
	}
	if !c.connected.Load() {
		return errors.New("not connected to server")
	}

	// 编码消息
	data, err := c.codec.Encode(msg)
	if err != nil {
		return errors.Join(errors.New("encode message failed"), err)
	}

	// 原子获取活跃连接
	connVal := c.conn.Load()
	conn, ok := connVal.(gnet.Conn)
	if !ok || conn == nil {
		return errors.New("no active connection")
	}
	connCtx := conn.Context().(*ConnContext)

	// 异步发送（gnet非阻塞写）
	err = conn.AsyncWrite(data, func(_ gnet.Conn, writeErr error) error {
		if writeErr != nil {
			log.Printf("send failed (connID: %s, dataLen: %d): %v", connCtx.ConnID, len(data), writeErr)
		}
		return writeErr
	})
	if err != nil {
		return errors.Join(errors.New("failed to start async write"), err)
	}

	return nil
}

// Close: 优雅关闭客户端（线程安全）
func (c *TcpClient) Close() {
	// 确保只关闭一次
	if !c.closed.CompareAndSwap(false, true) {
		log.Println("client already closed")
		return
	}
	log.Println("starting to close client")

	// 触发所有协程退出
	c.cancel()

	// 关闭活跃连接（避免连接泄露）
	connVal := c.conn.Load()
	if connVal != nil {
		conn, ok := connVal.(gnet.Conn)
		if ok && conn != nil {
			conn.Close()
		}
	}

	// 等待gnet协程退出
	c.wg.Wait()

	// 更新连接状态
	c.connected.Store(false)
	log.Println("client closed completely")
}

// IsConnected: 查询连接状态（线程安全）
func (c *TcpClient) IsConnected() bool {
	return c.connected.Load()
}

// IsClosed: 查询客户端是否已关闭（线程安全）
func (c *TcpClient) IsClosed() bool {
	return c.closed.Load()
}

// Ctx: 暴露上下文（供业务层退出判断）
func (c *TcpClient) Ctx() context.Context {
	return c.ctx
}
