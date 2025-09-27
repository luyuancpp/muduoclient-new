package muduo

import (
	"net"
	"time"

	"github.com/panjf2000/gnet/v2"
)

// nilConn 是一个空连接占位符，实现了 gnet.Conn 接口的所有方法
// 用于在连接关闭时替代 nil 存储到 atomic.Value 中，避免 panic
type nilConn struct{}

// ------------------------------ 实现 Reader 接口方法 ------------------------------

// Read 读取数据，空实现返回 EOF 表示连接已关闭
func (n *nilConn) Read(b []byte) (int, error) {
	return 0, net.ErrClosed
}

// ------------------------------ 实现 Writer 接口方法 ------------------------------

// Write 写入数据，空实现返回错误表示连接已关闭
func (n *nilConn) Write(b []byte) (int, error) {
	return 0, net.ErrClosed
}

// AsyncWrite 异步写入数据，空实现返回错误表示连接已关闭
func (n *nilConn) AsyncWrite(b []byte, callback gnet.AsyncCallback) error {
	return net.ErrClosed
}

// ------------------------------ 实现 Socket 接口方法 ------------------------------

// SetReadBuffer 设置读缓冲区大小，空实现返回错误
func (n *nilConn) SetReadBuffer(bytes int) error {
	return net.ErrClosed
}

// SetWriteBuffer 设置写缓冲区大小，空实现返回错误
func (n *nilConn) SetWriteBuffer(bytes int) error {
	return net.ErrClosed
}

// ------------------------------ 实现 Conn 接口其他方法 ------------------------------

// Context 返回用户定义的上下文，空实现返回 nil
func (n *nilConn) Context() any {
	return nil
}

// EventLoop 返回连接所属的事件循环，空实现返回 nil
func (n *nilConn) EventLoop() gnet.EventLoop {
	return nil
}

// SetContext 设置用户定义的上下文，空实现不做任何操作
func (n *nilConn) SetContext(ctx any) {}

// LocalAddr 返回本地地址，空实现返回 nil
func (n *nilConn) LocalAddr() net.Addr {
	return nil
}

// RemoteAddr 返回远程地址，空实现返回 nil
func (n *nilConn) RemoteAddr() net.Addr {
	return nil
}

// Wake 触发当前连接的 OnTraffic 事件，空实现返回错误
func (n *nilConn) Wake(callback gnet.AsyncCallback) error {
	return net.ErrClosed
}

// CloseWithCallback 带回调关闭连接，空实现返回错误
func (n *nilConn) CloseWithCallback(callback gnet.AsyncCallback) error {
	return net.ErrClosed
}

// Close 关闭连接，空实现返回错误
func (n *nilConn) Close() error {
	return net.ErrClosed
}

// SetDeadline 设置读写截止时间，空实现返回错误
func (n *nilConn) SetDeadline(t time.Time) error {
	return net.ErrClosed
}

// SetReadDeadline 设置读截止时间，空实现返回错误
func (n *nilConn) SetReadDeadline(t time.Time) error {
	return net.ErrClosed
}

// SetWriteDeadline 设置写截止时间，空实现返回错误
func (n *nilConn) SetWriteDeadline(t time.Time) error {
	return net.ErrClosed
}

// emptyConn 是全局单例的空连接占位符
// 用于在连接关闭时存储到 atomic.Value 中，替代 nil
var emptyConn = &nilConn{}
