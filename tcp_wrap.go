package tcputil

import (
	"errors"
	"io"
	"net"
)

//
// 面向包协议的监听器，Accept时返回面向包协议的连接实例。
//
type TcpListener struct {
	pack        int
	padding     int
	maxPackSize int
	listener    *net.TCPListener
}

//
// 基于现有的监听器包装一个面向包协议的监听器。
// 参数'pack'用于设置消息包的头部长度，消息包长度就存放在其中，所以请根据消息包的最大长度可能性设置此参数，'pack'必须是1, 2, 4或者8。
// 参数'padding'通常只要设置成0，这个参数用于优化网关通讯，避免不必要的内存分配和数据复制，请参考'Read'方法。
// 参数'maxPackSize'用于限制包的大小。
//
func NewTcpListener(listener *net.TCPListener, pack, padding, maxPackSize int) (*TcpListener, error) {
	if pack != 1 && pack != 2 && pack != 4 && pack != 8 {
		return nil, errors.New("pack != 1 && pack != 2 && pack != 4 && pack != 8")
	}

	return &TcpListener{
		pack:        pack,
		padding:     padding,
		maxPackSize: maxPackSize,
		listener:    listener,
	}, nil
}

//
// 监听指定地址和端口并返回一个基于消息包的监听器，参数说明参考‘NewTcpListener'。
//
func Listen(addr string, pack, padding, maxPackSize int) (*TcpListener, error) {
	var (
		err      error
		listener net.Listener
	)

	if listener, err = net.Listen("tcp", addr); err != nil {
		return nil, err
	}

	return NewTcpListener(listener.(*net.TCPListener), pack, padding, maxPackSize)
}

//
// 你懂的。
//
func (this *TcpListener) Close() error {
	return this.listener.Close()
}

//
// 等待一个新进连接，调用会一直阻塞，直到新的连接进入或者监听器关闭。
//
func (this *TcpListener) Accpet() *TcpConn {
	var conn, err1 = this.listener.AcceptTCP()

	if err1 != nil {
		return nil
	}

	var tcpConn, err2 = NewTcpConn(conn, this.pack, this.padding, this.maxPackSize)

	if err2 != nil {
		return nil
	}

	return tcpConn
}

//
// 面向包协议的网络连接。
//
type TcpConn struct {
	conn        *net.TCPConn
	pack        int
	padding     int
	maxPackSize int
	head        []byte
}

//
// 从现有网络连接包装一个面向包协议的网络连接，参数说明参考‘NewTcpListener'。
//
func NewTcpConn(conn *net.TCPConn, pack, padding, maxPackSize int) (*TcpConn, error) {
	if pack != 1 && pack != 2 && pack != 4 && pack != 8 {
		return nil, errors.New("pack != 1 && pack != 2 && pack != 4 && pack != 8")
	}

	return &TcpConn{
		conn:        conn,
		pack:        pack,
		padding:     padding,
		maxPackSize: maxPackSize,
		head:        make([]byte, pack),
	}, nil
}

//
// 连接目标地址，并返回一个面向包协议的连接，参数说明参考'NewTcpListener'。
//
func Connect(addr string, pack, padding, maxPackSize int) (*TcpConn, error) {
	var conn, err2 = net.Dial("tcp", addr)

	if err2 != nil {
		return nil, err2
	}

	return NewTcpConn(conn.(*net.TCPConn), pack, padding, maxPackSize)
}

//
// 连接网关，参考'Connect'。
//
func ConnectGateway(addr string, pack, padding, maxPackSize int, backendId uint32) (*TcpConn, error) {
	var conn, err1 = net.Dial("tcp", addr)

	if err1 != nil {
		return nil, err1
	}

	var tcpConn, err2 = NewTcpConn(conn.(*net.TCPConn), pack, padding, maxPackSize)

	if err2 != nil {
		return nil, err2
	}

	if err3 := tcpConn.NewPackage(4).WriteUint32(1).Send(); err3 != nil {
		tcpConn.Close()
		return nil, err3
	}

	return tcpConn, nil
}

//
// 你懂的。
//
func (this *TcpConn) Close() error {
	return this.conn.Close()
}

//
// 读取一个消息包，调用会一直阻塞，直到收到完整消息包或者连接断开。
//
func (this *TcpConn) Read() []byte {
	if _, err := io.ReadFull(this.conn, this.head); err != nil {
		return nil
	}

	var size = getUint(this.head, this.pack)

	if size > this.maxPackSize {
		return nil
	}

	var buff = make([]byte, this.padding+size)

	if buff == nil {
		return nil
	}

	// 不等待空消息
	if msg := buff[this.padding:]; len(msg) != 0 {
		if _, err := io.ReadFull(this.conn, msg); err != nil {
			return nil
		}
	}

	return buff
}

//
// 读取一个消息包，跟'Read'不同之处是返回的数据类型不一样。
//
func (this *TcpConn) ReadPackage() *TcpInput {
	var data = this.Read()

	if data != nil {
		return NewTcpInput(data)
	}

	return nil
}

//
// 创建一个用于发送的消息包，消息包内容填充完毕，请调用包实例的'Send'方法发送
//
func (this *TcpConn) NewPackage(size int) *TcpOutput {
	if size > this.maxPackSize {
		return nil
	}

	var buff = make([]byte, this.pack+size)

	if buff == nil {
		return nil
	}

	setUint(buff, this.pack, size)

	return &TcpOutput{this, buff, buff[this.pack:]}
}

func (this *TcpConn) sendRaw(msg []byte) error {
	_, err := this.conn.Write(msg)
	return err
}

func (this *TcpConn) SetNoDelay(noDelay bool) error {
	return this.conn.SetNoDelay(noDelay)
}
