package netutil

import (
	"errors"
	"io"
	"net"
)

//
// 面向包协议的监听器，Accept时返回面向包协议的连接实例。
//
type Listener struct {
	pack        int
	padding     int
	maxPackSize int
	listener    net.Listener
}

//
// 基于现有的监听器包装一个面向包协议的监听器。
// 参数'pack'用于设置消息包的头部长度，消息包长度就存放在其中，所以请根据消息包的最大长度可能性设置此参数，'pack'必须是1, 2, 4或者8。
// 参数'padding'通常只要设置成0，这个参数用于优化网关通讯，避免不必要的内存分配和数据复制，请参考'Read'方法。
// 参数'maxPackSize'用于限制包的大小。
//
func NewListener(listener net.Listener, pack, padding, maxPackSize int) (*Listener, error) {
	if pack != 1 && pack != 2 && pack != 4 && pack != 8 {
		return nil, errors.New("pack != 1 && pack != 2 && pack != 4 && pack != 8")
	}

	return &Listener{
		pack:        pack,
		padding:     padding,
		maxPackSize: maxPackSize,
		listener:    listener,
	}, nil
}

//
// 监听指定地址和端口并返回一个基于消息包的监听器，参数说明参考‘NewListener'。
//
func Listen(nettype, addr string, pack, padding, maxPackSize int) (*Listener, error) {
	var (
		err      error
		listener net.Listener
	)

	if listener, err = net.Listen(nettype, addr); err != nil {
		return nil, err
	}

	return NewListener(listener, pack, padding, maxPackSize)
}

//
// 你懂的。
//
func (this *Listener) Close() error {
	return this.listener.Close()
}

//
// 等待一个新进连接，调用会一直阻塞，直到新的连接进入或者监听器关闭。
//
func (this *Listener) Accpet() *Conn {
	var conn1, err1 = this.listener.Accept()

	if err1 != nil {
		return nil
	}

	var conn2, err2 = NewConn(conn1, this.pack, this.padding, this.maxPackSize)

	if err2 != nil {
		return nil
	}

	return conn2
}

//
// 面向包协议的网络连接。
//
type Conn struct {
	conn        net.Conn
	pack        int
	padding     int
	maxPackSize int
	head        []byte
}

//
// 从现有网络连接包装一个面向包协议的网络连接，参数说明参考‘NewListener'。
//
func NewConn(conn net.Conn, pack, padding, maxPackSize int) (*Conn, error) {
	if pack != 1 && pack != 2 && pack != 4 && pack != 8 {
		return nil, errors.New("pack != 1 && pack != 2 && pack != 4 && pack != 8")
	}

	return &Conn{
		conn:        conn,
		pack:        pack,
		padding:     padding,
		maxPackSize: maxPackSize,
		head:        make([]byte, pack),
	}, nil
}

//
// 连接目标地址，并返回一个面向包协议的连接，参数说明参考'NewListener'。
//
func Connect(nettype, addr string, pack, padding, maxPackSize int) (*Conn, error) {
	var conn, err = net.Dial(nettype, addr)

	if err != nil {
		return nil, err
	}

	return NewConn(conn, pack, padding, maxPackSize)
}

//
// 连接网关，参考'Connect'。
//
func ConnectGateway(nettype, addr string, pack, padding, maxPackSize int, backendId uint32) (*Conn, error) {
	var conn1, err1 = net.Dial(nettype, addr)

	if err1 != nil {
		return nil, err1
	}

	var conn2, err2 = NewConn(conn1, pack, padding, maxPackSize)

	if err2 != nil {
		return nil, err2
	}

	if err3 := conn2.NewPackage(4).WriteUint32(1).Send(); err3 != nil {
		conn2.Close()
		return nil, err3
	}

	return conn2, nil
}

//
// 你懂的。
//
func (this *Conn) Close() error {
	return this.conn.Close()
}

//
// 读取一个消息包，调用会一直阻塞，直到收到完整消息包或者连接断开。
//
func (this *Conn) Read() []byte {
	if _, err := io.ReadFull(this.conn, this.head); err != nil {
		return nil
	}

	var size = getUint(this.head, this.pack)

	if size > this.maxPackSize {
		return nil
	}

	var buff = make([]byte, this.padding+size)

	// 不等待空消息
	if msg := buff[this.padding:]; len(msg) != 0 {
		if _, err := io.ReadFull(this.conn, msg); err != nil {
			return nil
		}
	}

	return buff
}

//
// 读取一个消息包，调用会一直阻塞，直到收到完整消息包或者连接断开。
//
func (this *Conn) ReadInto(buff []byte) []byte {
	if _, err := io.ReadFull(this.conn, this.head); err != nil {
		return nil
	}

	var size = getUint(this.head, this.pack)

	if size > this.maxPackSize {
		return nil
	}

	var buffLen = this.padding + size

	if len(buff) < buffLen {
		buff = make([]byte, this.padding+size)
	} else {
		buff = buff[0:buffLen]
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
func (this *Conn) ReadPackage() *Input {
	var data = this.Read()

	if data != nil {
		return NewInput(data)
	}

	return nil
}

//
// 创建一个用于发送的消息包，消息包内容填充完毕，请调用包实例的'Send'方法发送
//
func (this *Conn) NewPackage(size int) *Output {
	if size > this.maxPackSize {
		return nil
	}

	var buff = make([]byte, this.pack+size)

	if buff == nil {
		return nil
	}

	setUint(buff, this.pack, size)

	return &Output{this, buff, buff[this.pack:]}
}

func (this *Conn) sendRaw(msg []byte) error {
	_, err := this.conn.Write(msg)
	return err
}
