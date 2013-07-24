package tcputil

import (
	"sync"
	"sync/atomic"
)

const (
	_GATEWAY_MAX_LINKS_ = 256
)

//
// 网关后端
//
type GatewayBackend struct {
	server     *Listener
	links      []*Conn
	linksMutex sync.RWMutex
	counterOn  bool
	inPack     uint64
	inByte     uint64
	outPack    uint64
	outByte    uint64
}

//
// 来自实际客户端的消息，用法跟Input是一样的，区别是多了ClientId
//
type GatewayIntput struct {
	LinkId   int
	ClientId uint32
	*Input
}

type GatewayOutput struct {
	owner    *GatewayBackend
	ClientId uint32
	*Output
}

//
// 在指定的地址和端口创建一个网关后端，等待网关前端连接。
// 一个网关后端可以被多个网关前端连接，客户端ID分配算法会保证不同网关前端的客户端ID不冲突。
//
func NewGatewayBackend(net, addr string, pack, maxPackSize int, messageHeandler func(msg *GatewayIntput)) (*GatewayBackend, error) {
	var server, err = Listen(net, addr, pack, 0, maxPackSize)

	if err != nil {
		return nil, err
	}

	var this = &GatewayBackend{
		server: server,
		links:  make([]*Conn, _GATEWAY_MAX_LINKS_),
	}

	go func() {
		for {
			var link = this.server.Accpet()

			if link == nil {
				break
			}

			go func() {
				defer func() {
					link.Close()
				}()

				var linkId = this.addLink(link)

				if linkId < 0 {
					return
				}

				defer func() {
					this.delLink(linkId)
				}()

				for {
					var msg = link.ReadPackage()

					if msg == nil {
						break
					}

					if this.counterOn {
						atomic.AddUint64(&this.inPack, uint64(1))
						atomic.AddUint64(&this.inByte, uint64(len(msg.Data)))
					}

					messageHeandler(&GatewayIntput{linkId, msg.ReadUint32(), msg})
				}

				messageHeandler(&GatewayIntput{linkId, 0, nil})
			}()
		}
	}()

	return this, nil
}

func (this *GatewayBackend) addLink(link *Conn) int {
	this.linksMutex.Lock()
	defer this.linksMutex.Unlock()

	for id := 1; id < len(this.links); id++ {
		if this.links[id] == nil {
			var serverIdMsg = link.NewPackage(4)

			serverIdMsg.WriteUint32(uint32(id) << 24)

			if err := serverIdMsg.Send(); err != nil {
				return -1
			}

			this.links[id] = link

			return id
		}
	}

	return -1
}

func (this *GatewayBackend) delLink(linkId int) {
	this.linksMutex.Lock()
	defer this.linksMutex.Unlock()

	this.links[linkId] = nil
}

func (this *GatewayBackend) getLink(clientId uint32) *Conn {
	this.linksMutex.RLock()
	defer this.linksMutex.RUnlock()

	return this.links[int(clientId>>24)]
}

//
// 你懂的。
//
func (this *GatewayBackend) Close() {
	this.linksMutex.Lock()
	defer this.linksMutex.Unlock()

	this.server.Close()

	for _, link := range this.links {
		if link != nil {
			link.Close()
		}
	}
}

//
// 创建一个发送给指定客户端的消息包
//
func (this *GatewayBackend) NewPackage(clientId uint32, size int) *GatewayOutput {
	var link = this.getLink(clientId)

	if link == nil {
		return nil
	}

	// [gateway command](1) + [client id](4) + [real package size](pack) + [real package content](return)
	var output = link.NewPackage(1+4+link.pack+size).WriteUint8(_GATEWAY_COMMAND_NONE_).WriteUint32(clientId).WriteUint(link.pack, uint64(size))

	return &GatewayOutput{this, clientId, output}
}

//
// 告诉网关前端，移除一个客户端
//
func (this *GatewayBackend) DelClient(clientId uint32) {
	var link = this.getLink(clientId)

	if link == nil {
		return
	}

	// [gateway command](1) + [client id](4)
	var output = link.NewPackage(1 + 4).WriteUint8(_GATEWAY_COMMAND_DEL_CLIENT_).WriteUint32(clientId)

	if this.counterOn {
		atomic.AddUint64(&this.outPack, uint64(1))
		atomic.AddUint64(&this.outByte, uint64(len(output.buff)))
	}

	output.Send()
}

//
// 创建一个发送给指定客户端的广播包，广播包的用法跟'Output'包一样
//
func (this *GatewayBackend) NewBroadcast(clientIds []uint32, size int) *Broadcast {
	var link *Conn

	// find first usable link
	for i := 0; i < len(clientIds); i++ {
		link = this.getLink(clientIds[i])

		if link != nil {
			break
		}
	}

	if link == nil {
		return nil
	}

	// [gateway command](1) + [client id list length](2) + [client id list](4 x len) + [real pack size](pack) + [real package content](return)
	var (
		output = link.NewPackage(1 + 2 + 4*len(clientIds) + link.pack + size)
		idNum  = len(clientIds)
	)

	output.WriteUint8(_GATEWAY_COMMAND_BROADCAST_).WriteUint16(uint16(idNum))

	for i := 0; i < idNum; i++ {
		output.WriteUint32(clientIds[i])
	}

	output.WriteUint(link.pack, uint64(size))

	return &Broadcast{this, idNum, output}
}

//
// 开启或关闭计数器
//
func (this *GatewayBackend) SetCounter(on bool) {
	this.counterOn = on
}

//
// 获取计数器
//
func (this *GatewayBackend) GetCounter() (inPack, inByte, outPack, outByte uint64) {
	inPack = atomic.LoadUint64(&this.inPack)
	inByte = atomic.LoadUint64(&this.inByte)
	outPack = atomic.LoadUint64(&this.outPack)
	outByte = atomic.LoadUint64(&this.outByte)
	return
}

//
// 重载Output的发送，统计发包数量
//
func (this *GatewayOutput) Send() error {
	if this.owner.counterOn {
		atomic.AddUint64(&this.owner.outPack, uint64(1))
		atomic.AddUint64(&this.owner.outByte, uint64(len(this.Output.buff)))
	}

	return this.Output.Send()
}

//
// 网关广播实例
//
type Broadcast struct {
	owner *GatewayBackend
	idNum int
	*Output
}

func (this *Broadcast) Send() error {
	this.owner.linksMutex.RLock()
	defer this.owner.linksMutex.RUnlock()

	var err error

	for _, link := range this.owner.links {
		if link != nil {
			this.Output.owner = link

			err = this.Output.Send()

			if this.owner.counterOn {
				atomic.AddUint64(&this.owner.outPack, uint64(1))
				atomic.AddUint64(&this.owner.outByte, uint64(len(this.Output.buff)))
			}
		}
	}

	return err
}
