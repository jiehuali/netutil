package tcputil

import (
	"errors"
	"sync"
	"sync/atomic"
)

const (
	_GATEWAY_COMMAND_NONE_       = 0
	_GATEWAY_COMMAND_ADD_CLIENT_ = 1
	_GATEWAY_COMMAND_DEL_CLIENT_ = 2
	_GATEWAY_COMMAND_BROADCAST_  = 3
	_GATEWAY_MAX_CLIENT_ID_      = 0x00FFFFFF
)

type GatewayLink struct {
	owner          *GatewayFrontend
	linkId         uint32
	backendId      uint32
	net            string
	addr           string
	maxClientNum   int
	takeClientAddr bool
	conn           *Conn
	clients        []*GatewayClient
	clientsMutex   sync.RWMutex
	maxClientId    int
	closed         int32
}

func newGatewayLink(owner *GatewayFrontend, backend *GatewayBackendInfo, pack, maxPackSize int) (*GatewayLink, error) {
	var (
		this      *GatewayLink
		conn      *Conn
		err       error
		linkIdMsg []byte
		linkId    uint32
	)

	if conn, err = Connect(backend.Net, backend.Addr, pack, 0, maxPackSize); err != nil {
		return nil, err
	}

	if linkIdMsg = conn.Read(); linkIdMsg == nil {
		return nil, errors.New("wait link id failed")
	}

	linkId = getUint32(linkIdMsg)

	this = &GatewayLink{
		owner:          owner,
		linkId:         linkId,
		backendId:      backend.Id,
		net:            backend.Net,
		addr:           backend.Addr,
		maxClientNum:   backend.MaxClientNum,
		takeClientAddr: backend.TakeClientAddr,
		conn:           conn,
		clients:        make([]*GatewayClient, backend.MaxClientNum),
	}

	go func() {
		defer this.Close(true)

		for {
			var msg = this.conn.ReadPackage()

			if msg == nil {
				break
			}

			switch msg.ReadUint8() {
			case _GATEWAY_COMMAND_NONE_:
				var clientId = msg.ReadUint32()

				if client := this.GetClient(clientId); client != nil {
					if this.owner.counterOn {
						atomic.AddUint64(&this.owner.outPack, uint64(1))
						atomic.AddUint64(&this.owner.outByte, uint64(len(msg.Data)))
					}

					client.Send(msg.Data)
				}
			case _GATEWAY_COMMAND_DEL_CLIENT_:
				var clientId = msg.ReadUint32()

				this.DelClient(clientId)
			case _GATEWAY_COMMAND_BROADCAST_:
				var (
					idNum       = int(msg.ReadUint16())
					realMsg     = msg.Data[4*idNum:]
					realSendNum = 0
				)

				for i := 0; i < idNum; i++ {
					var clientId = msg.ReadUint32()

					if client := this.GetClient(clientId); client != nil {
						realSendNum += 1

						client.Send(realMsg)
					}
				}

				if this.owner.counterOn {
					var bytes = uint64(realSendNum * len(realMsg))
					atomic.AddUint64(&this.owner.outPack, uint64(realSendNum))
					atomic.AddUint64(&this.owner.outByte, bytes)
					atomic.AddUint64(&this.owner.broPack, uint64(1))
					atomic.AddUint64(&this.owner.broByte, bytes)
				}
			}
		}
	}()

	return this, nil
}

func (this *GatewayLink) AddClient(conn *Conn) *GatewayClient {
	this.clientsMutex.Lock()
	defer this.clientsMutex.Unlock()

	this.maxClientId += 1

	if this.maxClientId == this.maxClientNum {
		this.maxClientId = 0
	}

	var clientId = -1

	if this.clients[this.maxClientId] == nil {
		clientId = int(this.maxClientId)
	} else {
		for i := 0; i < len(this.clients); i++ {
			if this.clients[i] != nil {
				clientId = i
				break
			}
		}
	}

	if clientId >= 0 {
		var client = newGatewayClient(this, this.linkId+uint32(clientId), conn)

		this.clients[clientId] = client

		return client
	}

	return nil
}

func (this *GatewayLink) DelClient(clientId uint32) {
	this.clientsMutex.Lock()
	defer this.clientsMutex.Unlock()

	var index = int(clientId - this.linkId)

	if index >= this.maxClientNum || index < 0 {
		return
	}

	var client = this.clients[index]

	this.clients[index] = nil

	if client != nil {
		client.Close(true)
	}
}

func (this *GatewayLink) GetClient(clientId uint32) *GatewayClient {
	this.clientsMutex.RLock()
	defer this.clientsMutex.RUnlock()

	var index = int(clientId - this.linkId)

	if index >= this.maxClientNum || index < 0 {
		return nil
	}

	return this.clients[index]
}

func (this *GatewayLink) SendDelClient(clientId uint32) {
	this.conn.NewPackage(4).WriteUint32(clientId).Send()
}

func (this *GatewayLink) SendToBackend(msg []byte) error {
	return this.conn.sendRaw(msg)
}

func (this *GatewayLink) Close(bySelf bool) {
	if atomic.CompareAndSwapInt32(&this.closed, 0, 1) {
		this.conn.Close()

		for _, client := range this.clients {
			if client != nil {
				client.conn.Close()
			}
		}

		// 如果是自己关闭的，需要从列表移除
		if bySelf {
			this.owner.delLink(this.backendId)
		}
	}
}

type GatewayClient struct {
	owner     *GatewayLink
	id        uint32
	conn      *Conn
	sendChan  chan []byte
	closeChan chan int
	closed    int32
}

func newGatewayClient(owner *GatewayLink, id uint32, conn *Conn) *GatewayClient {
	if owner.takeClientAddr {
		var addr = conn.conn.RemoteAddr().String()
		var addrMsg = conn.NewPackage(4 + 2 + len(addr))

		addrMsg.WriteUint32(id).WriteUint8(uint8(len(addr))).WriteBytes([]byte(addr))

		owner.SendToBackend(addrMsg.buff)
	}

	var this = &GatewayClient{
		owner,
		id,
		conn,
		make(chan []byte, 1000),
		make(chan int, 2),
		0,
	}

	go func() {
		defer this.Close(false)
	L:
		for {
			select {
			case data := <-this.sendChan:
				if conn.sendRaw(data) != nil {
					break L
				}
			case <-this.closeChan:
				break L
			}
		}
	}()

	return this
}

func (this *GatewayClient) Transport() {
	defer this.Close(false)

	var (
		link  = this.owner
		front = link.owner
		pack  = front.pack
		msg   []byte
	)

	for {
		msg = this.conn.ReadInto(msg)

		if msg == nil {
			break
		}

		setUint(msg, pack, len(msg)-pack)

		setUint32(msg[pack:], this.id)

		if front.counterOn {
			atomic.AddUint64(&front.inPack, uint64(1))
			atomic.AddUint64(&front.inByte, uint64(len(msg)))
		}

		link.SendToBackend(msg)
	}
}

func (this *GatewayClient) Send(data []byte) {
	select {
	case this.sendChan <- data:
	default:
		this.Close(false)
	}
}

func (this *GatewayClient) Close(byBackend bool) {
	if atomic.CompareAndSwapInt32(&this.closed, 0, 1) {
		// 如果不是后端关闭，需要从列表移除
		if byBackend == false {
			this.owner.DelClient(this.id)
			this.owner.SendDelClient(this.id)
		}

		this.conn.Close()
		this.closeChan <- 1
	}
}
