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

type tcpGatewayLink struct {
	owner          *TcpGatewayFrontend
	id             uint32
	addr           string
	pack           int
	conn           *TcpConn
	clients        map[uint32]*tcpGatewayClient
	clientsMutex   sync.RWMutex
	maxClientId    uint32
	takeClientAddr bool
	closed         int32
}

func newTcpGatewayLink(owner *TcpGatewayFrontend, backend *TcpGatewayBackendInfo, pack, maxPackSize int) (*tcpGatewayLink, error) {
	var (
		this             *tcpGatewayLink
		conn             *TcpConn
		err              error
		beginClientIdMsg []byte
		beginClientId    uint32
	)

	if conn, err = Connect(backend.Addr, pack, 0, maxPackSize); err != nil {
		return nil, err
	}

	if beginClientIdMsg = conn.Read(); beginClientIdMsg == nil {
		return nil, errors.New("wait link id failed")
	}

	beginClientId = getUint32(beginClientIdMsg)

	this = &tcpGatewayLink{
		owner:          owner,
		id:             backend.Id,
		addr:           backend.Addr,
		pack:           pack,
		conn:           conn,
		clients:        make(map[uint32]*tcpGatewayClient),
		maxClientId:    beginClientId,
		takeClientAddr: backend.TakeClientAddr,
	}

	this.conn.SetNoDelay(false)

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

				if client := this.GetClient(clientId); client != nil {
					client.Close()
				}
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

func (this *tcpGatewayLink) AddClient(conn *TcpConn) *tcpGatewayClient {
	this.clientsMutex.Lock()
	defer this.clientsMutex.Unlock()

	this.maxClientId += 1

	if this.maxClientId == _GATEWAY_MAX_CLIENT_ID_ {
		this.maxClientId = 0
	}

	var client = newTcpGatewayClient(this, this.maxClientId, conn)

	this.clients[this.maxClientId] = client

	return client
}

func (this *tcpGatewayLink) DelClient(clientId uint32) {
	this.clientsMutex.Lock()
	defer this.clientsMutex.Unlock()

	delete(this.clients, clientId)
}

func (this *tcpGatewayLink) GetClient(clientId uint32) *tcpGatewayClient {
	this.clientsMutex.RLock()
	defer this.clientsMutex.RUnlock()

	if client, exists := this.clients[clientId]; exists {
		return client
	}

	return nil
}

func (this *tcpGatewayLink) SendDelClient(clientId uint32) {
	this.conn.NewPackage(4).WriteUint32(clientId).Send()
}

func (this *tcpGatewayLink) SendToBackend(msg []byte) error {
	return this.conn.sendRaw(msg)
}

func (this *tcpGatewayLink) Close(removeFromFrontend bool) {
	if atomic.CompareAndSwapInt32(&this.closed, 0, 1) {
		this.conn.Close()

		for _, client := range this.clients {
			client.conn.Close()
		}

		if removeFromFrontend {
			this.owner.delLink(this.id)
		}
	}
}

type tcpGatewayClient struct {
	owner     *tcpGatewayLink
	id        uint32
	conn      *TcpConn
	sendChan  chan []byte
	closeChan chan int
	closed    int32
}

func newTcpGatewayClient(owner *tcpGatewayLink, id uint32, conn *TcpConn) *tcpGatewayClient {
	var this = &tcpGatewayClient{
		owner,
		id,
		conn,
		make(chan []byte, 1000),
		make(chan int, 2),
		0,
	}

	this.conn.SetNoDelay(false)

	go func() {
		defer this.Close()
	L:
		for {
			select {
			case data := <-this.sendChan:
				if this.conn.sendRaw(data) != nil {
					break L
				}
			case <-this.closeChan:
				break L
			}
		}
	}()

	return this
}

func (this *tcpGatewayClient) Send(data []byte) {
	select {
	case this.sendChan <- data:
	default:
		this.Close()
	}
}

func (this *tcpGatewayClient) Close() {
	if atomic.CompareAndSwapInt32(&this.closed, 0, 1) {
		go func() {
			this.owner.DelClient(this.id)
			this.owner.SendDelClient(this.id)
			this.conn.Close()
			this.closeChan <- 1
		}()
	}
}
