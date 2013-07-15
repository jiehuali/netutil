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
}

func newTcpGatewayLink(owner *TcpGatewayFrontend, backend *TcpGatewayBackendInfo, pack int, memPool MemPool) (*tcpGatewayLink, error) {
	var (
		this             *tcpGatewayLink
		conn             *TcpConn
		err              error
		beginClientIdMsg []byte
		beginClientId    uint32
	)

	if conn, err = Connect(backend.Addr, pack, 0, memPool); err != nil {
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

func (this *tcpGatewayLink) AddClient(client *TcpConn) uint32 {
	this.clientsMutex.Lock()
	defer this.clientsMutex.Unlock()

	this.maxClientId += 1

	this.clients[this.maxClientId] = newTcpGatewayClient(this, this.maxClientId, client)

	return this.maxClientId
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
	this.clientsMutex.Lock()
	defer this.clientsMutex.Unlock()

	this.conn.Close()

	for _, client := range this.clients {
		client.Close()
	}

	if removeFromFrontend {
		this.owner.delLink(this.id)
	}
}

type tcpGatewayClient struct {
	owner    *tcpGatewayLink
	clientId uint32
	conn     *TcpConn
	sendChan chan []byte
	closed   bool
}

func newTcpGatewayClient(owner *tcpGatewayLink, clientId uint32, conn *TcpConn) *tcpGatewayClient {
	var this = &tcpGatewayClient{
		owner,
		clientId,
		conn,
		make(chan []byte, 100),
		false,
	}

	go func() {
		defer func() {
			recover()

			if !this.closed {
				this.Close()
			}
		}()

		for {
			data, ok := <-this.sendChan

			if !ok {
				break
			}

			if this.conn.sendRaw(data) != nil {
				break
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
	this.conn.Close()
	go func() {
		this.owner.DelClient(this.clientId)
	}()
}
