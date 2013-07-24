package netutil

import (
	"sync"
	"sync/atomic"
	"time"
)

//
// 网关前端
//
type GatewayFrontend struct {
	server      *Listener
	pack        int
	maxPackSize int
	links       map[uint32]*GatewayLink
	linksMutex  sync.RWMutex
	counterOn   bool
	inPack      uint64
	inByte      uint64
	outPack     uint64
	outByte     uint64
	broPack     uint64
	broByte     uint64
}

//
// 网关后端信息
//
type GatewayBackendInfo struct {
	Id             uint32 // 后端ID
	Net            string // 连接类型
	Addr           string // 地址
	MaxClientNum   int    // 最大连接数
	TakeClientAddr bool   // 是否在客户端首次连接时发送IP
}

//
// 网关刷新结果
//
type GatewayUpdateResult struct {
	Id    uint32 // 后端ID
	IsNew bool   // 是否是新连接
	Net   string // 连接类型
	Addr  string // 后端地址，如果是旧连接对应被关闭的连接地址
	Error error  // 错误信息，只在创建新连接时产生
}

//
// 在指定地址和端口创建一个网关前端，连接到指定的网关后端，并等待客户端接入。
// 新接入的客户端首先需要发送一个uint32类型的后端ID，选择客户端实际所要连接的后端。
//
func NewGatewayFrontend(net, addr string, pack, maxPackSize int, backends []*GatewayBackendInfo) (*GatewayFrontend, error) {
	server, err := Listen(net, addr, pack, pack+4, maxPackSize)

	if err != nil {
		return nil, err
	}

	var this = &GatewayFrontend{
		server:      server,
		pack:        pack,
		maxPackSize: maxPackSize,
		links:       make(map[uint32]*GatewayLink),
	}

	this.UpdateBackends(backends)

	go func() {
		for {
			var conn = this.server.Accpet()

			if conn == nil {
				break
			}

			go func() {
				defer func() {
					conn.Close()
				}()

				var client = this.clientInit(conn)

				if client == nil {
					return
				}

				client.Transport()
			}()
		}
	}()

	return this, nil
}

func (this *GatewayFrontend) clientInit(conn *Conn) *GatewayClient {
	var (
		serverIdMsg []byte
		serverId    uint32
	)

	if serverIdMsg = conn.Read(); len(serverIdMsg) != this.pack+4+4 {
		return nil
	}

	serverId = getUint32(serverIdMsg[this.pack+4:])

	if link := this.getLink(serverId); link != nil {
		return link.AddClient(conn)
	}

	return nil
}

func (this *GatewayFrontend) addLink(id uint32, link *GatewayLink) {
	this.linksMutex.Lock()
	defer this.linksMutex.Unlock()

	this.links[id] = link
}

func (this *GatewayFrontend) delLink(id uint32) {
	this.linksMutex.Lock()
	defer this.linksMutex.Unlock()

	delete(this.links, id)
}

func (this *GatewayFrontend) getLink(id uint32) *GatewayLink {
	this.linksMutex.RLock()
	defer this.linksMutex.RUnlock()

	if link, exists := this.links[id]; exists {
		return link
	}

	return nil
}

func (this *GatewayFrontend) removeOldLinks(backends []*GatewayBackendInfo) []*GatewayUpdateResult {
	this.linksMutex.Lock()
	defer this.linksMutex.Unlock()

	var results = make([]*GatewayUpdateResult, 0, len(backends))

	for id, link := range this.links {
		var needClose = true

		for _, backend := range backends {
			if id == backend.Id && link.addr == backend.Addr {
				needClose = false
				break
			}
		}

		if needClose {
			link.Close(false)
			results = append(results, &GatewayUpdateResult{id, false, link.net, link.addr, nil})
		}
	}

	return results
}

//
// 更新网关后端，移除地址有变化或者已经不在新配置里的久连接，创建久配置中没有的连接。
//
func (this *GatewayFrontend) UpdateBackends(backends []*GatewayBackendInfo) []*GatewayUpdateResult {
	var results = this.removeOldLinks(backends)

	for _, backend := range backends {
		if this.getLink(backend.Id) != nil {
			continue
		}

		var link, err = newGatewayLink(this, backend, this.pack, this.maxPackSize)

		if link != nil {
			this.addLink(backend.Id, link)
		}

		results = append(results, &GatewayUpdateResult{backend.Id, true, backend.Net, backend.Addr, err})
	}

	return results
}

//
// 你懂的。
//
func (this *GatewayFrontend) Close() {
	this.linksMutex.Lock()
	defer this.linksMutex.Unlock()

	this.server.Close()

	for _, link := range this.links {
		link.Close(false)
	}
}

//
// 开启或关闭计数器
//
func (this *GatewayFrontend) SetCounter(on bool) {
	this.counterOn = on
}

//
// 获取计数器
//
func (this *GatewayFrontend) GetCounter() (inPack, inByte, outPack, outByte, broPack, broByte uint64) {
	inPack = atomic.LoadUint64(&this.inPack)
	inByte = atomic.LoadUint64(&this.inByte)
	outPack = atomic.LoadUint64(&this.outPack)
	outByte = atomic.LoadUint64(&this.outByte)
	broPack = atomic.LoadUint64(&this.broPack)
	broByte = atomic.LoadUint64(&this.broByte)

	return
}

func (this *GatewayFrontend) GetCounterPerSecond() (inPack, inByte, outPack, outByte, broPack, broByte uint64) {
	inPack1 := atomic.LoadUint64(&this.inPack)
	inByte1 := atomic.LoadUint64(&this.inByte)
	outPack1 := atomic.LoadUint64(&this.outPack)
	outByte1 := atomic.LoadUint64(&this.outByte)
	broPack1 := atomic.LoadUint64(&this.broPack)
	broByte1 := atomic.LoadUint64(&this.broByte)

	time.Sleep(time.Second)

	inPack2 := atomic.LoadUint64(&this.inPack)
	inByte2 := atomic.LoadUint64(&this.inByte)
	outPack2 := atomic.LoadUint64(&this.outPack)
	outByte2 := atomic.LoadUint64(&this.outByte)
	broPack2 := atomic.LoadUint64(&this.broPack)
	broByte2 := atomic.LoadUint64(&this.broByte)

	inPack = inPack2 - inPack1
	inByte = inByte2 - inByte1
	outPack = outPack2 - outPack1
	outByte = outByte2 - outByte1
	broPack = broPack2 - broPack1
	broByte = broByte2 - broByte1

	return
}
