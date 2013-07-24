package tcputil

import (
	"strings"
	"sync"
	"testing"
	"time"
)

//
// 测试基本通讯
//
func TestWrap(t *testing.T) {
	var wg sync.WaitGroup

	var server, err1 = Listen("tcp", "0.0.0.0:10086", 4, 0, 1024)

	if err1 != nil {
		t.Fatal(err1)
	}

	wg.Add(1)
	go func() {
		defer func() {
			server.Close()
			wg.Done()
		}()

		var client = server.Accpet()

		if client == nil {
			t.Fatal("could't accept")
		}

		defer func() {
			client.Close()
		}()

		if client.ReadPackage().ReadUint16() != 0xFFFF {
			t.Fatal("read message1 failed")
		}

		if client.ReadPackage().ReadUint32() != 0xFFFFFFFF {
			t.Fatal("read message1 failed")
		}

		if client.NewPackage(2).WriteUint16(0xFFFF).Send() != nil {
			t.Fatal("send message3 failed")
		}
	}()

	var client, err2 = Connect("tcp", "127.0.0.1:10086", 4, 0, 1024)

	if err2 != nil {
		t.Fatal(err2)
	}

	defer func() {
		client.Close()
	}()

	if client.NewPackage(2).WriteUint16(0xFFFF).Send() != nil {
		t.Fatal("send message1 failed")
	}

	if client.NewPackage(4).WriteUint32(0xFFFFFFFF).Send() != nil {
		t.Fatal("send message2 failed")
	}

	if client.ReadPackage().ReadUint16() != 0xFFFF {
		t.Fatal("read message3 failed")
	}

	wg.Wait()
}

//
// 测试消息头空间预留
//
func TestPadding(t *testing.T) {
	var wg sync.WaitGroup

	var server, err1 = Listen("tcp", "0.0.0.0:10086", 4, 2, 1024)

	if err1 != nil {
		t.Fatal(err1)
	}

	wg.Add(1)
	go func() {
		defer func() {
			server.Close()
			wg.Done()
		}()

		var client = server.Accpet()

		if client == nil {
			t.Fatal("could't accept")
		}

		defer func() {
			client.Close()
		}()

		if client.ReadPackage().Seek(2).ReadUint16() != 0xFFFF {
			t.Fatal("read message1 failed")
		}

		if client.NewPackage(2).WriteUint16(0xFFFF).Send() != nil {
			t.Fatal("send message2 failed")
		}
	}()

	var client, err2 = Connect("tcp", "127.0.0.1:10086", 4, 4, 1024)

	if err2 != nil {
		t.Fatal(err2)
	}

	defer func() {
		client.Close()
	}()

	if client.NewPackage(2).WriteUint16(0xFFFF).Send() != nil {
		t.Fatal("send message1 failed")
	}

	if client.ReadPackage().Seek(4).ReadUint16() != 0xFFFF {
		t.Fatal("read message2 failed")
	}

	wg.Wait()
}

//
// 测试网关
//
func TestGateway(t *testing.T) {
	var wg sync.WaitGroup

	var errChan = make(chan string)
	var msgChan = make(chan *GatewayIntput)

	var backend, err1 = NewGatewayBackend("tcp", "0.0.0.0:10010", 4, 1024, func(msg *GatewayIntput) {
		msgChan <- msg
	})

	var waitMsg = func(errMsg string) *GatewayIntput {
		select {
		case msg := <-msgChan:
			return msg
		case <-time.After(time.Second):
			errChan <- errMsg
		}
		return nil
	}

	var fatal = func(err string) {
		errChan <- err
	}

	if err1 != nil {
		t.Fatal(err1)
	}

	var closeWait = make(chan int)

	wg.Add(1)
	go func() {
		defer func() {
			backend.Close()
			wg.Done()
		}()

		var clientId1 uint32

		if message1 := waitMsg("wait message1"); message1.ReadUint32() != 1234 {
			fatal("read message1 failed")
		} else {
			clientId1 = message1.ClientId
		}

		if message2 := backend.NewPackage(clientId1, 4); message2.WriteUint32(1234).Send() != nil {
			fatal("send message2 failed")
		}

		var clientId2 uint32

		if message3 := waitMsg("wait message3"); message3.ReadUint32() != 4321 {
			fatal("read message3 failed")
		} else {
			clientId2 = message3.ClientId
		}

		if backend.NewPackage(clientId2, 4).WriteUint32(4321).Send() != nil {
			fatal("send message4 failed")
		}

		if backend.NewBroadcast([]uint32{clientId1, clientId2}, 4).WriteUint32(67890).Send() != nil {
			fatal("send broadcast failed")
		}

		if len(waitMsg("wait close1").Data) != 0 {
			fatal("close not match")
		}

		if len(waitMsg("wait close2").Data) != 0 {
			fatal("close not match")
		}

		closeWait <- 1
	}()

	var frontend, err2 = NewGatewayFrontend("tcp", "0.0.0.0:10086", 4, 1024, []*GatewayBackendInfo{
		{1, "tcp", "127.0.0.1:10010", 10, false},
	})

	if err2 != nil {
		t.Fatal(err2)
	}

	var client1, err3 = ConnectGateway("tcp", "0.0.0.0:10086", 4, 0, 1024, 1)

	if err3 != nil {
		t.Fatal(err3)
	}

	var client2, err4 = ConnectGateway("tcp", "0.0.0.0:10086", 4, 0, 1024, 1)

	if err4 != nil {
		t.Fatal(err4)
	}

	if client1.NewPackage(4).WriteUint32(1234).Send() != nil {
		t.Fatal("send message1 failed")
	}

	if client1.ReadPackage().ReadUint32() != 1234 {
		t.Fatal("read message2 failed")
	}

	if client2.NewPackage(4).WriteUint32(4321).Send() != nil {
		t.Fatal("send message3 failed")
	}

	if client2.ReadPackage().ReadUint32() != 4321 {
		t.Fatal("read message4 failed")
	}

	if client1.ReadPackage().ReadUint32() != 67890 {
		t.Fatal("read message5 failed")
	}

	if client2.ReadPackage().ReadUint32() != 67890 {
		t.Fatal("read message6 failed")
	}

	client1.Close()
	client2.Close()

	select {
	case <-closeWait:
	case err := <-errChan:
		t.Fatal(err)
	}

	frontend.Close()

	wg.Wait()
}

//
// 测试获取客户端IP
//
func TestTakeClientAddr(t *testing.T) {
	var wg sync.WaitGroup

	var msgChan = make(chan *GatewayIntput)
	var backend, err1 = NewGatewayBackend("tcp", "0.0.0.0:10010", 4, 1024, func(msg *GatewayIntput) {
		msgChan <- msg
	})

	if err1 != nil {
		t.Fatal(err1)
	}

	var closeWait = make(chan int)

	wg.Add(1)
	go func() {
		defer func() {
			backend.Close()
			wg.Done()
		}()

		var message1 = <-msgChan

		if strings.Index(string(message1.ReadBytes8()), "127.0.0.1:") != 0 {
			t.Fatal("read message1 failed")
		}

		if backend.NewPackage(message1.ClientId, 4).WriteUint32(1234).Send() != nil {
			t.Fatal("send message2 failed")
		}

		closeWait <- 1
	}()

	var frontend, err2 = NewGatewayFrontend("tcp", "0.0.0.0:10086", 4, 1024, []*GatewayBackendInfo{
		{1, "tcp", "127.0.0.1:10010", 10, true},
	})

	if err2 != nil {
		t.Fatal(err2)
	}

	var client1, err3 = ConnectGateway("tcp", "0.0.0.0:10086", 4, 0, 1024, 1)

	if err3 != nil {
		t.Fatal(err3)
	}

	if client1.ReadPackage().ReadUint32() != 1234 {
		t.Fatal("read message2 failed")
	}

	client1.Close()

	<-closeWait

	frontend.Close()

	wg.Wait()
}

//
// 测试unix套接字
//
func TestUnix(t *testing.T) {
	var wg sync.WaitGroup

	var server, err1 = Listen("unix", "test.sock", 4, 0, 1024)

	if err1 != nil {
		t.Fatal(err1)
	}

	wg.Add(1)
	go func() {
		defer func() {
			server.Close()
			wg.Done()
		}()

		var client = server.Accpet()

		if client == nil {
			t.Fatal("could't accept")
		}

		defer func() {
			client.Close()
		}()

		if client.ReadPackage().ReadUint16() != 0xFFFF {
			t.Fatal("read message1 failed")
		}

		if client.ReadPackage().ReadUint32() != 0xFFFFFFFF {
			t.Fatal("read message1 failed")
		}

		if client.NewPackage(2).WriteUint16(0xFFFF).Send() != nil {
			t.Fatal("send message3 failed")
		}
	}()

	var client, err2 = Connect("unix", "test.sock", 4, 0, 1024)

	if err2 != nil {
		t.Fatal(err2)
	}

	defer func() {
		client.Close()
	}()

	if client.NewPackage(2).WriteUint16(0xFFFF).Send() != nil {
		t.Fatal("send message1 failed")
	}

	if client.NewPackage(4).WriteUint32(0xFFFFFFFF).Send() != nil {
		t.Fatal("send message2 failed")
	}

	if client.ReadPackage().ReadUint16() != 0xFFFF {
		t.Fatal("read message3 failed")
	}

	wg.Wait()
}
