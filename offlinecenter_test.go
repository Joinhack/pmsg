package pmsg

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

func init() {
	OneConnectionForPeer = false
}

func TestOfflineCenterLocalDispatch(t *testing.T) {
	DefaultArchivedTime = 1
	DefaultFlushTime = 1
	cfg := &MsgHubFileStoreOfflineCenterConfig{Id: 1,
		MaxRange:          1024 * 1024,
		ServAddr:          ":0",
		OfflineRangeStart: 1,
		OfflineRangeEnd:   1000,
		OfflinePath:       "/tmp",
	}
	hub := NewMsgHubWithFileStoreOfflineCenter(cfg)
	ln, _ := net.Listen("tcp", ":0")
	go func() {
		for {
			if c, err := ln.Accept(); err != nil {
				panic(err)
			} else {
				go func(conn net.Conn) {
					var buf [1024]byte
					var n int
					var err error
					if n, err = conn.Read(buf[:]); err != nil {
						return
					}
					t.Log(c.RemoteAddr().String(), string(buf[:n]))
				}(c)
			}
		}
	}()
	time.Sleep(10 * time.Millisecond)
	servAddr := fmt.Sprintf("127.0.0.1:%d", ln.Addr().(*net.TCPAddr).Port)
	conn1, _ := net.Dial("tcp", servAddr)
	conn2, _ := net.Dial("tcp", servAddr)
	defer func() {
		conn1.Close()
	}()

	clientConn1 := NewSimpleClientConn(conn1, 1, 1)
	clientConn2 := NewSimpleClientConn(conn2, 2, 1)

	if err := hub.AddClient(clientConn1); err != nil {
		panic(err)
	}
	content2 := "hi2"
	hub.Dispatch(&DeliverMsg{To: 2, Carry: []byte(content2)})
	time.Sleep(100 * time.Millisecond)
	if err := hub.AddClient(clientConn2); err != nil {
		panic(err)
	}
	time.Sleep(1 * time.Second)
	hub.RemoveClient(clientConn1)
	clientConn1.Close()
	time.Sleep(20 * time.Millisecond)

}

func TestOfflineCenterRemoteDispatch(t *testing.T) {
	DefaultArchivedTime = 1
	DefaultFlushTime = 1
	cfg := &MsgHubFileStoreOfflineCenterConfig{
		Id:                1,
		MaxRange:          1024 * 1024,
		ServAddr:          ":0",
		OfflineRangeStart: 1,
		OfflineRangeEnd:   100,
		OfflinePath:       "/tmp",
	}
	hub1 := NewMsgHubWithFileStoreOfflineCenter(cfg)

	cfg = &MsgHubFileStoreOfflineCenterConfig{
		Id:                2,
		MaxRange:          1024 * 1024,
		ServAddr:          ":0",
		OfflineRangeStart: 101,
		OfflineRangeEnd:   200,
		OfflinePath:       "/tmp",
	}
	hub2 := NewMsgHubWithFileStoreOfflineCenter(cfg)
	ln, _ := net.Listen("tcp", ":0")
	go func() {
		for {
			if c, err := ln.Accept(); err != nil {
				panic(err)
			} else {
				go func(conn net.Conn) {
					var buf [1024]byte
					var n int
					var err error
					if n, err = conn.Read(buf[:]); err != nil {
						return
					}
					t.Log(c.RemoteAddr().String(), string(buf[:n]))
				}(c)
			}
		}
	}()
	go hub1.ListenAndServe()
	go hub2.ListenAndServe()
	time.Sleep(10 * time.Millisecond)
	hub1Addr := fmt.Sprintf("127.0.0.1:%d", hub1.listener.Addr().(*net.TCPAddr).Port)
	hub2Addr := fmt.Sprintf("127.0.0.1:%d", hub2.listener.Addr().(*net.TCPAddr).Port)
	hub1.AddOtherHub(2, hub2Addr)
	hub2.AddOtherHub(1, hub1Addr)
	time.Sleep(10 * time.Millisecond)
	servAddr := fmt.Sprintf("127.0.0.1:%d", ln.Addr().(*net.TCPAddr).Port)
	conn1, _ := net.Dial("tcp", servAddr)
	conn2, _ := net.Dial("tcp", servAddr)
	defer func() {
		conn1.Close()
		conn2.Close()
	}()

	clientConn1 := NewSimpleClientConn(conn1, 1, 1)
	clientConn2 := NewSimpleClientConn(conn2, 101, 1)

	if err := hub1.AddClient(clientConn1); err != nil {
		panic(err)
	}
	content2 := "hi2"
	hub1.Dispatch(&DeliverMsg{To: 101, Carry: []byte(content2)})
	time.Sleep(1 * time.Second)
	if err := hub1.AddClient(clientConn2); err != nil {
		panic(err)
	}
	time.Sleep(1 * time.Second)
	hub1.RemoveClient(clientConn1)
	clientConn1.Close()
	clientConn2.Close()
	time.Sleep(20 * time.Millisecond)

}

func TestOfflineCenterArchive(t *testing.T) {
	cfg := &MsgHubFileStoreOfflineCenterConfig{Id: 1,
		MaxRange:          1024 * 1024,
		ServAddr:          ":0",
		OfflineRangeStart: 1,
		OfflineRangeEnd:   20000,
		OfflinePath:       "/tmp",
	}

	hub1 := NewMsgHubWithFileStoreOfflineCenter(cfg)

	cfg = &MsgHubFileStoreOfflineCenterConfig{Id: 2,
		MaxRange:          1024 * 1024,
		ServAddr:          ":0",
		OfflineRangeStart: 20001,
		OfflineRangeEnd:   40000,
		OfflinePath:       "/tmp",
	}
	hub2 := NewMsgHubWithFileStoreOfflineCenter(cfg)
	go hub1.ListenAndServe()
	go hub2.ListenAndServe()
	time.Sleep(10 * time.Millisecond)
	hub1Addr := fmt.Sprintf("127.0.0.1:%d", hub1.listener.Addr().(*net.TCPAddr).Port)
	hub2Addr := fmt.Sprintf("127.0.0.1:%d", hub2.listener.Addr().(*net.TCPAddr).Port)
	hub1.AddOtherHub(2, hub2Addr)
	hub2.AddOtherHub(1, hub1Addr)
	time.Sleep(10 * time.Millisecond)
	wg := &sync.WaitGroup{}
	for m := 0; m < 2; m++ {
		wg.Add(1)
		go func() {
			for i := 20001; i < 40000; i++ {
				hub1.Dispatch(&DeliverMsg{To: uint64(i), Carry: []byte("11212121212")})
			}
			wg.Done()
		}()
	}
	wg.Wait()
	time.Sleep(1 * time.Second)
}
