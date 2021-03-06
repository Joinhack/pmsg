package pmsg

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"strings"
	"time"
)


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
	wg := &sync.WaitGroup{}
	hub := NewMsgHubWithFileStoreOfflineCenter(cfg)
	ln, _ := net.Listen("tcp", ":0")
	defer func(){
		hub.Close()
	}()
	go func() {
		for {
			if c, err := ln.Accept(); err != nil {
				return
			} else {
				go func(conn net.Conn) {
					var buf [1024]byte
					var n int
					var err error
					if n, err = conn.Read(buf[:]); err != nil {
						return
					}
					s := string(buf[:n])
					if strings.Index(s, "hi2") != -1 {
						wg.Done()
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
	hub.Dispatch(NewDeliverMsg(RouteMsgType, 2, []byte(content2)))
	//wait for go to offline
	time.Sleep(20 * time.Millisecond)
	if err := hub.AddClient(clientConn2); err != nil {
		panic(err)
	}
	wg.Add(1)
	wg.Wait()
	ln.Close()
	hub.RemoveClient(clientConn1)
	clientConn1.Close()
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
	defer func(){
		hub1.Close()
		hub2.Close()
	}()
	wg := &sync.WaitGroup{}
	go func() {
		for {
			if c, err := ln.Accept(); err != nil {
				return
			} else {
				go func(conn net.Conn) {
					var buf [1024]byte
					var n int
					var err error
					if n, err = conn.Read(buf[:]); err != nil {
						return
					}
					s := string(buf[:n])
					if strings.Index(s, "hi2") != -1 {
						wg.Done()
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
	time.Sleep(200 * time.Millisecond)
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
	hub1.Dispatch(NewDeliverMsg(RouteMsgType, 101, []byte(content2)))
	wg.Add(1)
	//wait for go to offline
	time.Sleep(20 * time.Millisecond)
	if err := hub1.AddClient(clientConn2); err != nil {
		panic(err)
	}
	wg.Wait()
	hub1.RemoveClient(clientConn1)
	clientConn1.Close()
	clientConn2.Close()
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
	defer func(){
		hub1.Close()
		hub2.Close()
	}()
	go hub1.ListenAndServe()
	go hub2.ListenAndServe()
	time.Sleep(5 * time.Millisecond)
	hub1Addr := fmt.Sprintf("127.0.0.1:%d", hub1.listener.Addr().(*net.TCPAddr).Port)
	hub2Addr := fmt.Sprintf("127.0.0.1:%d", hub2.listener.Addr().(*net.TCPAddr).Port)
	hub1.AddOtherHub(2, hub2Addr)
	hub2.AddOtherHub(1, hub1Addr)
	wait(hub1, hub2)
	wg := &sync.WaitGroup{}
	for m := 0; m < 2; m++ {
		wg.Add(1)
		go func() {
			for i := 20001; i < 40000; i++ {
				hub1.Dispatch(NewDeliverMsg(RouteMsgType, uint32(i), []byte("11212121212")))
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
