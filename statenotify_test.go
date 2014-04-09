package pmsg

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

func TestStateNotifer(t *testing.T) {
	hub1 := newMsgHub(1, 1024*1024, ":0")
	hub2 := newMsgHub(2, 1024*1024, ":0")
	ln, _ := net.Listen("tcp", ":0")
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

	servAddr := fmt.Sprintf("127.0.0.1:%d", ln.Addr().(*net.TCPAddr).Port)
	conn1, _ := net.Dial("tcp", servAddr)
	conn2, _ := net.Dial("tcp", servAddr)
	defer func() {
		conn1.Close()
		conn2.Close()
	}()

	clientConn1 := NewSimpleClientConn(conn1, 1, 1)
	clientConn2 := NewSimpleClientConn(conn2, 2, 1)

	time.Sleep(10 * time.Millisecond)
	hub1.AddOutgoing(2, hub2Addr)
	hub2.AddOutgoing(1, hub1Addr)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	elem := hub1.AddWatcher(2, func(e *StateEvent) {
		t.Log(e.State)
		wg.Done()
	})

	hub1.AddClient(clientConn1)
	hub2.AddClient(clientConn2)
	wg.Wait()
	hub1.RemoveWatcher(elem)
	hub1.RemoveClient(clientConn1)

	hub2.RemoveClient(clientConn2)

	clientConn2.Close()
	clientConn1.Close()

}
