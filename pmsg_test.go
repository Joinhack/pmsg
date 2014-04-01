package pmsg

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func TestBitOper(t *testing.T) {
	hub := NewMsgHub(1, 1024*1024, ":9999")
	hub.AddRoute(1, 1, 2, nil)
	time.Sleep(1 * time.Millisecond)
	if hub.router[1]&RouterMask != 2 {
		t.Fail()
	}
	hub.RemoveRoute(1, 1)
	time.Sleep(1 * time.Millisecond)
	if hub.router[1] != 0 {
		t.Fail()
	}

	t.Logf("%x", hub.router[1])

	hub.AddRoute(1, 1, 2, nil)
	time.Sleep(2 * time.Millisecond)

	hub.AddRoute(1, 2, 9, nil)
	time.Sleep(1 * time.Millisecond)
	if hub.router[1]&RouterTypeMask != 0x80 {
		t.Logf("%x", hub.router[1]&RouterTypeMask)
		t.Fail()
	}
	hub.RemoveRoute(1, 1)
	hub.RemoveRoute(1, 2)
	time.Sleep(1 * time.Millisecond)
	t.Logf("%x", hub.router[1])
	if hub.router[1] != 0 {
		t.Fail()
	}
}

func TestLocalDispatch(t *testing.T) {
	hub := NewMsgHub(1, 1024*1024, ":0")
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
		conn2.Close()
	}()

	clientConn1 := NewSimpleClientConn(conn1, 1, 1)
	clientConn2 := NewSimpleClientConn(conn2, 2, 1)
	hub.AddClient(clientConn1)
	hub.AddClient(clientConn2)
	time.Sleep(10 * time.Millisecond)
	content1 := "hi1"
	content2 := "hi2"
	hub.Dispatch(&DeliverMsg{To: 1, Carry: []byte(content1)})
	hub.Dispatch(&DeliverMsg{To: 2, Carry: []byte(content2)})
	hub.RemoveClient(clientConn1)
	hub.RemoveClient(clientConn2)
	clientConn2.Close()
	clientConn1.Close()
	time.Sleep(20 * time.Millisecond)

	t.Log(hub.router[1])
	if hub.router[1] != 0 || hub.router[2] != 0 {
		t.Fail()
	}
}

func TestKickoffDispatch(t *testing.T) {
	hub1 := NewMsgHub(1, 1024*1024, ":0")
	hub2 := NewMsgHub(2, 1024*1024, ":0")
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
	clientConn2 := NewSimpleClientConn(conn2, 1, 1)
	hub1.AddOutgoing(2, hub2Addr)
	hub2.AddOutgoing(1, hub1Addr)
	hub1.AddClient(clientConn1)
	time.Sleep(1 * time.Millisecond)
	hub2.AddClient(clientConn2)

	time.Sleep(100 * time.Millisecond)
	if hub1.router[1]&RouterMask != hub2.router[1]&RouterMask {
		t.Fail()
	}
}

func TestRemoteDispatch(t *testing.T) {
	hub1 := NewMsgHub(1, 1024*1024, ":0")
	hub2 := NewMsgHub(2, 1024*1024, ":0")
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
	hub1.AddClient(clientConn1)
	hub2.AddClient(clientConn2)
	time.Sleep(10 * time.Millisecond)
	hub1.AddOutgoing(2, hub2Addr)
	hub2.AddOutgoing(1, hub1Addr)
	time.Sleep(10 * time.Millisecond)
	content1 := "hi1"
	content2 := "hi2"
	hub2.Dispatch(&DeliverMsg{To: 1, Carry: []byte(content1)})
	hub1.Dispatch(&DeliverMsg{To: 2, Carry: []byte(content2)})
	hub1.Dispatch(&DeliverMsg{To: 2, Carry: []byte(content2)})
	hub1.Dispatch(&DeliverMsg{To: 2, Carry: []byte(content2)})
	hub1.Dispatch(&DeliverMsg{To: 2, Carry: []byte(content2)})
	time.Sleep(3 * time.Second)
	hub1.RemoveClient(clientConn1)
	hub2.RemoveClient(clientConn2)
	clientConn2.Close()
	clientConn1.Close()
	time.Sleep(20 * time.Millisecond)
	if hub1.router[1] != 0 || hub1.router[2] != 0 {
		t.Fail()
	}
	if hub2.router[1] != 0 || hub2.router[2] != 0 {
		t.Fail()
	}
}
