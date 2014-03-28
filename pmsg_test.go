package pmsg

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func TestBitOper(t *testing.T) {
	hub := NewMsgHub(1, 1024*1024, ":9999")
	hub.AddRoute(1, 1, 2)
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

	hub.AddRoute(1, 1, 2)
	time.Sleep(2 * time.Millisecond)

	hub.AddRoute(1, 2, 9)
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
				go func() {
					var buf [1024]byte
					var n int
					var err error
					if n, err = c.Read(buf[:]); err != nil {
						panic(err)
					}
					t.Log(c.LocalAddr, string(buf[:n]))
				}()
			}
		}
	}()
	time.Sleep(100 * time.Millisecond)
	servAddr := fmt.Sprintf("127.0.0.1:%d", ln.Addr().(*net.TCPAddr).Port)
	conn1, _ := net.Dial("tcp", servAddr)
	conn2, _ := net.Dial("tcp", servAddr)
	defer func() {
		conn1.Close()
		conn2.Close()
	}()
	clientConn1 := &ClientConn{Id: 1, Type: 1, Conn: conn1}
	clientConn2 := &ClientConn{Id: 2, Type: 1, Conn: conn2}
	hub.AddClient(clientConn1)
	hub.AddClient(clientConn2)
	time.Sleep(1000 * time.Millisecond)
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
