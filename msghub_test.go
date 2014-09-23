package pmsg

import (
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

var delta time.Duration

func init() {
	StateNotiferNum = 1
	writer, _ := os.OpenFile(os.DevNull, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	ERROR = log.New(writer, "", 0)
	INFO = log.New(writer, "", 0)
	WARN = log.New(writer, "", 0)
	TRACE = log.New(writer, "", 0)

	if OneConnectionForPeer {
		delta = 10
	} else {
		delta = 10
	}
}

func TestBitOper(t *testing.T) {
	hub := NewMsgHub(1, 1024*1024, ":9999")
	defer func() {
		hub.Close()
	}()
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
	defer func() {
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
	if err := hub.AddClient(clientConn1); err != nil {
		panic(err)
	}
	if err := hub.AddClient(clientConn2); err != nil {
		panic(err)
	}
	time.Sleep(10 * time.Millisecond)
	content1 := "hi1"
	content2 := "hi2"
	hub.Dispatch(NewDeliverMsg(RouteMsgType, 1, []byte(content1)))
	hub.Dispatch(NewDeliverMsg(RouteMsgType, 2, []byte(content2)))
	hub.RemoveClient(clientConn1)
	hub.RemoveClient(clientConn2)
	clientConn2.Close()
	clientConn1.Close()
	time.Sleep(20 * time.Millisecond)
	ln.Close()
	t.Log(hub.router[1])
	if hub.router[1] != 0 || hub.router[2] != 0 {
		t.Fail()
	}
}

func Test2DevDispatch(t *testing.T) {
	hub := NewMsgHub(1, 1024*1024, ":0")
	defer func() {
		hub.Close()
	}()
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
	time.Sleep(10 * time.Millisecond)
	servAddr := fmt.Sprintf("127.0.0.1:%d", ln.Addr().(*net.TCPAddr).Port)
	conn1, _ := net.Dial("tcp", servAddr)
	conn2, _ := net.Dial("tcp", servAddr)
	defer func() {
		conn1.Close()
		conn2.Close()
	}()

	clientConn1 := NewSimpleClientConn(conn1, 1, 1)
	clientConn2 := NewSimpleClientConn(conn2, 1, 2)
	if err := hub.AddClient(clientConn1); err != nil {
		panic(err)
	}
	if err := hub.AddClient(clientConn2); err != nil {
		panic(err)
	}
	time.Sleep(delta * time.Millisecond)
	content1 := "hi1"
	hub.Dispatch(NewDeliverMsg(RouteMsgType, 1, []byte(content1)))
	hub.RemoveClient(clientConn1)
	time.Sleep(1 * time.Millisecond)
	t.Log(hub.router[1])
	if hub.router[1] != 2<<6|1 {
		t.Fail()
	}
	hub.RemoveClient(clientConn2)

	clientConn2.Close()
	clientConn1.Close()
	time.Sleep(2 * time.Millisecond)
	ln.Close()
	t.Log(hub.router[1])
	if hub.router[1] != 0 {
		t.Fail()
	}
}

func TestKickoff1(t *testing.T) {
	hub1 := NewMsgHub(1, 1024*1024, ":0")
	hub2 := NewMsgHub(2, 1024*1024, ":0")
	ln, _ := net.Listen("tcp", ":0")
	defer func() {
		hub1.Close()
		hub2.Close()
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
	hub1.AddOtherHub(2, hub2Addr)
	hub2.AddOtherHub(1, hub1Addr)
	wait(hub1, hub2)
	if err := hub1.AddClient(clientConn1); err != nil {
		panic(err)
	}
	if err := hub2.AddClient(clientConn2); err != nil {
		panic(err)
	}
	time.Sleep(delta * 2 * time.Millisecond)
	ln.Close()
	if clientConn1.IsKickoff() != true || clientConn2.IsKickoff() != true {
		t.Fail()
	}

}

func TestKickoff2(t *testing.T) {
	hub1 := NewMsgHub(1, 1024*1024, ":0")
	hub2 := NewMsgHub(2, 1024*1024, ":0")
	ln, _ := net.Listen("tcp", ":0")
	defer func() {
		hub1.Close()
		hub2.Close()
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
	hub1.AddOtherHub(2, hub2Addr)
	hub2.AddOtherHub(1, hub1Addr)
	if err := hub1.AddClient(clientConn1); err != nil {
		panic(err)
	}

	for hub2.outgoing[1] == nil || hub2.outgoing[1].State() == 0 {
		time.Sleep(delta * time.Millisecond)
	}

	if err := hub2.AddClient(clientConn2); err != nil {
		panic(err)
	}
	time.Sleep(delta * 2 * time.Millisecond)

	ln.Close()
	if clientConn1.IsKickoff() != true || clientConn2.IsKickoff() != false || hub1.router[1] != hub2.router[1] {
		t.Fail()
	}
}

func TestRedirect(t *testing.T) {
	hub1 := NewMsgHub(1, 1024*1024, ":0")
	hub2 := NewMsgHub(2, 1024*1024, ":0")
	ln, _ := net.Listen("tcp", ":0")
	defer func() {
		hub1.Close()
		hub2.Close()
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
	clientConn2 := NewSimpleClientConn(conn2, 1, 2)
	hub1.AddOtherHub(2, hub2Addr)
	hub2.AddOtherHub(1, hub1Addr)
	wait(hub1, hub2)
	if err := hub1.AddClient(clientConn1); err != nil {
		panic(err)
	}
	time.Sleep(1 * time.Millisecond)
	if err := hub2.AddClient(clientConn2); err == nil {
		t.Fail()
	}

	time.Sleep(10 * time.Millisecond)
	ln.Close()
	t.Log(hub1.router[1], hub2.router[1])
	if hub1.router[1]&RouterMask != hub2.router[1]&RouterMask {
		t.Fail()
	}
}

func wait(h1 *MsgHub, h2 *MsgHub) {
	for h1.outgoing[2] == nil ||
		h1.outgoing[2].State() != conn_ok ||
		h2.outgoing[1] == nil ||
		h2.outgoing[1].State() != conn_ok {
		time.Sleep(delta * time.Millisecond)
	}
}

func TestRemoteDispatch(t *testing.T) {
	hub1 := NewMsgHub(1, 1024*1024, ":0")
	hub2 := NewMsgHub(2, 1024*1024, ":0")
	ln, _ := net.Listen("tcp", ":0")
	defer func() {
		hub1.Close()
		hub2.Close()
	}()
	wg := &sync.WaitGroup{}
	var conn1addr, conn2addr string
	go func() {
		for {
			if c, err := ln.Accept(); err != nil {
				return
			} else {
				go func(conn net.Conn) {
					var buf [1024]byte
					var n int
					var err error
					for {
						if n, err = conn.Read(buf[:]); err != nil {
							return
						}
						if conn.RemoteAddr().String() == conn1addr {
							wg.Done()
						}
						if conn.RemoteAddr().String() == conn2addr {
							s := string(buf[:n])
							if strings.Index(s, ",1") != -1 {
								wg.Done()
							}
							if strings.Index(s, ",2") != -1 {
								wg.Done()
							}
							if strings.Index(s, ",3") != -1 {
								wg.Done()
							}
							if strings.Index(s, ",4") != -1 {
								wg.Done()
							}
						}
						t.Log(c.RemoteAddr().String(), string(buf[:n]), "\n")
					}
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
	conn1addr = conn1.LocalAddr().String()
	conn2addr = conn2.LocalAddr().String()
	defer func() {
		conn1.Close()
		conn2.Close()
	}()

	clientConn1 := NewSimpleClientConn(conn1, 1, 1)
	clientConn2 := NewSimpleClientConn(conn2, 2, 1)
	hub1.AddClient(clientConn1)
	hub2.AddClient(clientConn2)
	hub1.AddOtherHub(2, hub2Addr)
	hub2.AddOtherHub(1, hub1Addr)
	wait(hub1, hub2)

	//wait for build route.
	for hub2.router[1] == 0 || hub1.router[2] == 0 {
		time.Sleep(delta * time.Millisecond)
	}
	content1 := "hi1"
	content2 := "\nhi2-:"
	hub2.Dispatch(NewDeliverMsg(RouteMsgType, 1, []byte(content1)))

	wg.Add(1)
	hub1.Dispatch(NewDeliverMsg(RouteMsgType, 2, []byte(content2+",1")))
	wg.Add(1)
	hub1.Dispatch(NewDeliverMsg(RouteMsgType, 2, []byte(content2+",2")))
	wg.Add(1)
	hub1.Dispatch(NewDeliverMsg(RouteMsgType, 2, []byte(content2+",3")))
	wg.Add(1)
	hub1.Dispatch(NewDeliverMsg(RouteMsgType, 2, []byte(content2+",4")))
	wg.Add(1)
	wg.Wait()
	hub1.RemoveClient(clientConn1)
	hub2.RemoveClient(clientConn2)
	clientConn2.Close()
	clientConn1.Close()
	for hub2.router[1] != 0 || hub1.router[2] != 0 {
		time.Sleep(delta * time.Millisecond)
	}
	ln.Close()
	if hub1.router[1] != 0 || hub1.router[2] != 0 {
		t.Fail()
	}
	if hub2.router[1] != 0 || hub2.router[2] != 0 {
		t.Fail()
	}
}

func TestConnectionBroken(t *testing.T) {
	hub1 := NewMsgHub(1, 1024*1024, ":0")
	hub2 := NewMsgHub(2, 1024*1024, ":0")
	ln, _ := net.Listen("tcp", ":0")
	defer func() {
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
					var buf [4]byte
					var n int
					var err error
					for {
						if n, err = conn.Read(buf[:]); err != nil {
							return
						}
						if string(buf[:n]) == "9999" {
							wg.Done()
						}
					}
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
	hub1.AddOtherHub(2, hub2Addr)
	hub2.AddOtherHub(1, hub1Addr)
	wait(hub1, hub2)
	
	//wait for build route.
	for hub2.router[1] == 0 || hub1.router[2] == 0 {
		time.Sleep(delta * time.Millisecond)
	}
	wg.Add(1)
	wg2 := &sync.WaitGroup{}
	wg2.Add(1)
	go func() {
		for i := 0; i < 9999; i++ {
			if i == 1000 {
				wg2.Done()
			}
			s := fmt.Sprintf("%04d", i)
			hub1.Dispatch(NewDeliverMsg(RouteMsgType, 2, []byte(s)))
		}
	}()
	wg2.Wait()
	for i := 0; i < 100; i++ {
		hub1.outgoing[2].Close()
		hub2.outgoing[1].Close()
		wait(hub1, hub2)
	}
	for hub1.router[2] == 0 || hub1.router[1] == 0 {
		time.Sleep(delta * time.Millisecond)
	}
	s := fmt.Sprintf("%04d", 9999)
	hub1.Dispatch(NewDeliverMsg(RouteMsgType, 2, []byte(s)))
	wg.Wait()
	hub1.RemoveClient(clientConn1)
	hub2.RemoveClient(clientConn2)
	clientConn2.Close()
	clientConn1.Close()
	for hub2.router[1] != 0 || hub1.router[2] != 0 {
		time.Sleep(delta * time.Millisecond)
	}
	ln.Close()
	if hub1.router[1] != 0 || hub1.router[2] != 0 {
		t.Fail()
	}
	if hub2.router[1] != 0 || hub2.router[2] != 0 {
		t.Fail()
	}
}
