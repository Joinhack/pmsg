package pmsg

import (
	"errors"
	"log"
	"net"
	"os"
	"time"
)

const (
	StringMsgType = 0 //like ping, small string

)

var (
	OutofServerRange             = errors.New("out of max server limits")
	TRACE            *log.Logger = log.New(os.Stdout, "TRACE ", log.Ldate|log.Ltime|log.Lshortfile)
	WARN             *log.Logger = log.New(os.Stdout, "WARN ", log.Ldate|log.Ltime|log.Lshortfile)
	INFO             *log.Logger = log.New(os.Stdout, "INFO ", log.Ldate|log.Ltime|log.Lshortfile)
	ERROR            *log.Logger = log.New(os.Stderr, "ERROR ", log.Ldate|log.Ltime|log.Lshortfile)
)

type Msg interface {
	Bytes() []byte
}

type RouterMsg interface {
	Msg
	Destination() uint64
}

type Conn struct {
	net.Conn
	address   string
	writeChan chan Msg
	id        int
}

type routerOper struct {
	destination uint64
	value       byte
	oper        int
}

const (
	RouterMaskBits = 1 << 6
	RouterMask     = 0xc0
	oper_set       = 0
	oper_and       = 1
	oper_or        = 2
)

type MsgHub struct {
	id             int
	maxDest        uint64
	router         []byte
	outgoing       [RouterMaskBits]*Conn
	incoming       [RouterMaskBits]*Conn
	routerOperChan chan *routerOper
}

func (hub *MsgHub) writeRouter() {
	for {
		select {
		case oper := <-hub.routerOperChan:
			if oper.destination > hub.maxDest {
				ERROR.Println("invalidate operation")
				continue
			}
			switch oper.oper {
			case oper_set:
				hub.router[oper.destination] = oper.value
			case oper_and:
				hub.router[oper.destination] = oper.value & hub.router[oper.destination]
			case oper_or:
				hub.router[oper.destination] = oper.value | hub.router[oper.destination]
			}
		}
	}
}

func (hub *MsgHub) RegisterRouter(typ int, d uint64, id int) error {
	if id >= len(hub.outgoing) || id < 0 {
		return OutofServerRange
	}
	v := byte(typ) << 6 & 0xff & byte(id)
	hub.routerOperChan <- &routerOper{destination: d, value: v}
	return nil
}

func NewMsgHub(id int, maxDest uint64) *MsgHub {
	hub := &MsgHub{
		id:             id,
		router:         make([]byte, maxDest),
		routerOperChan: make(chan *routerOper),
		maxDest:        maxDest,
	}
	go hub.writeRouter()
	return hub
}

type StringMsg struct {
	bytes []byte
}

func NewStringMsg(content string) *StringMsg {
	msg := &StringMsg{}
	msg.bytes = make([]byte, 1+len(content))
	msg.bytes[0] = StringMsgType
	copy(msg.bytes[1:], []byte(content))
	return msg
}

func (c *StringMsg) Bytes() []byte {
	return c.bytes
}

var Ping = NewStringMsg("PING")

func OutgoingLoop(conn *Conn) {
	var err error
	var msg Msg
	for {
		if conn.Conn == nil {
			conn.Conn, err = net.Dial("tcp", conn.address)
			if err != nil {
				ERROR.Println("connection to", conn.address, "fail, retry after 2 sec.")
				time.Sleep(2 * time.Second)
				continue
			}
		}

		if msg != nil {
			goto WRITE
		}

		select {
		case msg = <-conn.writeChan:
		case <-time.After(2 * time.Second):
			msg = Ping
		}
	WRITE:
		_, err = conn.Write(msg.Bytes())
		if err != nil {
			ERROR.Println(err)
			//close connection and reconnection later
			conn.Close()
			conn.Conn = nil
			continue
		}
		msg = nil
	}
}

func (hub *MsgHub) AddOutgoing(id int, addr string) (conn *Conn, err error) {
	if id >= len(hub.outgoing) || id < 0 {
		err = OutofServerRange
		return
	}
	var c net.Conn
	c, err = net.Dial("tcp", addr)
	if err != nil {
		return
	}
	conn = &Conn{id: id, Conn: c, address: addr}
	hub.outgoing[id] = conn
	go OutgoingLoop(conn)
	return
}
