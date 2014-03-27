package pmsg

import (
	"encoding/binary"
	"errors"
	//"fmt"
	"log"
	"net"
	"os"
	"time"
)

const (
	StringMsgType  = iota //like ping, framed msg
	ControlMsgType        //framed msg
	RouteMsgType
)

const (
	WhoamIControlType = iota
	AddRouteControlType
	RemoveRouteControlType
)

var (
	OutofServerRange = errors.New("out of max server limits")

	UnknownFramedMsg = errors.New("unknow framed msg")

	TRACE *log.Logger = log.New(os.Stdout, "TRACE ", log.Ldate|log.Ltime|log.Lshortfile)
	WARN  *log.Logger = log.New(os.Stdout, "WARN ", log.Ldate|log.Ltime|log.Lshortfile)
	INFO  *log.Logger = log.New(os.Stdout, "INFO ", log.Ldate|log.Ltime|log.Lshortfile)
	ERROR *log.Logger = log.New(os.Stderr, "ERROR ", log.Ldate|log.Ltime|log.Lshortfile)
)

type Msg interface {
	Bytes() []byte
}

type RouteMsg interface {
	Msg
	Destination() uint64
}

type Conn struct {
	net.Conn
	address string
	wchan   chan Msg
	id      int
}

type routerOper struct {
	destination uint64
	typ         byte
	serverid    int
	oper        int
}

const (
	RouterMaskBits = 1 << 6
	RouterMask     = 0x3f
	RouterTypeMask = 0x80
	oper_add       = 0
	oper_remove    = 1
)

type MsgHub struct {
	id             int
	maxDest        uint64
	router         []byte
	servAddr       string
	outgoing       [RouterMaskBits]*Conn
	incoming       [RouterMaskBits]*Conn
	routerOperChan chan *routerOper
}

func (hub *MsgHub) processRouterOper() {
	for {
		select {
		case oper := <-hub.routerOperChan:
			if oper.destination > hub.maxDest {
				ERROR.Println("invalidate operation")
				continue
			}
			switch oper.oper {
			case oper_add:

				v := (oper.typ << 6) | (RouterMask & byte(oper.serverid))
				hub.router[oper.destination] = v
			case oper_remove:
				if hub.router[oper.destination] != 0 {
					r := hub.router[oper.destination]
					typ := RouterTypeMask & r
					v := typ & ^(oper.typ << 6)
					if v == 0 {
						hub.router[oper.destination] = 0
					} else {
						hub.router[oper.destination] = (RouterMask & r) | v
					}
				}
			}
		}
	}
}

func (hub *MsgHub) AddRoute(d uint64, typ byte, id int) error {
	if id >= len(hub.outgoing) || id < 0 {
		return OutofServerRange
	}

	hub.routerOperChan <- &routerOper{destination: d, typ: typ, serverid: id, oper: oper_add}
	return nil
}

func (hub *MsgHub) RemoveRoute(d uint64, typ byte) error {
	hub.routerOperChan <- &routerOper{destination: d, typ: typ, oper: oper_remove}
	return nil
}

func NewMsgHub(id int, maxDest uint64, servAddr string) *MsgHub {
	hub := &MsgHub{
		id:             id,
		router:         make([]byte, maxDest),
		routerOperChan: make(chan *routerOper),
		maxDest:        maxDest,
		servAddr:       servAddr,
	}
	go hub.processRouterOper()
	return hub
}

type StringMsg struct {
	bytes []byte
}

type WhoamIMsg struct {
	ControlType int
	Who         int
}

type RouteControlMsg struct {
	ControlType int
	Type        uint8 //0x1 0x2
	Destination uint64
}

func (msg *RouteControlMsg) Unmarshal(bs []byte) error {
	if len(bs) != msg.Len() {
		return UnknownFramedMsg
	}
	msg.ControlType = int(bs[1])
	msg.Type = bs[2]
	msg.Destination = binary.LittleEndian.Uint64(bs[3:])
	return nil
}

func NewWhoamIMsg(who int) *WhoamIMsg {
	msg := &WhoamIMsg{ControlType: WhoamIControlType, Who: who}
	return msg
}

func (msg *RouteControlMsg) Len() int {
	return 11
}

func (msg *RouteControlMsg) Bytes() []byte {
	bs := make([]byte, msg.Len())
	bs[0] = ControlMsgType
	bs[1] = byte(msg.ControlType)
	bs[2] = msg.Type & RouterTypeMask
	binary.LittleEndian.PutUint64(bs[3:], msg.Destination)
	return bs
}

func (msg *WhoamIMsg) Len() int {
	return 3
}

func (msg *WhoamIMsg) Bytes() []byte {
	bs := make([]byte, 3)
	bs[0] = ControlMsgType
	bs[1] = WhoamIControlType
	bs[2] = byte(msg.Who)
	return bs
}

func (msg *WhoamIMsg) Unmarshal(bs []byte) error {

	if len(bs) != msg.Len() {
		return UnknownFramedMsg
	}
	if bs[0] != ControlMsgType {
		return UnknownFramedMsg
	}
	if bs[1] != WhoamIControlType {
		return UnknownFramedMsg
	}
	msg.Who = int(bs[2])
	return nil
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

func (hub *MsgHub) outgoingLoop(conn *Conn) {
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
			//regist self in other hub
			_, err = conn.Write(NewWhoamIMsg(hub.id).Bytes())
			if err != nil {
				ERROR.Println("write error, detail:", err)
				conn.Close()
				conn.Conn = nil
				continue
			}
		}

		if msg != nil {
			goto WRITE
		}

		select {
		case msg = <-conn.wchan:
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

func (hub *MsgHub) LocalDispatch(msg RouteMsg) {

}

func (hub *MsgHub) RemoteDispatch(msg RouteMsg) {
	outgoing := hub.outgoing[msg.Destination()]
	defer func() {
		if err := recover(); err != nil {
			//TODO: if outgoing write channel close,  should remove the outgoing from hub
			ERROR.Println(err)
		}
	}()
	if outgoing != nil {
		outgoing.wchan <- msg
	} else {
		//TODO: persist msg, wait for the outgoing connection. then send the msg
		WARN.Println("waiting for TODO")
	}
}

func (hub *MsgHub) Dispatch(msg RouteMsg) {
	dest := msg.Destination()
	id := int(hub.router[dest] & RouteMsg)
	if hub.id == id {
		hub.LocalDispatch(msg)
	} else {
		hub.RemoteDispatch(msg)
	}
}

func (hub *MsgHub) incomingLoop(c net.Conn) {
	var buf = make([]byte, 4096) //framed size
	var err error
	var n int
	n, err = c.Read(buf)
	var whoami WhoamIMsg
	if n != whoami.Len() {
		c.Close()
		ERROR.Println(UnknownFramedMsg)
		return
	}
	conn := &Conn{id: whoami.Who, Conn: c}
	if hub.incoming[conn.id] != nil {
		hub.incoming[conn.id].Close()
	}
	defer func() {
		conn.Close()
		hub.incoming[conn.id] = nil
	}()
	hub.incoming[conn.id] = conn
	for {
		n, err = conn.Read(buf)
		if err != nil {
			ERROR.Println("read from incoming error, detail:", err)
			return
		}
		switch buf[0] {
		case StringMsgType:
			//drop msg, should be ping
		case ControlMsgType: //cronntrol msg
			switch buf[1] {
			case AddRouteControlType, RemoveRouteControlType:
				var msg RouteControlMsg
				if err = msg.Unmarshal(buf[:n]); err != nil {
					ERROR.Println(err)
					return
				}
				if msg.ControlType == AddRouteControlType {
					err = hub.AddRoute(msg.Destination, msg.Type, conn.id)
				} else {
					err = hub.RemoveRoute(msg.Destination, msg.Type)
				}
				if err != nil {
					ERROR.Println(err)
					return
				}
			}
		}
	}

}

func (hub *MsgHub) ListenAndServe() error {
	var err error
	var ln net.Listener
	var c net.Conn
	ln, err = net.Listen("tcp", hub.servAddr)
	if err != nil {
		return err
	}
	for {
		if c, err = ln.Accept(); err != nil {
			return err
		}
		go hub.incomingLoop(c)
	}
}

func (hub *MsgHub) AddOutgoing(id int, addr string) (conn *Conn, err error) {
	if id >= len(hub.outgoing) || id < 0 {
		err = OutofServerRange
		return
	}
	conn = &Conn{id: id, address: addr, wchan: make(chan Msg, 1)}
	hub.outgoing[id] = conn
	go hub.outgoingLoop(conn)
	return
}
