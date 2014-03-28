package pmsg

import (
	"encoding/binary"
	"errors"
	"fmt"
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

	NoSuchType = errors.New("No such type")

	UnknownMsg = errors.New("unknow msg")

	OutOfHubIdRange = errors.New("Out of hub id range")

	TRACE *log.Logger = log.New(os.Stdout, "TRACE ", log.Ldate|log.Ltime|log.Lshortfile)
	WARN  *log.Logger = log.New(os.Stdout, "WARN ", log.Ldate|log.Ltime|log.Lshortfile)
	INFO  *log.Logger = log.New(os.Stdout, "INFO ", log.Ldate|log.Ltime|log.Lshortfile)
	ERROR *log.Logger = log.New(os.Stderr, "ERROR ", log.Ldate|log.Ltime|log.Lshortfile)
)

type Msg interface {
	Bytes() []byte
	Body() []byte
}

type RouteMsg interface {
	Msg
	Destination() uint64
}

type DeliverMsg struct {
	RouteMsg
	To    uint64
	Carry []byte
}

func (msg *DeliverMsg) Destination() uint64 {
	return msg.To
}

func (msg *DeliverMsg) Body() []byte {
	return msg.Carry
}

func (msg *DeliverMsg) Bytes() []byte {
	bs := make([]byte, 13+len(msg.Carry))
	copy(bs[13:], msg.Carry)
	bs[0] = RouteMsgType
	binary.LittleEndian.PutUint64(bs[1:], msg.Destination())
	binary.LittleEndian.PutUint32(bs[9:], uint32(len(msg.Carry)))
	return bs
}

type Conn struct {
	net.Conn
	address string
	wchan   chan Msg
	id      uint64
}

type ClientConn struct {
	net.Conn
	Type  byte
	wchan chan Msg
	Id    uint64
	hub   *MsgHub
}

func (conn *ClientConn) toKey() string {
	return fmt.Sprintf("%d.%d", conn.Id, conn.Type)
}

type routerOper struct {
	destination uint64
	typ         byte
	hubId       int
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
	clients        map[string]*ClientConn
	routerOperChan chan *routerOper
	listener       net.Listener
}

func (hub *MsgHub) processRouterOper() {
	for {
		select {
		case oper, ok := <-hub.routerOperChan:
			if !ok {
				//channel closed
				return
			}
			if oper.destination > hub.maxDest {
				ERROR.Println("invalidate operation")
				continue
			}
			switch oper.oper {
			case oper_add:

				v := (oper.typ << 6) | (RouterMask & byte(oper.hubId))
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

func (conn *ClientConn) sendMsgLoop() {
	defer func() {
		if err := recover(); err != nil {
			ERROR.Println(err)
		}
	}()
	var msg Msg
	var ok bool
	for {
		select {
		case msg, ok = <-conn.wchan:
			if !ok {
				// the channel is closed
				return
			}
			conn.Write(msg.Body())
		}

	}
}

func (hub *MsgHub) AddClient(client *ClientConn) error {
	if client.Type != 1 && client.Type != 2 {
		return NoSuchType
	}
	var routeMsg RouteControlMsg
	key := client.toKey()
	client.wchan = make(chan Msg, 1)
	hub.clients[key] = client
	hub.AddRoute(client.Id, client.Type, hub.id)
	client.hub = hub
	routeMsg.ControlType = AddRouteControlType
	routeMsg.Type = client.Type
	routeMsg.Id = client.Id
	hub.BordcastMsg(&routeMsg)
	go client.sendMsgLoop()
	return nil
}

func (hub *MsgHub) RemoveClient(client *ClientConn) {
	var routeMsg RouteControlMsg
	key := client.toKey()
	delete(hub.clients, key)
	hub.RemoveRoute(client.Id, client.Type)
	routeMsg.ControlType = RemoveRouteControlType
	routeMsg.Type = client.Type
	routeMsg.Id = client.Id
	hub.BordcastMsg(&routeMsg)
	close(client.wchan)
}

func (hub *MsgHub) AddRoute(d uint64, typ byte, id int) error {
	hub.routerOperChan <- &routerOper{destination: d, typ: typ, hubId: id, oper: oper_add}
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
		clients:        make(map[string]*ClientConn, 1024),
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
	Type        uint8  //0x1 0x2
	Id          uint64 //client Id
}

func (msg *RouteControlMsg) Unmarshal(bs []byte) error {
	if len(bs) != msg.Len() {
		return UnknownFramedMsg
	}
	msg.ControlType = int(bs[1])
	msg.Type = bs[2]
	msg.Id = binary.LittleEndian.Uint64(bs[3:])
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
	binary.LittleEndian.PutUint64(bs[3:], msg.Id)
	return bs
}

func (msg *RouteControlMsg) Body() []byte {
	return msg.Bytes()
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

func (c *StringMsg) Body() []byte {
	return c.Bytes()
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
		case msg = <-conn.wchan: //the channel will never closed.
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

func (hub *MsgHub) BordcastMsg(msg Msg) {
	for _, conn := range hub.outgoing {
		if conn != nil {
			conn.wchan <- msg
		}
	}
}

//we only have 2 bits for mask type.
func (hub *MsgHub) LocalDispatch(msg RouteMsg) {
	key1 := fmt.Sprintf("%d.1", msg.Destination())
	key2 := fmt.Sprintf("%d.2", msg.Destination())
	if conn, ok := hub.clients[key1]; ok {
		conn.wchan <- msg
	}

	if conn, ok := hub.clients[key2]; ok {
		conn.wchan <- msg
	}

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
	if msg == nil {
		return
	}
	dest := msg.Destination()
	id := int(hub.router[dest] & RouterMask)
	if id == 0 {
		//TODO: destination is offline
		WARN.Println("TODO: destination is offline")
		return
	}
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
	conn := &Conn{id: uint64(whoami.Who), Conn: c}
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
					err = hub.AddRoute(msg.Id, msg.Type, int(conn.id))
				} else {
					err = hub.RemoveRoute(msg.Id, msg.Type)
				}
				if err != nil {
					ERROR.Println(err)
					return
				}
			}
		case RouteMsgType:
			var msg DeliverMsg
			if n < 13 {
				ERROR.Println(UnknownMsg)
				return
			}
			msg.To = binary.LittleEndian.Uint64(buf[1:])
			l := binary.LittleEndian.Uint32(buf[9:])
			body := make([]byte, l)
			copy(body, buf[13:n])
			var rn uint32 = uint32(n - 13)

			for {
				n, err = conn.Read(body[rn:])
				if err != nil {
					ERROR.Println(err)
					return
				}
				if rn == l {
					break
				}
			}
			msg.Carry = body
			hub.LocalDispatch(&msg)
		}
	}

}

func (hub *MsgHub) ListenAndServe() error {
	var err error
	var c net.Conn
	hub.listener, err = net.Listen("tcp", hub.servAddr)
	if err != nil {
		return err
	}
	for {
		if c, err = hub.listener.Accept(); err != nil {
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
	conn = &Conn{id: uint64(id), address: addr, wchan: make(chan Msg, 1)}
	hub.outgoing[id] = conn
	go hub.outgoingLoop(conn)
	return
}
