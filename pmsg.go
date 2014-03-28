package pmsg

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

const (
	StringMsgType  = iota //like ping, framed msg end with \n
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
	oper_clearHub  = 3
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
	_clientsMutex  *sync.Mutex
	listener       net.Listener
}

func (hub *MsgHub) processRouterOper() {
	var oper *routerOper
	var ok bool
	for {
		select {
		case oper, ok = <-hub.routerOperChan:
		}
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
			//TODO: if the id is register in local, we should kick it off. Should add the testcase
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
		case oper_clearHub:
			sid := byte(oper.hubId)
			for i, b := range hub.router {
				if b&RouterMask == sid {
					hub.router[i] = 0
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
	hub._clientsMutex.Lock()
	hub.clients[key] = client
	hub._clientsMutex.Unlock()
	hub.AddRoute(client.Id, client.Type, hub.id)
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
	hub._clientsMutex.Lock()
	delete(hub.clients, key)
	hub._clientsMutex.Unlock()
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
		_clientsMutex:  &sync.Mutex{},
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
	Type        byte   //0x1 0x2
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
	bs[2] = msg.Type
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

func (msg *WhoamIMsg) Body() []byte {
	return msg.Bytes()
}

func (msg *WhoamIMsg) Unmarshal(reader io.Reader) error {
	var bs [3]byte
	if n, err := reader.Read(bs[:]); err != nil {
		return err
	} else if n < msg.Len() {
		return UnknownFramedMsg
	}
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
	msg.bytes = make([]byte, 2+len(content))
	msg.bytes[0] = StringMsgType
	copy(msg.bytes[1:], []byte(content))
	msg.bytes[len(msg.bytes)-1] = '\n'
	return msg
}

func (c *StringMsg) Bytes() []byte {
	return c.bytes
}

func (c *StringMsg) Body() []byte {
	return c.Bytes()
}

var Ping = NewStringMsg("PING")

func (hub *MsgHub) rebuildRemoteRouter(conn *Conn) error {
	hub._clientsMutex.Lock()
	defer hub._clientsMutex.Unlock()
	var routeControlMsg RouteControlMsg
	for _, client := range hub.clients {
		routeControlMsg.ControlType = AddRouteControlType
		routeControlMsg.Type = client.Type
		routeControlMsg.Id = client.Id
		if _, err := conn.Write(routeControlMsg.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

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
			//register self in other hub
			whoami := NewWhoamIMsg(hub.id)
			_, err = conn.Write(whoami.Bytes())
			if err != nil {
				goto ERROR
			}
			//rebuild remote router
			if err = hub.rebuildRemoteRouter(conn); err != nil {
				goto ERROR
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
	ERROR:
		if err != nil {
			ERROR.Println(err)
			//close connection and reconnection later
			conn.Close()
			conn.Conn = nil
			continue
		}
		//if no err clear msg
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
	hub._clientsMutex.Lock()
	defer hub._clientsMutex.Unlock()
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

func (hub *MsgHub) clearRouter(svrid int) {
	hub.routerOperChan <- &routerOper{hubId: svrid, oper: oper_clearHub}
}

func (hub *MsgHub) incomingLoop(c net.Conn) {
	var err error
	var n int
	reader := bufio.NewReader(c)

	defer func() {
		c.Close()
		if err != nil {
			ERROR.Println(err)
		}
	}()

	var whoami WhoamIMsg
	if err = whoami.Unmarshal(reader); err != nil {
		return
	}
	conn := &Conn{id: uint64(whoami.Who), Conn: c}
	if hub.incoming[conn.id] != nil {
		hub.incoming[conn.id].Close()
	}
	hub.incoming[conn.id] = conn
	defer func() { hub.incoming[conn.id] = nil }()
	var msgType byte
	var controlType byte
	for {
		msgType, err = reader.ReadByte()
		if err != nil {
			ERROR.Println("read from incoming error, detail:", err)
			return
		}
		switch msgType {
		case StringMsgType:
			//drop msg, should be ping
			var bs []byte
			bs, err = reader.ReadSlice('\n')
			if err != nil {
				return
			} else {
				INFO.Println(c.RemoteAddr().String(), "said:", string(bs[:len(bs)-1]))
			}
		case ControlMsgType: //cronntrol msg
			if controlType, err = reader.ReadByte(); err != nil {
				return
			}
			switch controlType {
			case AddRouteControlType, RemoveRouteControlType:
				var msg RouteControlMsg
				var buf = make([]byte, msg.Len())
				buf[0] = ControlMsgType
				buf[1] = controlType
				reader.Read(buf[2:])

				if err = msg.Unmarshal(buf); err != nil {
					return
				}
				if msg.ControlType == AddRouteControlType {
					err = hub.AddRoute(msg.Id, msg.Type, int(conn.id))
				} else {
					err = hub.RemoveRoute(msg.Id, msg.Type)
				}
				if err != nil {
					return
				}
			}
		case RouteMsgType:
			var msg DeliverMsg
			if err = binary.Read(reader, binary.LittleEndian, &msg.To); err != nil {
				return
			}
			var l uint32
			if err = binary.Read(reader, binary.LittleEndian, &l); err != nil {
				return
			}
			body := make([]byte, l)
			n = 0
			c := 0
			for c < int(l) {
				if n, err = reader.Read(body[n:]); err != nil {
					return
				}
				c += n
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

func (hub *MsgHub) AddOutgoing(id int, addr string) (err error) {
	if id >= len(hub.outgoing) || id < 0 {
		err = OutofServerRange
		return
	}
	conn := &Conn{id: uint64(id), address: addr, wchan: make(chan Msg, 1)}
	hub.outgoing[id] = conn
	go hub.outgoingLoop(conn)
	return
}
