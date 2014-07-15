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
	"sync/atomic"
	"time"
)

const OneConnectionForPeer = true

var (
	OutofServerRange = errors.New("out of max server limits")

	UnknownFramedMsg = errors.New("unknow framed msg")

	NeedRedirect = errors.New("the client need redirect")

	NoSuchType = errors.New("No such type")

	UnknownMsg = errors.New("unknow msg")

	OutOfHubIdRange = errors.New("Out of hub id range")

	OutOfMaxRange = errors.New("Out of max range")

	DuplicateHubId = errors.New("Dunplicate Hub Id")

	TRACE *log.Logger = log.New(os.Stdout, "TRACE ", log.Ldate|log.Ltime|log.Lshortfile)
	WARN  *log.Logger = log.New(os.Stdout, "WARN ", log.Ldate|log.Ltime|log.Lshortfile)
	INFO  *log.Logger = log.New(os.Stdout, "INFO ", log.Ldate|log.Ltime|log.Lshortfile)
	ERROR *log.Logger = log.New(os.Stderr, "ERROR ", log.Ldate|log.Ltime|log.Lshortfile)
)

const (
	conn_closed      = 0
	conn_half_closed = 1
	conn_ok          = 2
)

type Conn interface {
	Send(Msg)
	Close()
	State() byte
}

type SimplexConn struct {
	net.Conn
	state byte //the connection state
	wchan chan Msg
	id    int
}

func (c *SimplexConn) State() byte {
	return c.state
}

func (c *SimplexConn) Send(msg Msg) {
	if c.state == conn_ok {
		c.wchan <- msg
	}
}

func (c *SimplexConn) Close() {
	c.state = conn_closed
	defer func() {
		if e := recover(); e != nil {
			WARN.Println(e)
		}
	}()
	if c.Conn != nil {
		c.Conn.Close()
	}
}

type DuplexConn struct {
	net.Conn
	state byte //the connection state
	wchan chan Msg
	id    int
	q     chan byte
	mtx   *sync.Mutex
}

func (c *DuplexConn) Send(msg Msg) {
	if c.state == conn_ok {
		c.wchan <- msg
	}
}

func (c *DuplexConn) Close() {
	defer func() {
		if e := recover(); e != nil {
			WARN.Println(e)
		}
	}()
	if c.state == conn_ok {
		c.state = conn_half_closed
	} else {
		c.state = conn_closed
	}
	if c.Conn != nil {
		c.Conn.Close()
	}
	if c.q != nil {
		close(c.q)
		c.q = nil	
	}
}

func (c *DuplexConn) State() byte {
	return c.state
}

type routerOper struct {
	destination uint32
	typ         byte
	hubId       int
	oper        int
	client      Client
}

const (
	MaxRouter      = 1 << 6
	RouterMask     = 0x3f
	RouterTypeMask = 0xc0
	oper_add       = 0
	oper_remove    = 1
	oper_clearHub  = 3
)

type MsgHub struct {
	id             int
	maxRange       uint32
	router         []byte
	servAddr       string
	outgoing       [MaxRouter]Conn //just for write, when one conection for communication for peer reuse it
	incoming       [MaxRouter]Conn //just for read
	clients        map[string]Client
	routerOperChan chan *routerOper
	listener       net.Listener
	dropCount      uint64
	running        bool
	_clientsMutex  *sync.Mutex
	OfflineCenter
	*StateNotifer
}

func toKey(id uint32, typ byte) string {
	return fmt.Sprintf("%d.%d", id, typ)
}

func (hub *MsgHub) notifyOfflineMsgReplay(id uint32) {
	//offline msg is in local
	if hub.OfflineCenter != nil {
		hub.OfflineMsgReplay(id)
	}
}

func (hub *MsgHub) processRouterOper() {
	var oper *routerOper
	var ok bool
	defer func() {
		if e := recover(); e != nil {
			ERROR.Println(e)
		}
	}()
	for hub.running {
		select {
		case oper, ok = <-hub.routerOperChan:
		}
		if !ok {
			//channel closed
			return
		}
		if oper.destination > hub.maxRange {
			ERROR.Println("invalidate operation")
			continue
		}

		switch oper.oper {
		case oper_add:
			v := (oper.typ << 6) | (RouterMask & byte(oper.hubId))
			//Kickoff
			r := hub.router[oper.destination]
			key := toKey(oper.destination, oper.typ)
			var logonEvent bool = true
			if r != 0 {
				var client Client
				var ok bool
				hubid := r & RouterMask
				if int(hubid) == hub.id {
					client, ok = hub.clients[key]
					if ok {
						client.Kickoff()
						logonEvent = false
					}
				}

				//it different dev registed in different hub, I hope i always in the same hub
				if int(hubid) != oper.hubId && oper.typ<<6 != r&RouterTypeMask {
					if oper.client != nil {
						//it never happen I think
						WARN.Println("redirect client")
						oper.client.Redirect(int(hubid))
						continue
					} else {
						WARN.Println("come from other hub, I don't know how to do")
					}
				}
			}
			if oper.client != nil {
				hub._clientsMutex.Lock()
				hub.clients[key] = oper.client
				hub._clientsMutex.Unlock()
			}
			if logonEvent {
				hub.notifyLogon(oper.destination, oper.typ)
			}
			hub.router[oper.destination] = v
			hub.notifyOfflineMsgReplay(oper.destination)
		case oper_remove:
			r := hub.router[oper.destination]
			if r != 0 {
				typ := RouterTypeMask & r
				v := typ & ^(oper.typ << 6)
				hubid := r & RouterMask
				if int(hubid) == hub.id {
					key := toKey(oper.destination, oper.typ)
					delete(hub.clients, key)
				}
				hub.notifyLogoff(oper.destination, oper.typ)
				if v == 0 {
					hub.router[oper.destination] = 0
				} else {
					hub.router[oper.destination] = (hubid) | v
				}
			}
		case oper_clearHub:
			sid := byte(oper.hubId)
			for i, b := range hub.router {
				if b&RouterMask == sid {
					if b != 0 {
						typBit := (b & RouterMask) >> 6
						if typBit&0x01 > 0 {
							hub.notifyLogoff(uint32(i), 0x1)
						}
						if typBit&0x02 > 0 {
							hub.notifyLogoff(uint32(i), 0x2)
						}
					}
					hub.router[i] = 0
				}
			}
		}
	}
}

func (hub *MsgHub) AddClient(client Client) error {
	var routeMsg RouteControlMsg
	clientId := client.Id()
	clientType := client.Type()
	r := hub.router[client.Id()]
	if r != 0 && int(r&RouterMask) != hub.id && r&RouterTypeMask != client.Type()<<6 {
		return &RedirectError{int(r & RouterMask)}
	}
	hub.AddRoute(clientId, clientType, hub.id, client)
	routeMsg.ControlType = AddRouteControlType
	routeMsg.Type = clientType
	routeMsg.Id = clientId
	hub.bordcastMsg(&routeMsg)
	return nil
}

func (hub *MsgHub) RemoveClient(client Client) {
	var routeMsg RouteControlMsg
	clientId := client.Id()
	clientType := client.Type()
	if client.IsKickoff() {
		return
	}
	hub.RemoveRoute(clientId, clientType)
	routeMsg.ControlType = RemoveRouteControlType
	routeMsg.Type = clientType
	routeMsg.Id = clientId
	hub.bordcastMsg(&routeMsg)
}

func (hub *MsgHub) AddRoute(d uint32, typ byte, id int, client Client) {
	hub.routerOperChan <- &routerOper{destination: d, typ: typ, hubId: id, oper: oper_add, client: client}
}

func (hub *MsgHub) RemoveRoute(d uint32, typ byte) {
	hub.routerOperChan <- &routerOper{destination: d, typ: typ, oper: oper_remove}
}

type MsgHubFileStoreOfflineCenterConfig struct {
	Id                int
	MaxRange          uint32
	ServAddr          string
	OfflineRangeStart uint64
	OfflineRangeEnd   uint64
	OfflinePath       string
}

func NewMsgHubWithFileStoreOfflineCenter(cfg *MsgHubFileStoreOfflineCenterConfig) *MsgHub {
	hub := NewMsgHub(cfg.Id, cfg.MaxRange, cfg.ServAddr)
	if c, err := NewFileStoreOffline(cfg.OfflineRangeStart, cfg.OfflineRangeEnd, hub, cfg.OfflinePath); err != nil {
		panic(err)
	} else {
		hub.OfflineCenter = c
	}
	return hub
}

func NewMsgHub(id int, maxRange uint32, servAddr string) *MsgHub {
	hub := &MsgHub{
		id:             id,
		router:         make([]byte, maxRange),
		routerOperChan: make(chan *routerOper),
		maxRange:       maxRange,
		running:        true,
		servAddr:       servAddr,
		clients:        make(map[string]Client, 1024),
		_clientsMutex:  &sync.Mutex{},
		StateNotifer:   newStateNotifer(StateNotiferNum),
	}

	if OneConnectionForPeer {
		for i := 0; i < len(hub.outgoing); i++ {
			hub.outgoing[i] = &DuplexConn{wchan: make(chan Msg), id: i, mtx: &sync.Mutex{}}
		}
	}

	go hub.processRouterOper()
	return hub
}

func (hub *MsgHub) ClientsLock() {
	hub._clientsMutex.Lock()
}

func (hub *MsgHub) ClientsUnlock() {
	hub._clientsMutex.Unlock()
}

func (hub *MsgHub) MaxRange() uint32 {
	return hub.maxRange
}

func (hub *MsgHub) rebuildRemoteRouter(conn io.Writer) error {
	hub._clientsMutex.Lock()
	defer hub._clientsMutex.Unlock()
	var routeControlMsg RouteControlMsg
	for _, client := range hub.clients {
		routeControlMsg.ControlType = AddRouteControlType
		routeControlMsg.Type = client.Type()
		routeControlMsg.Id = client.Id()
		if _, err := conn.Write(routeControlMsg.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

func (hub *MsgHub) outgoingLoop(addr string, id int) {
	var err error
	var msg Msg
	var ok bool
	conn := &SimplexConn{id: id, wchan: make(chan Msg, 1)}
	hub.outgoing[id] = conn
	for hub.running {
		if conn.state == conn_closed {
			conn.Conn, err = net.Dial("tcp", addr)
			if err != nil {
				ERROR.Println("connection to", addr, "fail, retry after 2 sec.")
				goto GETMSG
			}
			//register self in other hub
			whoami := NewWhoamIMsg(hub.id)
			_, err = conn.Write(whoami.Bytes())
			if err != nil {
				goto ERROR
			}
			conn.state = conn_ok
			//rebuild remote router
			if err = hub.rebuildRemoteRouter(conn); err != nil {
				goto ERROR
			}
			if hub.OfflineCenter != nil {
				if err = hub.OfflineOutgoingPrepared(conn); err != nil {
					goto ERROR
				}
			}
		}

		if msg != nil {
			goto WRITE
		}
	GETMSG:
		select {
		case msg, ok = <-conn.wchan:
			if !ok {
				return
			}
		case <-time.After(2 * time.Second):
			msg = Ping
		}
		if conn.state == conn_closed {
			//if the connection is offline, I will drop msg.
			continue
		}
	WRITE:
		_, err = conn.Write(msg.Bytes())
	ERROR:
		if err != nil {
			ERROR.Println(err)
			//close connection and reconnection later
			conn.Close()
			continue
		}
		//if no err clear msg
		msg = nil
	}
}

func (hub *MsgHub) bordcastMsg(msg *RouteControlMsg) {
	for _, conn := range hub.outgoing {
		if conn != nil {
			conn.Send(msg)
		}
	}
}

//we only have 2 bits for mask type.
func (hub *MsgHub) LocalDispatch(msg RouteMsg) {
	defer func() {
		if err := recover(); err != nil {
			ERROR.Println(err)
		}
	}()
	key1 := fmt.Sprintf("%d.1", msg.Destination())
	key2 := fmt.Sprintf("%d.2", msg.Destination())
	var conn Client
	var ok1 = false
	var ok2 = false
	hub._clientsMutex.Lock()
	conn, ok1 = hub.clients[key1]
	hub._clientsMutex.Unlock()
	if ok1 {
		conn.SendMsg(msg)
	}

	hub._clientsMutex.Lock()
	conn, ok2 = hub.clients[key2]
	hub._clientsMutex.Unlock()

	if ok2 {
		conn.SendMsg(msg)
	}
	if !ok1 && !ok2 {
		atomic.AddUint64(&hub.dropCount, 1)
	}
}

func (hub *MsgHub) RemoteDispatch(id int, msg RouteMsg) {
	var outgoing Conn
	outgoing = hub.outgoing[id]
	defer func() {
		if err := recover(); err != nil {
			//TODO: if outgoing write channel close,  should remove the outgoing from hub
			ERROR.Println(err)
		}
	}()
	if outgoing != nil {
		outgoing.Send(msg)
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
		if msg.Type() == TempRouteMsgType {
			//drop packet.
			return
		}
		if hub.OfflineCenter != nil {
			hub.OfflineCenter.ProcessMsg(msg)
		} else {
			WARN.Println("No offline center configured.")
		}
		return
	}
	if hub.id == id {
		hub.LocalDispatch(msg)
	} else {
		hub.RemoteDispatch(id, msg)
	}
}

func (hub *MsgHub) clearRouter(svrid int) {
	hub.routerOperChan <- &routerOper{hubId: svrid, oper: oper_clearHub}
}

func readRouteMsgBody(reader io.Reader) (to uint32, body []byte, err error) {
	if err = binary.Read(reader, binary.LittleEndian, &to); err != nil {
		return
	}
	var l uint16
	if err = binary.Read(reader, binary.LittleEndian, &l); err != nil {
		return
	}
	body = make([]byte, l)
	var n = 0
	c := 0
	for c < int(l) {
		if n, err = reader.Read(body[n:]); err != nil {
			return
		}
		c += n
	}
	return
}

func (hub *MsgHub) msgProc(id int, reader *bufio.Reader) (err error) {
	var msgType byte
	var controlType byte

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
			INFO.Println(id, "said:", string(bs[:len(bs)-1]))
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
				hub.AddRoute(msg.Id, msg.Type, id, nil)
			} else {
				hub.RemoveRoute(msg.Id, msg.Type)
			}
		case OfflineControlType:
			if err = hub.OfflineIncomingControlMsg(ControlMsgType, OfflineMsgType, reader); err != nil {
				return
			}
		}
	case RouteMsgType, TempRouteMsgType:
		var msg DeliverMsg
		if msg.To, msg.Carry, err = readRouteMsgBody(reader); err != nil {
			return
		}
		msg.MsgType = msgType
		hub.LocalDispatch(&msg)
	case OfflineMsgType:
		if err = hub.OfflineIncomingMsg(OfflineMsgType, reader); err != nil {
			return
		}
	default:
		err = UnknownMsg
	}
	return nil
}

func (hub *MsgHub) incomingLoop(c net.Conn) {
	var err error

	defer func() {
		if e := recover(); e != nil {
			ERROR.Println(e)
		}
		c.Close()
		if err != nil && err != io.EOF {
			ERROR.Println(err)
		}
	}()
	var whoami WhoamIMsg
	whoamiSli := make([]byte, whoami.Len())

	if _, err = c.Read(whoamiSli); err != nil {
		return
	}
	if err = whoami.Unmarshal(whoamiSli); err != nil {
		return
	}
	conn := &SimplexConn{id: int(whoami.Who), Conn: c}
	if hub.incoming[conn.id] != nil {
		hub.incoming[conn.id].Close()
	}
	hub.incoming[conn.id] = conn
	conn.state = conn_ok
	defer func() { hub.incoming[conn.id] = nil }()
	reader := bufio.NewReader(conn)
	for hub.running {
		if err = hub.msgProc(conn.id, reader); err != nil {
			hub.clearRouter(conn.id)
			return
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
	for hub.running {
		if c, err = hub.listener.Accept(); err != nil {
			return err
		}
		if OneConnectionForPeer {
			go hub.inProc(c)
		} else {
			go hub.incomingLoop(c)
		}
	}
	return nil
}

func (hub *MsgHub) inLoop(c *DuplexConn) {
	var err error
	defer func() {
		WARN.Println(fmt.Sprintf("hub[%d] in loop quit. connect id:%d", hub.id, c.id))
		if e := recover(); e != nil {
			ERROR.Println(e)
		}
		c.mtx.Lock()
		c.Close()
		c.mtx.Unlock()
		if err != nil && err != io.EOF {
			ERROR.Println(err)
		}
	}()
	reader := bufio.NewReader(c.Conn)
	for {
		if err = hub.msgProc(c.id, reader); err != nil {
			hub.clearRouter(c.id)
			return
		}
	}
}

func (hub *MsgHub) outLoop(c *DuplexConn) {
	var err error
	defer func() {
		WARN.Println(fmt.Sprintf("hub[%d] out loop quit. connect id:%d", hub.id, c.id))
		c.mtx.Lock()
		c.Close()
		c.mtx.Unlock()
		if err != nil && err != io.EOF {
			ERROR.Println(err)
		}
	}()
	//rebuild remote router
	if err = hub.rebuildRemoteRouter(c); err != nil {
		return
	}
	if hub.OfflineCenter != nil {
		if err = hub.OfflineOutgoingPrepared(c); err != nil {
			return
		}
	}

	var msg Msg
	for {
		var ok bool
		select {
		case msg, ok = <-c.wchan: //the channel will never closed.
			if !ok {
				return
			}
		case <-c.q:
			return
		case <-time.After(2 * time.Second):
			msg = Ping
		}
		if _, err = c.Write(msg.Bytes()); err != nil {
			return
		}
	}
}

func (hub *MsgHub) outProc(id int, addr string) {
	var err error
	var wait time.Duration
	var conn *DuplexConn
	for hub.running {
		var c net.Conn
		var whoamiAck WhoamIMsg
		var whoami *WhoamIMsg
		wait = 300
		//if connection is ok or handleshake, just wait a moment.
		//if in handleshake wait finish the handleshake.
		conn = hub.outgoing[id].(*DuplexConn)
		conn.mtx.Lock()
		if conn.state != conn_closed {
			if conn.state == conn_ok {
				wait = 2000
			}
			conn.mtx.Unlock()
			time.Sleep(wait * time.Millisecond)
			continue
		}
		conn.mtx.Unlock()
		whoamiSli := make([]byte, whoamiAck.Len())
		c, err = net.Dial("tcp", addr)
		if err != nil {
			//if the peer is not listen. need more time.
			wait = 2000
			goto RETRY
		}
		//handleshake with the peer.
		whoami = NewWhoamIMsg(hub.id)
		_, err = c.Write(whoami.Bytes())
		if err != nil {
			goto RETRY
		}
		//wait for the peer give the ack if every thing is fine.
		//if the peer found already have a connection for this peer, should close the connection.
		if _, err = c.Read(whoamiSli); err != nil {
			c.Close()
			goto RETRY
		}
		if err = whoamiAck.Unmarshal(whoamiSli); err != nil {
			c.Close()
			goto RETRY
		}
		if whoamiAck.Who != id {
			panic("can't be happend.")
		}
		//dual check
		conn.mtx.Lock()
		if conn.state == conn_ok {
			if byte(id) > byte(hub.id) {
				conn.mtx.Unlock()
				c.Close()
				continue
			} else {
				conn.Close()
			}
		}
		conn.Conn = c
		conn.q = make(chan byte)
		conn.state = conn_ok
		conn.mtx.Unlock()
		go hub.inLoop(conn)
		hub.outLoop(conn)
		continue
	RETRY:
		time.Sleep(time.Duration(wait) * time.Millisecond)
		ERROR.Println("connection to", addr, "fail, retry later.")
	}
}

func (hub *MsgHub) inProc(c net.Conn) {
	var err error

	defer func() {
		c.Close()
		if err != nil {
			ERROR.Println(err)
		}
	}()
	var whoami WhoamIMsg
	whoamiSli := make([]byte, whoami.Len())
	if _, err = c.Read(whoamiSli); err != nil {
		return
	}
	if err = whoami.Unmarshal(whoamiSli); err != nil {
		return
	}

	conn := hub.outgoing[whoami.Who].(*DuplexConn)
	conn.mtx.Lock()
	if conn.state != conn_closed {
		conn.mtx.Unlock()
		return
	}
	conn.mtx.Unlock()
	if _, err = c.Write(NewWhoamIAckMsg(hub.id).Bytes()); err != nil {
		return
	}
	//dual check
	conn.mtx.Lock()
	if conn != nil && conn.state == conn_ok {
		//drop the smaller id in active
		if byte(conn.id) < byte(hub.id) {
			conn.mtx.Unlock()
			return
		} else {
			conn.Close()
		}
	}
	conn.Conn = c
	conn.q = make(chan byte)
	conn.state = conn_ok
	conn.mtx.Unlock()
	go hub.outLoop(conn)
	hub.inLoop(conn)
}

func (hub *MsgHub) AddOtherHub(id int, addr string) (err error) {
	if id >= len(hub.outgoing) || id < 0 {
		err = OutofServerRange
		return
	}
	if id == hub.id {
		return DuplicateHubId
	}
	if OneConnectionForPeer {
		go hub.outProc(id, addr)
	} else {
		go hub.outgoingLoop(addr, id)
	}
	return
}

func (hub *MsgHub) Close() {
	INFO.Println("hub:", hub.id, "close.")
	hub.running = false
	close(hub.routerOperChan)
	if hub.listener != nil {
		hub.listener.Close()
	}

	for _, c := range hub.incoming {
		if c != nil {
			c.Close()
		}
	}
	for _, c := range hub.outgoing {
		if c != nil {
			c.Close()
		}
	}
	for _, c := range hub.clients {
		if c != nil {
			c.Kickoff()
		}
	}
	if hub.StateNotifer != nil {
		hub.StateNotifer.Close()
	}

	if hub.OfflineCenter != nil {
		hub.OfflineCenter.Close()
	}

}
