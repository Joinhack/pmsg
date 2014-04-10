package pmsg

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var (
	OutofServerRange = errors.New("out of max server limits")

	UnknownFramedMsg = errors.New("unknow framed msg")

	NeedRedirect = errors.New("the client need redirect")

	NoSuchType = errors.New("No such type")

	UnknownMsg = errors.New("unknow msg")

	OutOfHubIdRange = errors.New("Out of hub id range")

	OutOfMaxRange = errors.New("Out of max range")

	TRACE *log.Logger = log.New(os.Stdout, "TRACE ", log.Ldate|log.Ltime|log.Lshortfile)
	WARN  *log.Logger = log.New(os.Stdout, "WARN ", log.Ldate|log.Ltime|log.Lshortfile)
	INFO  *log.Logger = log.New(os.Stdout, "INFO ", log.Ldate|log.Ltime|log.Lshortfile)
	ERROR *log.Logger = log.New(os.Stderr, "ERROR ", log.Ldate|log.Ltime|log.Lshortfile)
)

type Conn struct {
	net.Conn
	wchan chan Msg
	id    uint64
}

type routerOper struct {
	destination uint64
	typ         byte
	hubId       int
	oper        int
	client      Client
}

const (
	RouterMaskBits = 1 << 6
	RouterMask     = 0x3f
	RouterTypeMask = 0xc0
	oper_add       = 0
	oper_remove    = 1
	oper_clearHub  = 3
)

type MsgHub struct {
	id             int
	maxRange       uint64
	router         []byte
	offlineRouter  []byte
	servAddr       string
	outgoing       [RouterMaskBits]*Conn
	incoming       [RouterMaskBits]*Conn
	clients        map[string]Client
	routerOperChan chan *routerOper
	listener       net.Listener
	dropCount      uint64
	_clientsMutex  *sync.Mutex
	*OfflineCenter
	*StateNotifer
}

func toKey(id uint64, typ byte) string {
	return fmt.Sprintf("%d.%d", id, typ)
}

func (hub *MsgHub) notifyOfflineMsgReplay(id uint64) {
	//offline msg is in local
	if hub.OfflineCenter != nil && hub.offlineRouter[id] == byte(hub.id) {
		hub.offlineMsgReplay(id)
	}
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
							hub.notifyLogoff(uint64(i), 0x1)
						}
						if typBit&0x02 > 0 {
							hub.notifyLogoff(uint64(i), 0x2)
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

func (hub *MsgHub) AddRoute(d uint64, typ byte, id int, client Client) {
	hub.routerOperChan <- &routerOper{destination: d, typ: typ, hubId: id, oper: oper_add, client: client}
}

func (hub *MsgHub) RemoveRoute(d uint64, typ byte) {
	hub.routerOperChan <- &routerOper{destination: d, typ: typ, oper: oper_remove}
}

type MsgHubConfig struct {
	Id                int
	MaxRange          uint64
	ServAddr          string
	OfflineRangeStart uint64
	OfflineRangeEnd   uint64
	OfflinePath       string
}

func NewMsgHub(cfg *MsgHubConfig) *MsgHub {
	hub := newMsgHub(cfg.Id, cfg.MaxRange, cfg.ServAddr)
	if offlineCenter, err := newOfflineCenter(cfg.OfflineRangeStart, cfg.OfflineRangeEnd, hub, cfg.OfflinePath); err != nil {
		panic(err)
	} else {
		hub.OfflineCenter = offlineCenter
	}
	if cfg.OfflineRangeEnd > cfg.MaxRange {
		panic(OutOfMaxRange)
	}
	if (cfg.OfflineRangeStart >= cfg.OfflineRangeEnd) || cfg.OfflineRangeStart == 0 {
		return hub
	}
	if err := hub.AddOfflineRouter(cfg.OfflineRangeStart, cfg.OfflineRangeEnd, hub.id); err != nil {
		panic(err)
	}
	return hub
}

func newMsgHub(id int, maxRange uint64, servAddr string) *MsgHub {
	hub := &MsgHub{
		id:             id,
		router:         make([]byte, maxRange),
		offlineRouter:  make([]byte, maxRange),
		routerOperChan: make(chan *routerOper),
		maxRange:       maxRange,
		servAddr:       servAddr,
		clients:        make(map[string]Client, 1024),
		_clientsMutex:  &sync.Mutex{},
		StateNotifer:   newStateNotifer(10),
	}
	go hub.processRouterOper()

	return hub
}

func (hub *MsgHub) AddOfflineRouter(srange, erange uint64, hubid int) error {
	if srange > hub.maxRange || erange > hub.maxRange {
		return OutOfMaxRange
	}
	hub._clientsMutex.Lock()
	for i := srange; i < erange; i++ {
		hub.offlineRouter[i] = byte(hubid)
	}
	hub._clientsMutex.Unlock()
	return nil
}

func (hub *MsgHub) rebuildRemoteRouter(conn *Conn) error {
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

func (hub *MsgHub) sendOfflineRouter(conn *Conn) error {
	if hub.OfflineCenter == nil {
		return nil
	}
	hub._clientsMutex.Lock()
	defer hub._clientsMutex.Unlock()
	var routeControlMsg OfflineRouteControlMsg
	routeControlMsg.ControlType = AddOfflineRouteControlType
	routeControlMsg.HubId = byte(hub.id)
	routeControlMsg.RangeStart = hub.rangeStart
	routeControlMsg.RangeEnd = hub.rangeEnd
	if _, err := conn.Write(routeControlMsg.Bytes()); err != nil {
		return err
	}
	return nil
}

func (hub *MsgHub) outgoingLoop(addr string, id uint64) {
	var err error
	var msg Msg
	conn := &Conn{id: uint64(id), wchan: make(chan Msg, 1)}
	hub.outgoing[id] = conn
	for {
		if conn.Conn == nil {
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
			//rebuild remote router
			if err = hub.rebuildRemoteRouter(conn); err != nil {
				goto ERROR
			}
			if err = hub.sendOfflineRouter(conn); err != nil {
				goto ERROR
			}
		}

		if msg != nil {
			goto WRITE
		}
	GETMSG:
		select {
		case msg = <-conn.wchan: //the channel will never closed.
		case <-time.After(2 * time.Second):
			msg = Ping
		}
		if conn.Conn == nil {
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
			conn.Conn = nil
			continue
		}
		//if no err clear msg
		msg = nil
	}
}

func (hub *MsgHub) bordcastMsg(msg *RouteControlMsg) {
	for _, conn := range hub.outgoing {

		if conn != nil && conn.Conn != nil {
			conn.wchan <- msg
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
	outgoing := hub.outgoing[id]
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

func (hub *MsgHub) localOfflineDispatch(msg *OfflineMsg) {
	hub.Archive(msg)
}

func (hub *MsgHub) remoteOfflineDispatch(id int, msg *OfflineMsg) {
	hub.RemoteDispatch(id, msg)
}

func (hub *MsgHub) offlineDispatch(msg RouteMsg) {
	r := int(hub.offlineRouter[msg.Destination()])
	if r == 0 {
		WARN.Println("TODO: offline router is not set.")
		return
	}
	offline := &OfflineMsg{To: uint64(msg.Destination()), Carry: msg.Body()}
	if r == hub.id {
		hub.localOfflineDispatch(offline)
	} else {
		hub.remoteOfflineDispatch(r, offline)
	}
}

func (hub *MsgHub) Dispatch(msg RouteMsg) {
	if msg == nil {
		return
	}
	dest := msg.Destination()
	id := int(hub.router[dest] & RouterMask)
	if id == 0 {
		hub.offlineDispatch(msg)
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

func readRouteMsgBody(reader *bufio.Reader) (to uint64, body []byte, err error) {
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

func (hub *MsgHub) incomingLoop(c net.Conn) {
	var err error
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
					hub.AddRoute(msg.Id, msg.Type, int(conn.id), nil)
				} else {
					hub.RemoveRoute(msg.Id, msg.Type)
				}
			case AddOfflineRouteControlType:
				var msg OfflineRouteControlMsg
				var buf = make([]byte, msg.Len())
				buf[0] = ControlMsgType
				buf[1] = controlType
				reader.Read(buf[2:])
				if err = msg.Unmarshal(buf); err != nil {
					return
				}
				hub.AddOfflineRouter(msg.RangeStart, msg.RangeEnd, int(msg.HubId))
			}
		case RouteMsgType:
			var msg DeliverMsg
			if msg.To, msg.Carry, err = readRouteMsgBody(reader); err != nil {
				return
			}
			hub.LocalDispatch(&msg)
		case OfflineMsgType:
			var msg OfflineMsg
			if msg.To, msg.Carry, err = readRouteMsgBody(reader); err != nil {
				return
			}
			if msg.To > uint64(10000) {
				panic(msg.To)
			}
			hub.localOfflineDispatch(&msg)
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
	go hub.outgoingLoop(addr, uint64(id))
	return
}
