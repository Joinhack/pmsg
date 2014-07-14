package pmsg

import (
	"encoding/binary"
)

const (
	_              = iota
	StringMsgType  //like ping, framed msg end with \n
	ControlMsgType //framed msg
	RouteMsgType
	TempRouteMsgType //do not save
	OfflineMsgType   //another route msg
)

const (
	_ = iota
	WhoamIControlType
	WhoamIAckControlType
	AddRouteControlType
	RemoveRouteControlType
	OfflineControlType
)

var (
	Ping = NewStringMsg("PING")
)

type Msg interface {
	Bytes() []byte
	Body() []byte
}

type RouteMsg interface {
	Msg
	Type() int
	SetType(t int)
	Destination() uint32
}

type DeliverMsg struct {
	RouteMsg
	IsReplay bool
	MsgType  byte
	To       uint32
	Carry    []byte
}

func (msg *DeliverMsg) SetType(t int) {
	msg.MsgType = byte(t)
}

func (msg *DeliverMsg) Type() int {
	return int(msg.MsgType)
}

func (msg *DeliverMsg) Destination() uint32 {
	return msg.To
}

func (msg *DeliverMsg) Body() []byte {
	return msg.Carry
}

func NewDeliverMsg(typ byte, to uint32, carry []byte) *DeliverMsg {
	if typ != RouteMsgType && typ != TempRouteMsgType && typ != OfflineMsgType {
		panic(UnknownMsg)
	}
	return &DeliverMsg{MsgType: typ, To: to, Carry: carry}
}

func (msg *DeliverMsg) Bytes() []byte {
	bs := make([]byte, 7+len(msg.Carry))
	copy(bs[7:], msg.Carry)

	bs[0] = msg.MsgType
	binary.LittleEndian.PutUint32(bs[1:], msg.Destination())
	binary.LittleEndian.PutUint16(bs[5:], uint16(len(msg.Carry)))
	return bs
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
	Id          uint32 //client Id
}

func (msg *RouteControlMsg) Unmarshal(bs []byte) error {
	if len(bs) != msg.Len() {
		return UnknownFramedMsg
	}
	msg.ControlType = int(bs[1])
	msg.Type = bs[2]
	msg.Id = binary.LittleEndian.Uint32(bs[3:])
	return nil
}

func NewWhoamIMsg(who int) *WhoamIMsg {
	msg := &WhoamIMsg{ControlType: WhoamIControlType, Who: who}
	return msg
}

func NewWhoamIAckMsg(who int) *WhoamIMsg {
	msg := &WhoamIMsg{ControlType: WhoamIAckControlType, Who: who}
	return msg
}

func (msg *RouteControlMsg) Len() int {
	return 7
}

func (msg *RouteControlMsg) Bytes() []byte {
	bs := make([]byte, msg.Len())
	bs[0] = ControlMsgType
	bs[1] = byte(msg.ControlType)
	bs[2] = msg.Type
	binary.LittleEndian.PutUint32(bs[3:], msg.Id)
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

type OfflineRouteControlMsg struct {
	ControlType          int
	HubId                byte
	RangeStart, RangeEnd uint64
}

func (msg *OfflineRouteControlMsg) Bytes() []byte {
	bs := make([]byte, msg.Len())
	bs[0] = ControlMsgType
	bs[1] = byte(msg.ControlType)
	bs[2] = msg.HubId
	binary.LittleEndian.PutUint64(bs[3:], msg.RangeStart)
	binary.LittleEndian.PutUint64(bs[11:], msg.RangeEnd)
	return bs
}

func (msg *OfflineRouteControlMsg) Unmarshal(bs []byte) error {
	if len(bs) != msg.Len() {
		return UnknownFramedMsg
	}
	msg.ControlType = int(bs[1])
	msg.HubId = bs[2]
	msg.RangeStart = binary.LittleEndian.Uint64(bs[3:])
	msg.RangeEnd = binary.LittleEndian.Uint64(bs[11:])
	return nil
}

func (msg *OfflineRouteControlMsg) Body() []byte {
	return msg.Bytes()
}

func (msg *OfflineRouteControlMsg) Len() int {
	return 19
}
