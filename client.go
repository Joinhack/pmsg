package pmsg

import (
	"fmt"
	"net"
)

type RedirectError struct {
	HubId int
}

func (e *RedirectError) Error() string {
	return fmt.Sprintf("redirect to hub %d", e.HubId)
}

type Client interface {
	Id() uint64
	Type() byte
	SendMsg(msg Msg)
	Kickoff() //invoke by hub.
	Redirect(hubid int)
	IsKickoff() bool
}

type SimpleClientConn struct {
	net.Conn
	isKickoff  bool
	ClientId   uint64
	ClientType byte
	Wchan      chan Msg
}

func NewSimpleClientConn(conn net.Conn, id uint64, typ byte) *SimpleClientConn {
	sconn := &SimpleClientConn{
		Conn:       conn,
		ClientId:   id,
		ClientType: typ,
		isKickoff:  false,
		Wchan:      make(chan Msg, 1),
	}
	go sconn.SendMsgLoop()
	return sconn
}

func (conn *SimpleClientConn) IsKickoff() bool {
	return conn.isKickoff
}

func (conn *SimpleClientConn) CloseWchan() {
	close(conn.Wchan)
}

func (conn *SimpleClientConn) Kickoff() {
	conn.isKickoff = true
	defer func() {
		if err := recover(); err != nil {
			ERROR.Println(err)
		}
	}()
	conn.Conn.Close()
}

func (conn *SimpleClientConn) Redirect(i int) {
}

func (conn *SimpleClientConn) Id() uint64 {
	return conn.ClientId
}

func (conn *SimpleClientConn) Type() byte {
	return conn.ClientType
}

func (conn *SimpleClientConn) SendMsg(msg Msg) {
	if msg == nil {
		return
	}
	conn.Wchan <- msg
}

func (conn *SimpleClientConn) SendMsgLoop() {
	var msg Msg
	var ok bool
	var err error
	defer func() {
		if err := recover(); err != nil {
			ERROR.Println(err)
		}
	}()

	for {
		select {
		case msg, ok = <-conn.Wchan:
			if !ok {
				// the channel is closed
				return
			}
			if _, err = conn.Write(msg.Body()); err != nil {
				ERROR.Println(err)
				conn.Conn.Close()
				return
			}
		}

	}
}
