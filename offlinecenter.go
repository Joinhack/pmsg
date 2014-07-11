package pmsg

import (
	"io"
)

type OfflineMsgFilter func(msg RouteMsg) bool

type OfflineCenter interface {
	ProcessMsg(msg RouteMsg)
	AddOfflineMsgFilter(filter OfflineMsgFilter)
	OfflineOutgoingPrepared(conn io.Writer) error
	OfflineMsgReplay(id uint64)
	OfflineIncomingMsg(byte, io.Reader) error
	OfflineIncomingControlMsg(byte, byte, io.Reader) error
	Close()
}
