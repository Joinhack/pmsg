package pmsg

import (
	"testing"
	"time"
)

func TestBitOper(t *testing.T) {
	hub := NewMsgHub(1, 1024*1024, ":9999")
	hub.AddRoute(1, 1, 2)
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

	hub.AddRoute(1, 1, 2)
	time.Sleep(2 * time.Millisecond)

	hub.AddRoute(1, 2, 9)
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
