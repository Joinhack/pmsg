package pmsg

import (
	"container/list"
	"sync"
)

const (
	OnlineState = iota
	OfflineState
)

type StateEvent struct {
	State   byte
	Id      uint64
	DevType byte
}

type Notify func(*StateEvent)

type StateNotifer struct {
	stateTaskNum   int
	stateTaskChans []chan *StateEvent
	stateMutex     []*sync.Mutex
	watchers       []map[uint64]*list.List
}

func newStateNotifer(num int) *StateNotifer {
	chans := make([]chan *StateEvent, num)
	mutexs := make([]*sync.Mutex, num)
	watchers := make([]map[uint64]*list.List, num)
	for i := 0; i < num; i++ {
		chans[i] = make(chan *StateEvent, 1)
		mutexs[i] = &sync.Mutex{}
		watchers[i] = map[uint64]*list.List{}
	}
	notifer := &StateNotifer{
		stateTaskNum:   num,
		stateTaskChans: chans,
		stateMutex:     mutexs,
		watchers:       watchers,
	}
	for i := 0; i < num; i++ {
		go notifer.stateProcess(i)
	}
	return notifer
}

type elementValue struct {
	id     uint64
	notify Notify
}

func (notifer *StateNotifer) AddWatcher(id uint64, notify Notify) *list.Element {
	var l *list.List
	var ok bool
	elem := &elementValue{id: id, notify: notify}
	idx := int(id % uint64(notifer.stateTaskNum))
	notifer.stateMutex[idx].Lock()
	defer notifer.stateMutex[idx].Unlock()
	if l, ok = notifer.watchers[idx][id]; !ok {
		l = list.New()
		notifer.watchers[idx][id] = l
	}
	return l.PushBack(elem)
}

func (notifer *StateNotifer) RemoveWatcher(e *list.Element) {
	elemVal := e.Value.(*elementValue)
	idx := int(elemVal.id % uint64(notifer.stateTaskNum))
	notifer.stateMutex[idx].Lock()
	defer notifer.stateMutex[idx].Unlock()
	notifer.watchers[idx][elemVal.id].Remove(e)
}

func (notifer *StateNotifer) stateProcess(i int) {
	var event *StateEvent
	for {
		select {
		case event = <-notifer.stateTaskChans[i]:
			notifer.stateMutex[i].Lock()
			if l, ok := notifer.watchers[i][event.Id]; ok {
				for e := l.Front(); e != nil; e = e.Next() {
					e.Value.(*elementValue).notify(event)
				}
			}
			notifer.stateMutex[i].Unlock()
		}
	}
}

func (notifer *StateNotifer) notifyLogoff(id uint64, devType byte) {
	idx := int(id % uint64(notifer.stateTaskNum))
	notifer.stateTaskChans[idx] <- &StateEvent{
		Id:      id,
		DevType: devType,
		State:   OfflineState,
	}
}

func (notifer *StateNotifer) notifyLogon(id uint64, devType byte) {
	idx := int(id % uint64(notifer.stateTaskNum))
	notifer.stateTaskChans[idx] <- &StateEvent{
		Id:      id,
		DevType: devType,
		State:   OnlineState,
	}
}
