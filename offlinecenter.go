package pmsg

import (
	"bufio"
	"container/list"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	archiveTaskType  = iota
	dispatchTaskType //if user online send the notify control sub task send msg
)

const (
	MAXIDPERDIR = 10000
)

var (
	DefaultCacheLimit             = 1024 * 1024 * 50
	DefaultMaxItem                = 1024
	DefaultArchedTime      int64  = 2
	DefaultFlushTime              = 30
	DefaultArchedSizeLimit uint64 = 1024 * 1024 * 50
)

type offlineTask struct {
	taskType int
	id       uint64
	msg      RouteMsg
}

type offlineSubTask struct {
	taskchan    chan *offlineTask
	hub         *MsgHub
	_flushMutex *sync.Mutex
	cache       map[uint64]*list.List
	baseDir     string
	cacheBytes  uint64
	cacheLimit  int
	maxItem     int
	id          int
}

type OfflineCenter struct {
	wchan                chan RouteMsg
	hub                  *MsgHub
	_wfile               *os.File //just handle
	archiveFile          string
	archiveDir           string
	writer               *bufio.Writer
	lastArchivedTime     *time.Time
	rangeStart, rangeEnd uint64
	_dispatchChan        chan string
	wBytes               uint64
	subTask              []*offlineSubTask
}

func newOfflineSubTask(hub *MsgHub, mutex *sync.Mutex, path string, id int) *offlineSubTask {
	subtask := &offlineSubTask{
		taskchan:    make(chan *offlineTask, 1),
		hub:         hub,
		_flushMutex: mutex,
		cache:       make(map[uint64]*list.List, 1000),
		baseDir:     path,
		cacheBytes:  0,
		cacheLimit:  DefaultCacheLimit,
		maxItem:     DefaultMaxItem,
		id:          id,
	}
	go subtask.taskLoop()
	return subtask
}

func _readMsg(reader *bufio.Reader) (body []byte, err error) {
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

func (st *offlineSubTask) dispatchMsgFromFile(id uint64) {
	hub := st.hub
	var finfo os.FileInfo
	var err error
	var file *os.File
	path := filepath.Join(st.baseDir, fmt.Sprintf("%d", st.id), fmt.Sprintf("%d", id))
	if finfo, err = os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return
		}
		panic(err)
	} else if finfo.IsDir() {
		panic(fmt.Sprintf("%s must be regard file\n", path))
	}

	if file, err = os.OpenFile(path, os.O_RDONLY, 0644); err != nil {
		panic(err)
	}
	defer func() {
		if file != nil {
			file.Close()
			file = nil
		}
	}()
	reader := bufio.NewReader(file)
	var body []byte
	for {
		if body, err = _readMsg(reader); err != nil {
			if err == io.EOF {
				break
			}
			ERROR.Println(err)
			break
		}
		hub.Dispatch(&DeliverMsg{To: id, Carry: body})
	}
	file.Close()
	file = nil
	if err = os.Remove(path); err != nil {
		ERROR.Println(err)
	}
}

func (st *offlineSubTask) dispatchMsgFromCache(id uint64) {
	l := st.cache[id]
	if l == nil {
		return
	}
	hub := st.hub
	for e := l.Front(); e != nil; e = e.Next() {
		msg := e.Value.(RouteMsg)
		st.cacheBytes -= uint64(len(msg.Body()))
		hub.Dispatch(&DeliverMsg{To: id, Carry: msg.Body()})
	}
	l.Init()
}

func (st *offlineSubTask) dispatchMsg(id uint64) {
	hub := st.hub
	if hub.router[id] == 0 {
		return
	}
	st.dispatchMsgFromFile(id)
	st.dispatchMsgFromCache(id)
}

func (st *offlineSubTask) writeMsg(msg RouteMsg) {
	var l *list.List
	var ok bool
	id := msg.Destination()
	if l, ok = st.cache[id]; !ok {
		l = list.New()
		st.cache[id] = l
	}
	l.PushBack(msg)
	st.cacheBytes += uint64(len(msg.Body()))
	if l.Len() > st.maxItem {
		st._flushMutex.Lock()
		defer st._flushMutex.Unlock()
		st.flush(id, l)
	}
}

func _writeMsg(writer *bufio.Writer, msg RouteMsg) {
	var err error
	body := msg.Body()
	var l uint16 = uint16(len(body))
	if err = binary.Write(writer, binary.LittleEndian, l); err != nil {
		panic(err)
	}
	if err = binary.Write(writer, binary.LittleEndian, body); err != nil {
		panic(err)
	}
}

func (st *offlineSubTask) flushAll() {
	st._flushMutex.Lock()
	defer st._flushMutex.Unlock()
	for id, l := range st.cache {
		if l.Len() > 0 {
			st.flush(id, l)
		}
	}
	if st.cacheBytes != 0 {
		panic(fmt.Sprintf("please check cacheBytes shold be zero, but it's %d", st.cacheBytes))
	}
}

func (st *offlineSubTask) flush(id uint64, l *list.List) {
	path := filepath.Join(st.baseDir, fmt.Sprintf("%d", st.id), fmt.Sprintf("%d", id))
	var file *os.File
	var err error
	if file, err = open(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE); err != nil {
		panic(err)
	}
	writer := bufio.NewWriter(file)
	defer file.Close()
	for e := l.Front(); e != nil; e = e.Next() {
		msg := e.Value.(RouteMsg)
		_writeMsg(writer, msg)
		st.cacheBytes -= uint64(len(msg.Body()))
	}
	writer.Flush()
	l.Init()
}

func (st *offlineSubTask) taskLoop() {
	var task *offlineTask
	var ok bool
	for {
		select {
		case task, ok = <-st.taskchan:
			if !ok {
				//channel is closed
				return
			}
			if task.taskType == archiveTaskType {
				st.writeMsg(task.msg)
			}
			if task.taskType == dispatchTaskType {
				st.dispatchMsg(task.id)
			}
		case <-time.After(time.Duration(DefaultFlushTime) * time.Second):
			st.flushAll()
		}
		if st.cacheBytes > uint64(st.cacheLimit) {
			st.flushAll()
		}
	}
}

func (c *OfflineCenter) archiveLoop() {
	var msg RouteMsg
	var ok bool
	for {
		select {
		case msg, ok = <-c.wchan:
			if ok == false {
				//channel closed
				return
			}
			c.writeMsg(msg)
		case <-time.After(time.Duration(DefaultArchedTime) * time.Second):
		}
		c.processArchive()
	}
}

func (c *OfflineCenter) archivedFileName() string {
	return fmt.Sprintf("%d", time.Now().Unix())
}

func (c *OfflineCenter) processArchive() {
	now := time.Now()
	if c.lastArchivedTime == nil {
		c.lastArchivedTime = &now
		return
	}
	if now.Unix()-c.lastArchivedTime.Unix() >= DefaultArchedTime || c.wBytes > DefaultArchedSizeLimit {
		*c.lastArchivedTime = now
		if c.writer != nil {
			err := c.writer.Flush()
			if err != nil {
				panic(err)
			}
			c.wBytes = 0
			c._wfile.Close()
			var target string = filepath.Join(c.archiveDir, c.archivedFileName())
			os.Link(c.archiveFile, target)
			c.writer = nil
			c._dispatchChan <- target
		}
	}
}

func (c *OfflineCenter) writeMsg(msg RouteMsg) {
	var err error
	val := msg.Body()
	if len(val) == 0 {
		return
	}
	if c.writer == nil {
		c.openWriter()
	}
	if msg.Destination() > uint64(60000) {
		println(msg.Destination())
		panic("sssss")
	}
	var l uint16 = uint16(len(val))
	var to uint64 = msg.Destination()
	if err = binary.Write(c.writer, binary.LittleEndian, to); err != nil {
		panic(err)
	}
	if err = binary.Write(c.writer, binary.LittleEndian, l); err != nil {
		panic(err)
	}
	if err = binary.Write(c.writer, binary.LittleEndian, val); err != nil {
		panic(err)
	}
	c.wBytes += uint64(l)
	if err != nil {
		panic(err)
	}
}

func (c *OfflineCenter) openWriter() {
	var err error
	c._wfile, err = open(c.archiveFile, os.O_WRONLY|os.O_CREATE)
	if err != nil {
		panic(err)
	}
	c.writer = bufio.NewWriter(c._wfile)
}

func newOfflineCenter(srange, erange uint64, hub *MsgHub, path string) (c *OfflineCenter, err error) {
	path, err = filepath.Abs(path)
	if err != nil {
		return
	}
	var stat os.FileInfo
	path = filepath.ToSlash(path)
	if stat, err = os.Stat(path); err != nil {
		return
	}
	if !stat.IsDir() {
		err = MustbeDir
		return
	}
	archDir := filepath.Join(path, "arch")
	stat, err = os.Stat(archDir)
	if err != nil {
		if os.IsNotExist(err) {
			if e := os.Mkdir(archDir, 0755); e != nil {
				err = e
				return
			}
		} else {
			return
		}
	} else if !stat.IsDir() {
		err = MustbeDir
		return
	}
	taskNum := int(erange-srange)/MAXIDPERDIR + 1
	subtasks := make([]*offlineSubTask, taskNum)
	for i := 0; i < taskNum; i++ {
		if err = os.Mkdir(filepath.Join(path, fmt.Sprintf("%d", i)), 0755); err != nil {
			if !os.IsExist(err) {
				panic(err)
			}
		}
	}
	mutex := &sync.Mutex{}
	for i := 0; i < taskNum; i++ {
		subtasks[i] = newOfflineSubTask(hub, mutex, path, i)
	}
	center := &OfflineCenter{
		wchan:         make(chan RouteMsg, 1),
		hub:           hub,
		archiveDir:    archDir,
		archiveFile:   filepath.Join(path, "binlog"),
		_dispatchChan: make(chan string, 1),
		subTask:       subtasks,
		rangeStart:    srange,
		rangeEnd:      erange,
	}
	err = nil
	c = center
	go center.archiveLoop()
	go center.dispatchLoop()
	return
}

func (c *OfflineCenter) dispatch(path string) {
	var reader *bufio.Reader
	var rfile *os.File
	var err error

	hub := c.hub
	if rfile, err = os.OpenFile(path, os.O_RDONLY, 0644); err != nil {
		ERROR.Println(err)
		return
	}
	defer func() {
		if rfile != nil {
			rfile.Close()
			rfile = nil
		}
	}()
	reader = bufio.NewReader(rfile)
	for {
		msg := &DeliverMsg{}
		if msg.To, msg.Carry, err = readRouteMsgBody(reader); err != nil {
			if err == io.EOF {
				break
			}
			ERROR.Println(err)
			return
		}
		//check online table.
		if msg.To > uint64(60000) {
			println(msg.To)
			panic(path)
		}
		if hub.router[msg.To] != 0 {
			hub.Dispatch(msg)
			continue
		}
		// every sub task manage 10000 id
		sidx := (msg.To - c.rangeStart) / MAXIDPERDIR
		//TODO: if sidx out of the subtasks
		c.subTask[sidx].taskchan <- &offlineTask{msg: msg, id: msg.Destination(), taskType: archiveTaskType}
	}
	rfile.Close()
	rfile = nil
	if err = os.Remove(path); err != nil {
		ERROR.Println(err)
	}
}

func (c *OfflineCenter) offlineMsgReplay(id uint64) {
	c.subTask[(id-c.rangeStart)/uint64(MAXIDPERDIR)].taskchan <- &offlineTask{id: id, taskType: dispatchTaskType}
}

func (c *OfflineCenter) dispatchLoop() {
	var path string
	var ok bool
	for {
		select {
		case path, ok = <-c._dispatchChan:
			if !ok {
				// channel is closed
				return
			}
		}
		c.dispatch(path)
	}
}

func (c *OfflineCenter) Archive(msg RouteMsg) {
	c.wchan <- msg
}
