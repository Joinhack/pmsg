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
	archiveTaskType = iota
	replayTaskType  //if user online send the notify control sub task send msg
)

const (
	MAXIDPERDIR = 10000
)

var (
	DefaultArchDispatchQueueLen        = 10
	DefaultCacheLimit                  = 1024 * 1024 * 50
	DefaultMaxItem                     = 1024 * 4
	DefaultArchivedTime         int64  = 60
	DefaultFlushTime                   = 30
	DefaultArchiveFiles                = (3600 * 3) / int(DefaultArchivedTime)
	DefaultArchivedSizeLimit    uint64 = 1024 * 1024 * 500
)

type OfflineMsgFilter func(msg RouteMsg) bool

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
	wBytes               uint64
	offlineMsgFilters    []OfflineMsgFilter
	archivedFiles        *list.List
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

func (st *offlineSubTask) replayMsgFromFile(id uint64) {
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
		hub.Dispatch(&DeliverMsg{
			To:       id,
			Carry:    body,
			MsgType:  RouteMsgType,
			IsReplay: true,
		})
	}
	file.Close()
	file = nil
	if err = os.Remove(path); err != nil {
		ERROR.Println(err)
	}
}

func (st *offlineSubTask) replayMsgFromCache(id uint64) {
	l := st.cache[id]
	if l == nil {
		return
	}
	hub := st.hub
	for e := l.Front(); e != nil; e = e.Next() {
		msg := e.Value.(RouteMsg)
		st.cacheBytes -= uint64(len(msg.Body()))
		hub.Dispatch(&DeliverMsg{
			To:       id,
			Carry:    msg.Body(),
			MsgType:  RouteMsgType,
			IsReplay: true,
		})
	}
	l.Init()
}

func (st *offlineSubTask) replayMsg(id uint64) {
	hub := st.hub
	if hub.router[id] == 0 {
		return
	}
	st.replayMsgFromFile(id)
	st.replayMsgFromCache(id)
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
	TRACE.Println("flush caches to disk")
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
			if task.taskType == replayTaskType {
				st.replayMsg(task.id)
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
			c.writeMsg2binlog(msg)
			c.dispatchTask(msg)
		case <-time.After(time.Duration(DefaultArchivedTime) * time.Second):
		}
		c.archived()
	}
}

func (c *OfflineCenter) AddOfflineMsgFilter(filter OfflineMsgFilter) {
	c.offlineMsgFilters = append(c.offlineMsgFilters, filter)
}

func (c *OfflineCenter) dispatchTask(msg RouteMsg) {
	hub := c.hub
	to := msg.Destination()
	//check online table.
	if hub.router[to] != 0 {
		hub.Dispatch(msg)
		return
	}
	// every sub task manage 10000 id
	sidx := (to - c.rangeStart) / MAXIDPERDIR
	//TODO: if sidx out of the subtasks
	c.subTask[sidx].taskchan <- &offlineTask{msg: msg, id: msg.Destination(), taskType: archiveTaskType}
}

func (c *OfflineCenter) archivedFileName() string {
	return fmt.Sprintf("%d", time.Now().Unix())
}

func (c *OfflineCenter) archived() {
	now := time.Now()
	if c.lastArchivedTime == nil {
		c.lastArchivedTime = &now
		return
	}
	if now.Unix()-c.lastArchivedTime.Unix() >= DefaultArchivedTime || c.wBytes > DefaultArchivedSizeLimit {
		*c.lastArchivedTime = now
		if c.writer != nil {
			err := c.writer.Flush()
			if err != nil {
				panic(err)
			}
			c.wBytes = 0
			c._wfile.Close()
			targetPath := filepath.Join(c.archiveDir, c.archivedFileName())
			os.Rename(c.archiveFile, targetPath)
			c.writer = nil

			c.archivedFiles.PushBack(targetPath)
			if c.archivedFiles.Len() > DefaultArchiveFiles {
				elem := c.archivedFiles.Front()
				os.Remove(elem.Value.(string))
				c.archivedFiles.Remove(elem)
			}
		}
	}
}

func (c *OfflineCenter) writeMsg2binlog(msg RouteMsg) {
	var err error
	val := msg.Body()
	if len(val) == 0 {
		return
	}
	if c.writer == nil {
		c.openWriter()
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
	archDir := filepath.Join(path, "archived")
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
		wchan:         make(chan RouteMsg, 10),
		hub:           hub,
		archiveDir:    archDir,
		archiveFile:   filepath.Join(path, "binlog"),
		subTask:       subtasks,
		rangeStart:    srange,
		rangeEnd:      erange,
		archivedFiles: list.New(),
	}
	err = nil
	c = center
	go center.archiveLoop()
	return
}

func (c *OfflineCenter) offlineMsgReplay(id uint64) {
	c.subTask[(id-c.rangeStart)/uint64(MAXIDPERDIR)].taskchan <- &offlineTask{id: id, taskType: replayTaskType}
}

func (c *OfflineCenter) Archive(msg RouteMsg) {
	if msg.Type() != OfflineMsgType {
		panic("error msg type")
	}
	for _, filter := range c.offlineMsgFilters {
		if !filter(msg) {
			return
		}
	}
	c.wchan <- msg
}
