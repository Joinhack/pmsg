package pmsg

import (
	"bufio"
	"container/list"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	archiveTaskType = iota
	notifyTaskType  //if user online send the notify control sub task send msg
)

type offlineTask struct {
	taskType int
	id       uint64
	msg      RouteMsg
}

type offlineSubTask struct {
	taskchan chan *offlineTask
	hub      *MsgHub
	cache    map[uint64]*list.List
	baseDir  string
	memBytes uint64
}

type OfflineCenter struct {
	wchan            chan RouteMsg
	hub              *MsgHub
	_wfile           *os.File //just handle
	archiveFile      string
	archiveDir       string
	writer           *bufio.Writer
	lastArchivedTime *time.Time
	_dispatchChan    chan string
	wBytes           uint64
	subTask          []*offlineSubTask
}

func (st *offlineSubTask) sendMsg(id uint64) {
}

func (st *offlineSubTask) writeMsg(msg RouteMsg) {
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
		}
		if task.taskType == archiveTaskType {
			st.writeMsg(task.msg)
		}
		if task.taskType == notifyTaskType {
			st.sendMsg(task.id)
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
		case <-time.After(2 * time.Second):
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
	if now.Unix()-c.lastArchivedTime.Unix() >= 1 || c.wBytes > limit {
		*c.lastArchivedTime = now
		if c.wBytes > 0 {
			err := c.writer.Flush()
			c.wBytes = 0
			if err != nil {
				panic(err)
			}
			c._wfile.Close()
			var target string = filepath.Join(c.archiveDir, "arch", c.archivedFileName())
			os.Link(c.archiveFile, target)
			c.writer = nil
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
	var l uint16 = uint16(len(val))
	var to uint64 = msg.Destination()
	binary.Write(c.writer, binary.LittleEndian, to)
	binary.Write(c.writer, binary.LittleEndian, l)
	binary.Write(c.writer, binary.LittleEndian, val)
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

func newOfflineCenter(num int, hub *MsgHub, path string) (c *OfflineCenter, err error) {
	path, err = filepath.Abs(path)
	if err != nil {
		return
	}
	path = filepath.ToSlash(path)
	var file, dir string
	dir, file = filepath.Split(path)
	if file == "" {
		err = MustbeFilePath
		return
	}
	var stat os.FileInfo
	archDir := filepath.Join(dir, "arch")
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
	center := &OfflineCenter{
		wchan:         make(chan RouteMsg, 1024),
		hub:           hub,
		archiveDir:    archDir,
		archiveFile:   path,
		_dispatchChan: make(chan string, 1),
	}
	err = nil
	c = center
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
		if hub.router[msg.To] != 0 {
			hub.Dispatch(msg)
			continue
		}
		// every sub task manage 10000 id
		sidx := msg.To % 10000
		c.subTask[sidx].taskchan <- &offlineTask{msg: msg, id: msg.Destination(), taskType: archiveTaskType}
	}
	rfile.Close()
	rfile = nil
	if err = os.Remove(path); err != nil {
		ERROR.Println(err)
	}
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
