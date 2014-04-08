package pmsg

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type OfflineCenter struct {
	wchan            chan Msg
	hub              *MsgHub
	_wfile           *os.File //just handle
	archiveFile      string
	archiveDir       string
	writer           *bufio.Writer
	subTaskNum       int
	lastArchivedTime *time.Time
	wBytes           uint64
}

func (c *OfflineCenter) archiveLoop() {
	var msg Msg
	var ok bool
	for {
		select {
		case msg, ok = <-c.wchan:
			if ok == false {
				//channel closed
				return
			}
			c.writeItem(msg.Body())
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

func (c *OfflineCenter) writeItem(item []byte) {
	var err error
	if len(item) == 0 {
		return
	}
	if c.writer == nil {
		c.openWriter()
	}
	var l uint32 = uint32(len(item))
	binary.Write(c.writer, binary.LittleEndian, l)
	binary.Write(c.writer, binary.LittleEndian, item)
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
		wchan:       make(chan Msg, 1024),
		hub:         hub,
		subTaskNum:  num,
		archiveDir:  archDir,
		archiveFile: path,
	}
	err = nil
	c = center
	return
}

func (c *OfflineCenter) Archive(msg Msg) {
	c.wchan <- msg
}
