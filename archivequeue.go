package pmsg

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

var (
	MustbeFilePath = errors.New("Must be file path")
	MustbeDir      = errors.New("Must be dir")
)

type ArchiveQueue struct {
	Path         string
	Dir          string
	WriteBytes   int64
	writerChan   chan []byte
	running      bool
	_writer_file *os.File //just handle
	writer       *bufio.Writer
	lastArchTime *time.Time
}

func (queue *ArchiveQueue) Push(b []byte) {
	queue.writerChan <- b
}

func (q *ArchiveQueue) writeItem(item []byte) {
	var err error
	if len(item) == 0 {
		return
	}
	if q.writer == nil {
		q.openWriter()
	}
	var l int64 = int64(len(item))
	binary.Write(q.writer, binary.LittleEndian, l)
	binary.Write(q.writer, binary.LittleEndian, item)
	if err != nil {
		panic(err)
	}
	q.WriteBytes += 8 + l
}

func (q *ArchiveQueue) openWriter() {
	var err error
	q._writer_file, err = open(q.Path, os.O_WRONLY|os.O_CREATE)
	if err != nil {
		panic(err)
	}
	q.writer = bufio.NewWriter(q._writer_file)
}

func (q *ArchiveQueue) pushin() {
	var item []byte
	for q.running {
		select {
		case item = <-q.writerChan:
			q.writeItem(item)
		}
		q.arch()
	}
}

func (q *ArchiveQueue) archFileName() string {
	return fmt.Sprintf("%d", time.Now().Unix())
}

const (
	limit = 1024 * 1024 * 1024
)

func (q *ArchiveQueue) arch() {
	now := time.Now()
	if q.lastArchTime == nil {
		q.lastArchTime = &now
		return
	}
	if now.Unix()-q.lastArchTime.Unix() >= 1 || q.WriteBytes > limit {
		*q.lastArchTime = now
		if q.WriteBytes > 0 {
			err := q.writer.Flush()
			q.WriteBytes = 0
			if err != nil {
				panic(err)
			}
			q._writer_file.Close()
			os.Link(q.Path, filepath.Join(q.Dir, "arch", q.archFileName()))
			q.writer = nil
		}
	}

}

func open(path string, flag int) (*os.File, error) {
	var stat os.FileInfo
	var f *os.File
	var err error
	if stat, err = os.Stat(path); err == nil {
		if stat.IsDir() {
			panic("meta data must be reglar file")
		}
		if f, err = os.OpenFile(path, flag, 0644); err != nil {
			return nil, err
		}
	} else if os.IsNotExist(err) {
		if f, err = os.OpenFile(path, flag|os.O_CREATE, 0644); err != nil {
			return nil, err
		}
	}
	return f, nil
}

func createArchiveQueue(p string) (queue *ArchiveQueue, err error) {
	q := &ArchiveQueue{}
	p, err = filepath.Abs(p)
	if err != nil {
		return
	}
	var file string
	q.Path = filepath.ToSlash(p)
	q.Dir, file = filepath.Split(q.Path)
	var stat os.FileInfo
	archDir := filepath.Join(q.Dir, "arch")
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

	if file == "" {
		err = MustbeFilePath
		return
	}
	q.writerChan = make(chan []byte, 10)
	q.running = true
	go q.pushin()
	queue = q
	err = nil
	return
}
