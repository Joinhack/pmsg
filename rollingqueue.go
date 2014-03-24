package pmsg

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	MaxMetaSize = 1024
	Magic       = "RQ"
	MetaSize    = 30
)

var (
	UnknownMeta    = errors.New("unknown meta data")
	MustbeFilePath = errors.New("Must be file path")
)

type queueMeta struct {
	Magic     [2]byte
	PathCrc   uint32
	MaxSize   int64
	WriterPtr int64
	ReaderPtr int64
}

type RollingQueue struct {
	queueMeta
	Path        string
	writerChan  chan []byte
	readerChan  chan []byte
	running     bool
	_writerFile *os.File //just handle
	_readerFile *os.File //just handle
	writer      io.Writer
	reader      io.Reader
	meta        *os.File
}

func (queue *RollingQueue) Push(b []byte) {
	queue.writerChan <- b
}

func (queue *RollingQueue) Pop() []byte {
	return <-queue.readerChan
}

func (queue *RollingQueue) dumpMeta() error {
	queue.meta.Seek(0, os.SEEK_SET)
	buf := bytes.NewBuffer(nil)
	datas := []interface{}{
		queue.Magic[:],
		queue.PathCrc,
		queue.MaxSize,
		queue.WriterPtr,
		queue.ReaderPtr,
	}
	for _, data := range datas {
		if err := binary.Write(buf, binary.LittleEndian, data); err != nil {
			panic(err)
		}
	}
	data := buf.Bytes()
	_, err := queue.meta.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func (queue *RollingQueue) readMeta() error {
	data := make([]byte, MetaSize, MaxMetaSize)
	n, err := queue.meta.Read(data)
	if err != nil {
		return err
	}

	if n != MetaSize {
		return UnknownMeta
	}
	hash := crc32.ChecksumIEEE([]byte(queue.Path))

	buf := bytes.NewBuffer(data)
	err = binary.Read(buf, binary.LittleEndian, queue.Magic[:])
	if err != nil || string(queue.Magic[:]) != Magic {
		return UnknownMeta
	}
	err = binary.Read(buf, binary.LittleEndian, &queue.PathCrc)
	if err != nil || hash != queue.PathCrc {
		return UnknownMeta
	}

	err = binary.Read(buf, binary.LittleEndian, &queue.MaxSize)
	if err != nil {
		return UnknownMeta
	}

	err = binary.Read(buf, binary.LittleEndian, &queue.WriterPtr)
	if err != nil {
		return UnknownMeta
	}

	err = binary.Read(buf, binary.LittleEndian, &queue.ReaderPtr)
	if err != nil {
		return UnknownMeta
	}
	return nil
}

func (q *RollingQueue) initMeta(metaPath string) error {
	var err error
	if q.meta, err = os.OpenFile(metaPath, os.O_RDWR|os.O_CREATE, 0644); err != nil {
		return err
	}
	copy(q.Magic[:], []byte(Magic))
	q.PathCrc = crc32.ChecksumIEEE([]byte(q.Path))
	println(q.PathCrc)
	return nil
}

func (q *RollingQueue) loadMeta(metaPath string) error {
	var err error
	if q.meta, err = os.OpenFile(metaPath, os.O_RDWR, 0644); err != nil {
		return err
	}

	if err = q.readMeta(); err != nil {
		return err
	}
	return nil
}

func (q *RollingQueue) writeAll(items [][]byte) {
	if len(items) == 0 {
		return
	}
	buf := new(bytes.Buffer)
	for _, item := range items {
		var l int64 = int64(len(item))
		binary.Write(buf, binary.LittleEndian, l)
		binary.Write(buf, binary.LittleEndian, item)
	}
	q.writer.Write(buf.Bytes())
}

func (q *RollingQueue) pushin() {
	var item []byte
	var cache [][]byte = make([][]byte, 0, 100)
	for q.running {
		select {
		case item = <-q.writerChan:
			cache = append(cache, item)
			if len(cache) == cap(cache) {
				q.writeAll(cache)
				cache = cache[:]
			}
		case <-time.After(1 * time.Second):
			q.writeAll(cache)
			cache = cache[:]
		}
	}
}

func (q *RollingQueue) readItem() []byte {
	return nil
}

func (q *RollingQueue) popout() {
	var item []byte
	for q.running {
		item = q.readItem()
		q.readerChan <- item
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

func createRollingQueue(p string) (queue *RollingQueue, err error) {
	q := &RollingQueue{}
	var stat os.FileInfo
	q.Path = filepath.ToSlash(p)

	dir, file := filepath.Split(q.Path)
	if file == "" {
		err = MustbeFilePath
		return
	}
	metaPath := filepath.Join(dir, ".meta."+file)
	if stat, err = os.Stat(metaPath); err == nil {
		if stat.IsDir() {
			panic("meta data must be reglar file")
		}
		if err = q.loadMeta(metaPath); err != nil {
			return
		}
	} else if os.IsNotExist(err) {
		if err = q.initMeta(metaPath); err != nil {
			q = nil
			return
		}
	}
	q.dumpMeta()
	q.running = true
	if q._readerFile, err = open(q.Path, os.O_RDONLY); err != nil {
		return
	}
	if q._writerFile, err = open(q.Path, os.O_WRONLY); err != nil {
		return
	}
	q.reader = bufio.NewReader(q._readerFile)
	q.writer = bufio.NewWriter(q._writerFile)
	q.readerChan = make(chan []byte, 1)
	q.writerChan = make(chan []byte, 1)
	go q.pushin()
	go q.popout()
	queue = q
	return
}
