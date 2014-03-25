package pmsg

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	MaxMetaSize = 65535
	Magic       = "RQ"
	MetaSize    = 65535
)

var (
	UnknownMeta     = errors.New("unknown meta data")
	MustbeFilePath  = errors.New("Must be file path")
	errNegativeRead = errors.New("rolling queue returned negative count from Read")
)

type queueMeta struct {
	Magic                 [2]byte
	Path                  string
	MaxFileSize           int64
	MaxFile               uint8
	WriterFileIdx         uint8
	ReaderFileIdx         uint8
	CurrentFileWriteBytes int64
	CurrentFileReadBytes  int64
	WriteBytes            int64
	LimitBytes            int64 //LimitBytes must be greater than MaxFileSize
	ReadBytes             int64
}

type RollingQueue struct {
	queueMeta
	writerChan       chan []byte
	readerChan       chan []byte
	running          bool
	_writer_file     *os.File //just handle
	_reader_file     *os.File //just handle
	writer           *bufio.Writer
	reader           *bufio.Reader
	meta             *os.File
	writer_notify    chan int
	is_writer_notify bool
	reader_notify    chan int
	is_reader_notify bool
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
	var flen int32
	flen = int32(len(queue.Path))
	datas := []interface{}{
		queue.Magic[:],
		flen,
		[]byte(queue.Path),
		queue.MaxFileSize,
		queue.CurrentFileWriteBytes,
		queue.CurrentFileReadBytes,
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

	buf := bytes.NewBuffer(data)
	err = binary.Read(buf, binary.LittleEndian, queue.Magic[:])
	if err != nil || string(queue.Magic[:]) != Magic {
		return UnknownMeta
	}

	var flen int32
	err = binary.Read(buf, binary.LittleEndian, &flen)
	if err != nil {
		return UnknownMeta
	}
	path := make([]byte, flen)

	err = binary.Read(buf, binary.LittleEndian, path)
	queue.Path = string(path)
	if err != nil {
		return UnknownMeta
	}

	err = binary.Read(buf, binary.LittleEndian, &queue.MaxFileSize)
	if err != nil {
		return UnknownMeta
	}

	err = binary.Read(buf, binary.LittleEndian, &queue.CurrentFileWriteBytes)
	if err != nil {
		return UnknownMeta
	}

	err = binary.Read(buf, binary.LittleEndian, &queue.CurrentFileReadBytes)
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
	var err error
	if len(items) == 0 {
		return
	}
	buf := new(bytes.Buffer)
	for _, item := range items {
		var l int64 = int64(len(item))
		binary.Write(buf, binary.LittleEndian, l)
		binary.Write(buf, binary.LittleEndian, item)
	}
	bs := buf.Bytes()
	if q.WriteBytes > 0 && q.WriteBytes-q.ReadBytes >= q.LimitBytes {
		q.is_writer_notify = true
		for q.WriteBytes > 0 && q.WriteBytes-q.ReadBytes >= q.LimitBytes {
			select {
			case <-q.writer_notify:
			}
		}
		q.is_writer_notify = false
	} else {
		q.is_writer_notify = false
	}
	if q.writer == nil {
		q.openWriter()
	}
	var n int
	n, err = q.writer.Write(bs)
	if err != nil {
		panic(err)
	}
	err = q.writer.Flush()
	if err != nil {
		panic(err)
	}
	q.CurrentFileWriteBytes += int64(n)
	if q.CurrentFileWriteBytes > q.MaxFileSize {
		q._writer_file.Close()
		q.CurrentFileWriteBytes = 0
		q.WriterFileIdx++
		if q.WriterFileIdx > q.MaxFile {
			q.WriterFileIdx = 0
		}
		q.openWriter()
	}
	q.WriteBytes += int64(n)
	if q.is_reader_notify {

		q.reader_notify <- 1
	}
}

func (q *RollingQueue) openWriter() {
	var err error
	q._writer_file, err = open(fmt.Sprintf("%s.%d", q.Path, q.WriterFileIdx), os.O_WRONLY|os.O_CREATE)
	if err != nil {
		panic(err)
	}
	q.writer = bufio.NewWriter(q._writer_file)
}

func (q *RollingQueue) openReader() {
	var err error
	q._reader_file, err = open(fmt.Sprintf("%s.%d", q.Path, q.ReaderFileIdx), os.O_RDONLY)
	if err != nil {
		panic(err)
	}
	q.reader = bufio.NewReader(q._reader_file)
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
				cache = make([][]byte, 0, 100)
			}
		case <-time.After(1 * time.Second):
			q.writeAll(cache)
			cache = make([][]byte, 0, 100)
		}
	}
}

//read data item, if err ocurr panic.
func (q *RollingQueue) readItem() []byte {
	var l int64
	var err error
	var bs []byte
	if q.WriteBytes < 4 || q.WriteBytes-q.ReadBytes < 4 {
		q.is_reader_notify = true
		<-q.reader_notify
		q.is_reader_notify = false
	} else {
		q.is_reader_notify = false
	}
	if q.reader == nil {
		q.openReader()
	}
	err = binary.Read(q.reader, binary.LittleEndian, &l)
	if err != nil {
		var flen int64
		if state, err := q._reader_file.Stat(); err != nil {
			panic(err)
		} else {
			flen = state.Size()
		}
		if err == io.EOF && q.CurrentFileReadBytes == flen {
			q.CurrentFileReadBytes = 0
			q.ReaderFileIdx++
			if q.ReaderFileIdx > q.MaxFile {
				q.ReaderFileIdx = 0
			}
			var name = q._reader_file.Name()
			q._reader_file.Close()
			println(name)
			os.Remove(name)
			q.openReader()
			return q.readItem()
		}
		panic(err)
	}
	if l > 10000 {
		rp, _ := q._reader_file.Seek(0, os.SEEK_CUR)
		wp, _ := q._writer_file.Seek(0, os.SEEK_CUR)
		println(q.WriteBytes, q.ReadBytes, wp, rp, l)
	}
	bs = make([]byte, int(l))
	var n int
	n, err = q.reader.Read(bs)
	if err != nil {
		panic(err)
	}
	q.ReadBytes += int64(n) + 8
	q.CurrentFileReadBytes += int64(n) + 8
	if q.ReadBytes == q.WriteBytes {
		q.ReadBytes = 0
		q.WriteBytes = 0
	}
	if q.is_writer_notify {
		q.writer_notify <- 1
	}
	return bs
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
	q.MaxFileSize = 1024 * 1024 * 5
	if q.LimitBytes < q.MaxFileSize {
		q.LimitBytes = q.MaxFileSize
	}
	if q.MaxFile <= 1 {
		q.MaxFile = 3
	}
	q.dumpMeta()
	q.running = true
	q.readerChan = make(chan []byte, 1)
	q.writerChan = make(chan []byte, 1)
	q.reader_notify = make(chan int, 1)
	q.writer_notify = make(chan int, 1)
	q.is_reader_notify = false
	q.is_writer_notify = false
	go q.pushin()
	go q.popout()
	queue = q
	return
}
