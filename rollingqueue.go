package pmsg

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
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
	UnknownMeta     = errors.New("unknown meta data")
	MustbeFilePath  = errors.New("Must be file path")
	errNegativeRead = errors.New("rolling queue returned negative count from Read")
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
	Path               string
	writerChan         chan []byte
	readerChan         chan []byte
	running            bool
	writer             *os.File //just handle
	reader             *os.File //just handle
	meta               *os.File
	reader_w, reader_r int
	reader_buf         []byte
	reader_err         error
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

func (q *RollingQueue) readErr() error {
	err := q.reader_err
	q.reader_err = nil
	return err
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
	_, err = q.writer.Write(buf.Bytes())
	if err != nil {
		panic(err)
	}
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

//read data item, if err ocurr panic.
func (q *RollingQueue) readItem() []byte {
	var l int64
	var err error
	var bs []byte
	err = binary.Read(q.reader, binary.LittleEndian, &l)
	if err != nil {
		panic(err)
	}
	bs = make([]byte, int(l))
	err = binary.Read(q.reader, binary.LittleEndian, &l)
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

func (q *RollingQueue) fill() {
	// Slide existing data to beginning.
	if q.reader_r > 0 {
		copy(q.reader_buf, q.reader_buf[q.reader_r:q.reader_w])
		q.reader_w -= q.reader_r
		q.reader_r = 0
	}
	var remainBytes int = q.remainBytes()
	var n int
	var err error
	if remainBytes < len(q.reader_buf)-q.reader_w {
		n, err = q.reader.Read(q.reader_buf[q.reader_w : q.reader_w+remainBytes])
	} else {
		// Read new data.
		n, err = q.reader.Read(q.reader_buf[q.reader_w:])
	}
	if n < 0 {
		panic(errNegativeRead)
	}
	q.reader_w += n
	if err != nil {
		q.reader_err = err
	}
}

func (q *RollingQueue) Read(p []byte) (n int, err error) {
	n = len(p)
	if n == 0 {
		return n, q.readErr()
	}
	if q.reader_w == q.reader_r {
		if q.reader_err != nil {
			return 0, q.readErr()
		}
		if len(p) >= len(q.reader_buf) {
			// Large read, empty buffer.
			// Read directly into p to avoid copy.
			n, q.reader_err = q.reader.Read(p)
			return n, q.readErr()
		}
		q.fill()
		if q.reader_w == q.reader_r {
			return 0, q.readErr()
		}
	}

	if n > q.reader_w-q.reader_r {
		n = q.reader_w - q.reader_r
	}
	copy(p[0:n], q.reader_buf[q.reader_r:])
	q.reader_r += n
	return n, nil
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
	if q.reader, err = open(q.Path, os.O_RDONLY); err != nil {
		return
	}
	if q.writer, err = open(q.Path, os.O_WRONLY); err != nil {
		return
	}
	q.readerChan = make(chan []byte, 1)
	q.writerChan = make(chan []byte, 1)
	go q.pushin()
	go q.popout()
	queue = q
	return
}
