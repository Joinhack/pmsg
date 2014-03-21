package pmsg

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"unsafe"
)

const (
	MaxMetaSize = 1024
	Magic       = "RQ"
	MetaSize    = int(unsafe.Sizeof(&queueMeta{}))
)

var (
	UnknowMeta     = errors.New("unknow meta data")
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
	Path   string
	writer *os.File
	reader *os.File
	meta   *os.File
}

func (queue *RollingQueue) Write(b []byte) {

}

func (queue *RollingQueue) dumpMeta() error {
	queue.meta.Seek(0, os.SEEK_SET)
	data := make([]byte, MetaSize)
	buf := bytes.NewBuffer(data)
	binary.Write(buf, binary.LittleEndian, queue.Magic[:])
	binary.Write(buf, binary.LittleEndian, queue.PathCrc)
	binary.Write(buf, binary.LittleEndian, queue.MaxSize)
	binary.Write(buf, binary.LittleEndian, queue.WriterPtr)
	binary.Write(buf, binary.LittleEndian, queue.ReaderPtr)
	_, err := queue.meta.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func (queue *RollingQueue) readMeta() error {
	data := make([]byte, MaxMetaSize)
	n, err := queue.meta.Read(data)
	if err != nil {
		return err
	}
	if n != MetaSize {
		return UnknowMeta
	}
	hash := crc32.ChecksumIEEE([]byte(queue.Path))

	buf := bytes.NewBuffer(data)
	err = binary.Read(buf, binary.LittleEndian, &queue.Magic)
	if err != nil || string(queue.Magic[:]) != Magic {
		return UnknowMeta
	}
	err = binary.Read(buf, binary.LittleEndian, &queue.PathCrc)
	if err != nil || hash != queue.PathCrc {
		return UnknowMeta
	}
	err = binary.Read(buf, binary.LittleEndian, &queue.MaxSize)
	if err != nil {
		return UnknowMeta
	}
	err = binary.Read(buf, binary.LittleEndian, &queue.WriterPtr)
	if err != nil {
		return UnknowMeta
	}
	err = binary.Read(buf, binary.LittleEndian, &queue.ReaderPtr)
	if err != nil {
		return UnknowMeta
	}
	return nil
}

func createRollingQueue(p string) (q *RollingQueue, err error) {
	q = &RollingQueue{}
	q.Path = filepath.ToSlash(p)
	dir, file := filepath.Split(q.Path)
	if file == "" {
		err = MustbeFilePath
		return
	}
	q.meta, err = os.OpenFile(filepath.Join(dir, ".meta."+file), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return
	}
	q.dumpMeta()
	return
}
