package simpleton

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"sync"

	"github.com/golang/snappy"
)

type EncodingFunc func([]byte) []byte
type DecodingFunc func([]byte) []byte

type Engine struct {
	rwlock      sync.RWMutex
	f           *os.File
	keyToOffset map[string]int64
	encf        EncodingFunc
	decf        DecodingFunc
}

var SnappyDecodingFunc DecodingFunc = func(b []byte) []byte {
	d, err := snappy.Decode(nil, b)
	if err != nil {
		panic(err)
	}
	return d
}

var SnappyEncodingFunc EncodingFunc = func(b []byte) []byte {
	return snappy.Encode(nil, b)
}

func NewEngine(f *os.File, encf EncodingFunc, decf DecodingFunc) (*Engine, error) {
	if encf == nil || decf == nil {
		encf = SnappyEncodingFunc
		decf = SnappyDecodingFunc
	}
	e := &Engine{
		f:    f,
		encf: encf,
		decf: decf,
	}
	return e, e.init()
}

type operation int

const (
	write operation = iota
	remove
)

func (e *Engine) init() error {
	e.rwlock.Lock()
	defer e.rwlock.Unlock()
	e.keyToOffset = make(map[string]int64)
	endOffset, err := e.f.Seek(0, 2)
	if err != nil {
		return err
	}
	if endOffset == 0 {
		return nil
	}
	if _, err := e.f.Seek(0, 0); err != nil {
		return err
	}

	offset := int64(0)
	buf := make([]byte, 8)
	for {
		op, encKey, nextOffset, err := e.readNextKey(offset, buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		key := string(e.decf(encKey))
		if op == write {
			e.keyToOffset[key] = offset
		} else {
			delete(e.keyToOffset, key)
		}
		offset = nextOffset
	}
	return nil
}

func (e *Engine) Get(key string) (string, error) {
	e.rwlock.RLock()
	defer e.rwlock.RUnlock()
	o, ok := e.keyToOffset[key]
	if !ok {
		return "", fmt.Errorf("key not found %v", key)
	}
	meta := make([]byte, 8)
	if _, err := e.f.Seek(o, 0); err != nil {
		return "", fmt.Errorf("could not seek offset %v reason %v", o, err)
	}
	if _, err := e.f.Read(meta); err != nil {
		return "", err
	}
	op, keylen, vallen := decodeMeta(meta)
	if op != write {
		return "", fmt.Errorf("unexpected operation at offset %v", o)
	}
	if _, err := e.f.Seek(int64(keylen), 1); err != nil {
		return "", fmt.Errorf("could not seek offset %v reason %v", o+int64(keylen), err)
	}
	buf := make([]byte, vallen)
	if _, err := e.f.Read(buf); err != nil && err != io.EOF {
		return "", fmt.Errorf("could not read value for key %v reason %v", key, err)
	}

	return string(e.decf(buf)), nil
}

func (e *Engine) Put(key, value string) error {
	encodedKey := e.encf([]byte(key))
	if uint(len(encodedKey)) > (math.MaxUint32 >> 1) {
		return fmt.Errorf("key length is greater than max allowed %v", math.MaxUint32>>1)
	}
	encodedValue := e.encf([]byte(value))
	if uint(len(encodedValue)) > math.MaxUint32 {
		return fmt.Errorf("value length is greater than max allowed %v", math.MaxUint32)
	}
	meta := encodeMeta(write, uint32(len(encodedKey)), uint32(len(encodedValue)))
	e.rwlock.Lock()
	defer e.rwlock.Unlock()
	o, err := e.f.Seek(0, 2)
	if err != nil {
		return err
	}
	if _, err := e.f.Write(meta); err != nil {
		return err
	}
	if _, err := e.f.Write(encodedKey); err != nil {
		return err
	}
	if _, err := e.f.Write(encodedValue); err != nil {
		return err
	}
	if err := e.f.Sync(); err != nil {
		return err
	}
	e.keyToOffset[key] = o
	return nil
}

func (e *Engine) readNextKey(offset int64, buf []byte) (operation, []byte, int64, error) {
	if len(buf) != 8 {
		panic("invalid length of buffer during readKey")
	}
	n, err := e.f.ReadAt(buf, offset)
	if err != nil {
		return write, nil, 0, err
	}
	if n != len(buf) {
		panic("read bytes are less than buffer length during readKey")
	}
	op, keylen, vallen := decodeMeta(buf)
	kbuf := make([]byte, keylen)
	n, err = e.f.ReadAt(kbuf, offset+8)
	if err != nil {
		return write, nil, 0, err
	}
	if n != len(kbuf) {
		panic("could not read key entirely during readKey")
	}
	offset += 8 + int64(keylen) + int64(vallen)
	return op, kbuf, offset, nil
}

// operation is 1 bit
// key is 31 bit
// value is 32 bit
func decodeMeta(meta []byte) (operation, uint32, uint32) {
	var op operation
	p := binary.BigEndian.Uint32(meta[0:4])
	keylen := (p << 1) >> 1
	if p == keylen {
		op = write
	} else {
		op = remove
	}
	vallen := binary.BigEndian.Uint32(meta[4:8])
	return op, keylen, vallen
}

func encodeMeta(op operation, keylen, vallen uint32) []byte {
	if (keylen >> 31) == 1 {
		panic(fmt.Sprintf("encode meta failed for keylen %v", keylen))
	}
	if op == remove {
		keylen &= (1 << 31)
	}
	meta := make([]byte, 8)
	binary.BigEndian.PutUint32(meta[0:4], keylen)
	binary.BigEndian.PutUint32(meta[4:8], vallen)
	return meta
}
