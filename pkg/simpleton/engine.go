package simpleton

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"sync"

	"github.com/golang/snappy"
)

// Engine is the storage engine structure for simpleton. It implements the below KVStore interface.
type Engine struct {
	rwlock      sync.RWMutex
	f           *os.File
	keyToOffset map[string]int64
	encf        EncodingFunc
	decf        DecodingFunc
}

// KVStore is the interface that is implemented by the Engine structure of simpleton.
// This is currently for documentation purposes only.
type KVStore interface {
	Get(key string) (string, error)
	Put(key, value string) error
}

// EncodingFunc is a function that encodes keys and values before they are written to persistence layer.
type EncodingFunc func([]byte) []byte

// DecodingFunc is a function that decodes encoded keys and values that are retrieved from persistence layer.
type DecodingFunc func([]byte) []byte

// SnappyDecodingFunc utilizes snappy to decode keys and values that are retrieved from persistence layer
// and is currently the default decoding function.
var SnappyDecodingFunc DecodingFunc = func(b []byte) []byte {
	d, err := snappy.Decode(nil, b)
	if err != nil {
		panic(err)
	}
	return d
}

// SnappyEncodingFunc utilizes snappy to encode keys and values before they are written to persistence layer
// and is currently the default encoding function.
var SnappyEncodingFunc EncodingFunc = func(b []byte) []byte {
	return snappy.Encode(nil, b)
}

// NewEngine returns a new Engine structure.
// If either encoding function or decoding function is not provided, default snappy functions are used.
// Whether or not encoding and decoding functions complement each other is tested with a basic string encode then decode
// test. This same string is currently persisted in the header as well to try make sure that when an existing simpleton file
// is opened again, the previous encoding function is the same as current one.
func NewEngine(f *os.File, encf EncodingFunc, decf DecodingFunc) (*Engine, error) {
	if encf == nil || decf == nil {
		encf = SnappyEncodingFunc
		decf = SnappyDecodingFunc
	}
	teststr := `
	This is a test string for encoding and decoding functions. 
	I do know that it's not the best way to make sure that encoding and decoding functions complement each other,
	but this is what I could come up with at this point in time.
	123456789
	!@#$%^
	{}{}{}{}{}{}
	[][][][][][]
	()()()()()()
	`
	encodedteststr := encf([]byte(teststr))
	encdecresultstr := decf(encodedteststr)
	if teststr != string(encdecresultstr) {
		panic("encoding and decoding functions do not complement each other")
	}

	e := &Engine{
		f:    f,
		encf: encf,
		decf: decf,
	}
	return e, e.init(encodedteststr)
}

type operation int

const (
	write operation = iota
	remove
)

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

func (e *Engine) init(encodedteststr []byte) error {
	e.rwlock.Lock()
	defer e.rwlock.Unlock()
	e.keyToOffset = make(map[string]int64)
	endOffset, err := e.f.Seek(0, 2)
	if err != nil {
		return err
	}
	if endOffset == 0 {
		if _, err := e.f.Write(encodedteststr); err != nil {
			panic(err)
		}
		return e.f.Sync()
	}
	if _, err := e.f.Seek(0, 0); err != nil {
		return err
	}

	offset := int64(0)
	testbuf := make([]byte, len(encodedteststr))
	_, err = e.f.Read(testbuf)
	if err != nil {
		panic(err)
	}
	if bytes.Compare(encodedteststr, testbuf) != 0 {
		panic("database file could be corrupt or invalid encoding and decoding functions are provided")
	}
	offset += int64(len(testbuf))
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
