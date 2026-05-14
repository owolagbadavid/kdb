// Package sstable implements the immutable on-disk sorted table format
// used by the LSM. A file is:
//
//	[record 0]
//	[record 1]
//	...
//	[record N-1]
//	[index entry 0]      -- one per every indexInterval records
//	[index entry 1]
//	...
//	[footer]             -- fixed 16 bytes at end of file
//
// Record: [varint keylen][key][varint vallen][value][uint8 kind][uint64 seqno]
// Index entry: [varint keylen][key][uint64 offset]
// Footer: [uint64 indexOffset][uint64 indexLen]
package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/owolagbadavid/kdb/internal/kv"
)

const (
	indexInterval = 16
	footerSize    = 16
)

type indexEntry struct {
	key    []byte
	offset uint64
}

// Writer builds an SSTable. Add must be called with keys in strictly
// ascending order; Finish atomically renames the .tmp into place.
type Writer struct {
	path    string
	tmpPath string
	f       *os.File
	bw      *bufio.Writer
	offset  uint64
	count   int
	index   []indexEntry
	lastKey []byte
}

func NewWriter(path string) (*Writer, error) {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &Writer{path: path, tmpPath: tmp, f: f, bw: bufio.NewWriter(f)}, nil
}

func (w *Writer) Add(e kv.Entry) error {
	if w.lastKey != nil && bytes.Compare(e.Key, w.lastKey) <= 0 {
		return fmt.Errorf("sstable: keys must be strictly ascending (got %q after %q)", e.Key, w.lastKey)
	}
	if w.count%indexInterval == 0 {
		w.index = append(w.index, indexEntry{
			key:    append([]byte(nil), e.Key...),
			offset: w.offset,
		})
	}
	n, err := encodeRecord(w.bw, e)
	if err != nil {
		return err
	}
	w.offset += uint64(n)
	w.count++
	w.lastKey = append(w.lastKey[:0], e.Key...)
	return nil
}

func (w *Writer) Finish() error {
	indexOffset := w.offset
	for _, ie := range w.index {
		n, err := encodeIndexEntry(w.bw, ie)
		if err != nil {
			return err
		}
		w.offset += uint64(n)
	}
	indexLen := w.offset - indexOffset

	var footer [footerSize]byte
	binary.LittleEndian.PutUint64(footer[0:8], indexOffset)
	binary.LittleEndian.PutUint64(footer[8:16], indexLen)
	if _, err := w.bw.Write(footer[:]); err != nil {
		return err
	}
	if err := w.bw.Flush(); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	if err := w.f.Close(); err != nil {
		return err
	}
	return os.Rename(w.tmpPath, w.path)
}

func (w *Writer) Abort() {
	_ = w.f.Close()
	_ = os.Remove(w.tmpPath)
}

func encodeRecord(w *bufio.Writer, e kv.Entry) (int, error) {
	var u [binary.MaxVarintLen64]byte
	n := 0
	nn := binary.PutUvarint(u[:], uint64(len(e.Key)))
	if _, err := w.Write(u[:nn]); err != nil {
		return n, err
	}
	n += nn
	if _, err := w.Write(e.Key); err != nil {
		return n, err
	}
	n += len(e.Key)
	nn = binary.PutUvarint(u[:], uint64(len(e.Value)))
	if _, err := w.Write(u[:nn]); err != nil {
		return n, err
	}
	n += nn
	if len(e.Value) > 0 {
		if _, err := w.Write(e.Value); err != nil {
			return n, err
		}
		n += len(e.Value)
	}
	if err := w.WriteByte(byte(e.Kind)); err != nil {
		return n, err
	}
	n++
	binary.LittleEndian.PutUint64(u[0:8], e.Seqno)
	if _, err := w.Write(u[0:8]); err != nil {
		return n, err
	}
	n += 8
	return n, nil
}

func encodeIndexEntry(w *bufio.Writer, ie indexEntry) (int, error) {
	var u [binary.MaxVarintLen64]byte
	n := 0
	nn := binary.PutUvarint(u[:], uint64(len(ie.key)))
	if _, err := w.Write(u[:nn]); err != nil {
		return n, err
	}
	n += nn
	if _, err := w.Write(ie.key); err != nil {
		return n, err
	}
	n += len(ie.key)
	binary.LittleEndian.PutUint64(u[0:8], ie.offset)
	if _, err := w.Write(u[0:8]); err != nil {
		return n, err
	}
	n += 8
	return n, nil
}

// Reader reads a finished SSTable. The index is parsed into memory on Open;
// the file is kept open for ReadAt-based record fetches.
type Reader struct {
	f       *os.File
	path    string
	index   []indexEntry
	dataLen uint64
}

func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if stat.Size() < footerSize {
		f.Close()
		return nil, fmt.Errorf("sstable: %s too small (%d bytes)", path, stat.Size())
	}
	var footer [footerSize]byte
	if _, err := f.ReadAt(footer[:], stat.Size()-footerSize); err != nil {
		f.Close()
		return nil, err
	}
	indexOffset := binary.LittleEndian.Uint64(footer[0:8])
	indexLen := binary.LittleEndian.Uint64(footer[8:16])
	if indexOffset+indexLen+footerSize != uint64(stat.Size()) {
		f.Close()
		return nil, fmt.Errorf("sstable: %s corrupt footer (indexOff=%d indexLen=%d size=%d)",
			path, indexOffset, indexLen, stat.Size())
	}

	indexBuf := make([]byte, indexLen)
	if _, err := f.ReadAt(indexBuf, int64(indexOffset)); err != nil {
		f.Close()
		return nil, err
	}
	var index []indexEntry
	p := indexBuf
	for len(p) > 0 {
		klen, n := binary.Uvarint(p)
		if n <= 0 {
			f.Close()
			return nil, fmt.Errorf("sstable: bad index keylen")
		}
		p = p[n:]
		if uint64(len(p)) < klen+8 {
			f.Close()
			return nil, fmt.Errorf("sstable: truncated index entry")
		}
		key := append([]byte(nil), p[:klen]...)
		p = p[klen:]
		off := binary.LittleEndian.Uint64(p[:8])
		p = p[8:]
		index = append(index, indexEntry{key: key, offset: off})
	}
	return &Reader{f: f, path: path, index: index, dataLen: indexOffset}, nil
}

func (r *Reader) Close() error { return r.f.Close() }
func (r *Reader) Path() string { return r.path }

// Get returns the entry for key. The bool reports whether key was present
// (a tombstone hit returns true with kind == KindDelete; callers translate
// that into "deleted").
func (r *Reader) Get(key []byte) (kv.Entry, bool, error) {
	if len(r.index) == 0 {
		return kv.Entry{}, false, nil
	}
	i := sort.Search(len(r.index), func(i int) bool {
		return bytes.Compare(r.index[i].key, key) > 0
	})
	if i == 0 {
		// key sorts before the first record in this SSTable.
		return kv.Entry{}, false, nil
	}
	startOffset := r.index[i-1].offset
	var endOffset uint64
	if i < len(r.index) {
		endOffset = r.index[i].offset
	} else {
		endOffset = r.dataLen
	}
	sr := io.NewSectionReader(r.f, int64(startOffset), int64(endOffset-startOffset))
	br := bufio.NewReader(sr)
	for {
		e, err := decodeRecord(br)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return kv.Entry{}, false, nil
			}
			return kv.Entry{}, false, err
		}
		switch bytes.Compare(e.Key, key) {
		case 0:
			return e, true, nil
		case 1:
			return kv.Entry{}, false, nil
		}
	}
}

func (r *Reader) NewIterator() kv.Iterator {
	sr := io.NewSectionReader(r.f, 0, int64(r.dataLen))
	it := &sstIterator{r: r, br: bufio.NewReader(sr)}
	it.advance()
	return it
}

func decodeRecord(br *bufio.Reader) (kv.Entry, error) {
	var e kv.Entry
	klen, err := binary.ReadUvarint(br)
	if err != nil {
		return e, err
	}
	key := make([]byte, klen)
	if _, err := io.ReadFull(br, key); err != nil {
		return e, err
	}
	vlen, err := binary.ReadUvarint(br)
	if err != nil {
		return e, err
	}
	var val []byte
	if vlen > 0 {
		val = make([]byte, vlen)
		if _, err := io.ReadFull(br, val); err != nil {
			return e, err
		}
	}
	kind, err := br.ReadByte()
	if err != nil {
		return e, err
	}
	var seqBuf [8]byte
	if _, err := io.ReadFull(br, seqBuf[:]); err != nil {
		return e, err
	}
	e.Key = key
	e.Value = val
	e.Kind = kv.Kind(kind)
	e.Seqno = binary.LittleEndian.Uint64(seqBuf[:])
	return e, nil
}

type sstIterator struct {
	r     *Reader
	br    *bufio.Reader
	cur   kv.Entry
	valid bool
}

func (it *sstIterator) Valid() bool   { return it.valid }
func (it *sstIterator) Key() []byte   { return it.cur.Key }
func (it *sstIterator) Value() []byte { return it.cur.Value }
func (it *sstIterator) Seqno() uint64 { return it.cur.Seqno }
func (it *sstIterator) Kind() kv.Kind { return it.cur.Kind }
func (it *sstIterator) Next()         { it.advance() }
func (it *sstIterator) Close() error  { return nil }

func (it *sstIterator) Seek(key []byte) {
	var startOffset uint64
	if len(it.r.index) > 0 {
		i := sort.Search(len(it.r.index), func(i int) bool {
			return bytes.Compare(it.r.index[i].key, key) > 0
		})
		if i > 0 {
			startOffset = it.r.index[i-1].offset
		}
	}
	sr := io.NewSectionReader(it.r.f, int64(startOffset), int64(it.r.dataLen-startOffset))
	it.br = bufio.NewReader(sr)
	for {
		it.advance()
		if !it.valid {
			return
		}
		if bytes.Compare(it.cur.Key, key) >= 0 {
			return
		}
	}
}

func (it *sstIterator) advance() {
	e, err := decodeRecord(it.br)
	if err != nil {
		it.valid = false
		return
	}
	it.cur = e
	it.valid = true
}
