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
//	[filter block]       -- bloom filter over user-keys (omitted if no records)
//	[footer]             -- fixed 40 bytes at end of file
//
// Record:       [varint keylen][internal key][varint vallen][value]
// Index entry:  [varint keylen][internal key][uint64 offset]
// Filter block: [uint8 version][uint64 m][uint32 k][bits]
// Footer:       [uint64 indexOffset][uint64 indexLen][uint64 filterOffset][uint64 filterLen][uint64 magic]
//
// Records are sorted by internal key (user key ascending, then seqno
// descending). The bloom filter is hashed over user keys (not internal
// keys) so point lookups can short-circuit before any seqno-aware seek.
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

	"github.com/owolagbadavid/kdb/internal/bloom"
	"github.com/owolagbadavid/kdb/internal/kv"
)

const (
	indexInterval    = 16
	footerSize       = 40
	magic            = 0xB10F11A5_5570DEAD
	filterVersion1   = 1
	filterV1HeaderSz = 13
)

type indexEntry struct {
	key    []byte
	offset uint64
}

// Writer builds an SSTable. Add must be called with internal keys in
// strictly ascending order (user key ascending, then seqno descending
// for the same user key). Finish atomically renames the .tmp into place.
type Writer struct {
	path            string
	tmpPath         string
	f               *os.File
	bw              *bufio.Writer
	offset          uint64
	count           int
	index           []indexEntry
	hashes          []uint64
	lastInternalKey []byte
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
	ik := kv.MakeInternalKey(e.Key, e.Seqno, e.Kind)
	if w.lastInternalKey != nil && kv.CompareInternal(ik, w.lastInternalKey) <= 0 {
		return fmt.Errorf("sstable: keys must be strictly ascending (got user=%q seq=%d after %q)",
			e.Key, e.Seqno, kv.UserKeyOf(w.lastInternalKey))
	}
	if w.count%indexInterval == 0 {
		w.index = append(w.index, indexEntry{
			key:    append([]byte(nil), ik...),
			offset: w.offset,
		})
	}
	// Bloom is hashed over user keys so point lookups by user key can
	// short-circuit without knowing any seqno.
	w.hashes = append(w.hashes, bloom.Hash(e.Key))
	n, err := encodeRecord(w.bw, ik, e.Value)
	if err != nil {
		return err
	}
	w.offset += uint64(n)
	w.count++
	w.lastInternalKey = append(w.lastInternalKey[:0], ik...)
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
	filterOffset := w.offset

	var filterBytes []byte
	if len(w.hashes) > 0 {
		f := bloom.New(uint64(len(w.hashes)), 0.01)
		for _, h := range w.hashes {
			f.AddHash(h)
		}
		filterBytes = encodeFilter(f)
	}
	w.hashes = nil

	if len(filterBytes) > 0 {
		nn, err := w.bw.Write(filterBytes)
		if err != nil {
			return err
		}
		w.offset += uint64(nn)
	}
	filterLen := uint64(len(filterBytes))

	var footer [footerSize]byte
	binary.LittleEndian.PutUint64(footer[0:8], indexOffset)
	binary.LittleEndian.PutUint64(footer[8:16], indexLen)
	binary.LittleEndian.PutUint64(footer[16:24], filterOffset)
	binary.LittleEndian.PutUint64(footer[24:32], filterLen)
	binary.LittleEndian.PutUint64(footer[32:], magic)

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

func encodeRecord(w *bufio.Writer, internalKey, value []byte) (int, error) {
	var u [binary.MaxVarintLen64]byte
	n := 0
	nn := binary.PutUvarint(u[:], uint64(len(internalKey)))
	if _, err := w.Write(u[:nn]); err != nil {
		return n, err
	}
	n += nn
	if _, err := w.Write(internalKey); err != nil {
		return n, err
	}
	n += len(internalKey)
	nn = binary.PutUvarint(u[:], uint64(len(value)))
	if _, err := w.Write(u[:nn]); err != nil {
		return n, err
	}
	n += nn
	if len(value) > 0 {
		if _, err := w.Write(value); err != nil {
			return n, err
		}
		n += len(value)
	}
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
	f        *os.File
	path     string
	index    []indexEntry
	dataLen  uint64
	smallest []byte
	largest  []byte
	filter   *bloom.Filter
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

	if binary.LittleEndian.Uint64(footer[32:40]) != magic {
		f.Close()
		return nil, fmt.Errorf("sstable: %s corrupt footer (bad magic)", path)
	}

	indexOffset := binary.LittleEndian.Uint64(footer[0:8])
	indexLen := binary.LittleEndian.Uint64(footer[8:16])
	filterOffset := binary.LittleEndian.Uint64(footer[16:24])
	filterLen := binary.LittleEndian.Uint64(footer[24:32])

	if filterOffset != indexOffset+indexLen {
		f.Close()
		return nil, fmt.Errorf("sstable: %s gap/overlap: filterOff=%d want %d",
			path, filterOffset, indexOffset+indexLen)
	}

	if filterOffset+filterLen+footerSize != uint64(stat.Size()) {
		f.Close()
		return nil, fmt.Errorf("sstable: %s corrupt footer (indexOff=%d indexLen=%d filterLen=%d size=%d)",
			path, indexOffset, indexLen, filterLen, stat.Size())
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
		if klen > uint64(len(p)) || uint64(len(p))-klen < 8 {
			f.Close()
			return nil, fmt.Errorf("sstable: truncated index entry")
		}
		key := append([]byte(nil), p[:klen]...)
		p = p[klen:]
		off := binary.LittleEndian.Uint64(p[:8])
		p = p[8:]
		index = append(index, indexEntry{key: key, offset: off})
	}

	filterBuf := make([]byte, filterLen)
	if _, err := f.ReadAt(filterBuf, int64(filterOffset)); err != nil {
		f.Close()
		return nil, err
	}

	filter, err := parseFilter(filterBuf)
	if err != nil {
		f.Close()
		return nil, err
	}

	r := &Reader{f: f, path: path, index: index, dataLen: indexOffset, filter: filter}
	if len(index) > 0 {
		// Index entries hold internal keys; expose only the user-key
		// portion via Smallest/Largest, which is what the manifest
		// and range-overlap logic care about.
		r.smallest = append([]byte(nil), kv.UserKeyOf(index[0].key)...)
		last, err := r.findLargestKey()
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("sstable: %s: %w", path, err)
		}
		r.largest = last
	}
	return r, nil
}

func (r *Reader) Close() error { return r.f.Close() }
func (r *Reader) Path() string { return r.path }

// SmallestKey returns the first key in the SSTable, or nil if empty.
func (r *Reader) SmallestKey() []byte { return r.smallest }

// LargestKey returns the last key in the SSTable, or nil if empty.
func (r *Reader) LargestKey() []byte { return r.largest }

// findLargestKey scans the final data block and returns the user key of
// the last record. Used to populate the manifest's per-SSTable range.
func (r *Reader) findLargestKey() ([]byte, error) {
	last := r.index[len(r.index)-1]
	sr := io.NewSectionReader(r.f, int64(last.offset), int64(r.dataLen-last.offset))
	br := bufio.NewReader(sr)
	var lastIK []byte
	for {
		ik, _, err := decodeRecord(br)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if lastIK == nil {
					return nil, nil
				}
				return append([]byte(nil), kv.UserKeyOf(lastIK)...), nil
			}
			return nil, err
		}
		lastIK = ik
	}
}

// Get returns the newest visible version of userKey. A tombstone hit
// returns true with Kind == KindDelete; callers translate that into
// "deleted." For a snapshot-aware read, use GetAt.
func (r *Reader) Get(userKey []byte) (kv.Entry, bool, error) {
	return r.GetAt(userKey, kv.SeqnoMax)
}

// GetAt returns the newest version of userKey with seqno <= snapSeq, or
// (Entry{}, false, nil) if no such version is present. The bloom filter
// short-circuits when the user key is definitely absent.
func (r *Reader) GetAt(userKey []byte, snapSeq uint64) (kv.Entry, bool, error) {
	if len(r.index) == 0 {
		return kv.Entry{}, false, nil
	}
	if r.filter != nil && !r.filter.Contains(userKey) {
		return kv.Entry{}, false, nil
	}
	seek := kv.MakeInternalKey(userKey, snapSeq, kv.KindMax)
	i := sort.Search(len(r.index), func(i int) bool {
		return kv.CompareInternal(r.index[i].key, seek) > 0
	})
	// i == 0 means the seek sorts before every index entry's first
	// record. The seek for (userKey, snapSeq, kindMax) is the smallest
	// possible internal key for userKey, so seek < index[0] does NOT
	// imply "userKey absent" — we still need to scan block 0 to see if
	// it holds the user key. Read all records from startOffset to EOF
	// (the scan terminates as soon as the user key is found or passed).
	var startOffset uint64
	if i > 0 {
		startOffset = r.index[i-1].offset
	}
	sr := io.NewSectionReader(r.f, int64(startOffset), int64(r.dataLen-startOffset))
	br := bufio.NewReader(sr)
	for {
		ik, value, err := decodeRecord(br)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return kv.Entry{}, false, nil
			}
			return kv.Entry{}, false, err
		}
		uk := kv.UserKeyOf(ik)
		switch bytes.Compare(uk, userKey) {
		case -1:
			continue // earlier user key, keep scanning
		case 1:
			return kv.Entry{}, false, nil // passed it, no visible version
		}
		// uk == userKey. Records here are sorted by seqno descending,
		// so the first one with seqno <= snapSeq is the newest visible.
		seqno, kind := kv.DecodeTrailer(ik)
		if seqno > snapSeq {
			continue
		}
		return kv.Entry{
			Key:   append([]byte(nil), uk...),
			Value: value,
			Seqno: seqno,
			Kind:  kind,
		}, true, nil
	}
}

func (r *Reader) NewIterator() kv.Iterator {
	sr := io.NewSectionReader(r.f, 0, int64(r.dataLen))
	it := &sstIterator{r: r, br: bufio.NewReader(sr)}
	it.advance()
	return it
}

// decodeRecord reads one (internalKey, value) record from br.
func decodeRecord(br *bufio.Reader) (internalKey, value []byte, err error) {
	klen, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, nil, err
	}
	internalKey = make([]byte, klen)
	if _, err := io.ReadFull(br, internalKey); err != nil {
		return nil, nil, err
	}
	vlen, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, nil, err
	}
	if vlen > 0 {
		value = make([]byte, vlen)
		if _, err := io.ReadFull(br, value); err != nil {
			return nil, nil, err
		}
	}
	return internalKey, value, nil
}

type sstIterator struct {
	r     *Reader
	br    *bufio.Reader
	curIK []byte // current internal key
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

// Seek positions at the first version (any seqno) of the smallest user
// key >= target. Within the same user key, iteration proceeds from
// newest seqno to oldest before moving to the next user key.
func (it *sstIterator) Seek(target []byte) {
	seek := kv.MakeInternalKey(target, kv.SeqnoMax, kv.KindMax)
	var startOffset uint64
	if len(it.r.index) > 0 {
		i := sort.Search(len(it.r.index), func(i int) bool {
			return kv.CompareInternal(it.r.index[i].key, seek) > 0
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
		if kv.CompareInternal(it.curIK, seek) >= 0 {
			return
		}
	}
}

func (it *sstIterator) advance() {
	ik, value, err := decodeRecord(it.br)
	if err != nil {
		it.valid = false
		return
	}
	seqno, kind := kv.DecodeTrailer(ik)
	it.curIK = ik
	it.cur = kv.Entry{
		Key:   kv.UserKeyOf(ik),
		Value: value,
		Seqno: seqno,
		Kind:  kind,
	}
	it.valid = true
}

func parseFilter(buf []byte) (*bloom.Filter, error) {
	if len(buf) == 0 {
		return nil, nil
	}
	switch v := buf[0]; v { // byte 0 is the only field with a fixed position
	case filterVersion1:
		return parseFilterV1(buf)
	default:
		return nil, fmt.Errorf("sstable: unknown filter version %d", v)
	}
}

func parseFilterV1(buf []byte) (*bloom.Filter, error) {
	if len(buf) < filterV1HeaderSz {
		return nil, fmt.Errorf("sstable: filter v1 too small: %d bytes", len(buf))
	}
	m := binary.LittleEndian.Uint64(buf[1:9])
	k := uint64(binary.LittleEndian.Uint32(buf[9:13]))
	bits := buf[filterV1HeaderSz:]

	if m == 0 || k == 0 {
		return nil, fmt.Errorf("sstable: invalid filter params m=%d k=%d", m, k)
	}
	if want := (m + 7) / 8; uint64(len(bits)) != want {
		return nil, fmt.Errorf("sstable: filter length mismatch: have %d want %d (m=%d)",
			len(bits), want, m)
	}
	return &bloom.Filter{Version: filterVersion1, Bits: bits, M: m, K: k}, nil
}

func encodeFilter(f *bloom.Filter) []byte {
	buf := make([]byte, filterV1HeaderSz+len(f.Bits))
	buf[0] = filterVersion1
	binary.LittleEndian.PutUint64(buf[1:9], f.M)
	binary.LittleEndian.PutUint32(buf[9:13], uint32(f.K))
	copy(buf[filterV1HeaderSz:], f.Bits)
	return buf
}
