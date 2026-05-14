// Package kdb is a small LSM-tree key/value store.
//
// Writes go through a per-call fsynced WAL into an in-memory skiplist
// memtable. When the memtable crosses Options.MemtableSizeBytes it is
// frozen and synchronously flushed to a sorted on-disk SSTable; the WAL
// is then rotated. Reads consult the active memtable, then the immutable
// memtable (if a flush is in flight), then SSTables newest-first.
package kdb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/owolagbadavid/kdb/internal/kv"
	"github.com/owolagbadavid/kdb/internal/memtable"
	"github.com/owolagbadavid/kdb/internal/sstable"
	"github.com/owolagbadavid/kdb/internal/wal"
)

var ErrNotFound = errors.New("kdb: key not found")

const (
	walFilename = "wal"
	sstSuffix   = ".sst"
	tmpSuffix   = ".tmp"
)

type Options struct {
	// MemtableSizeBytes is the threshold (key+value bytes in the active
	// memtable) above which a flush is triggered after the next write.
	MemtableSizeBytes int64
}

func defaultOptions() *Options {
	return &Options{MemtableSizeBytes: 4 << 20}
}

type Stats struct {
	MemtableCount     int
	MemtableSizeBytes int64
	SSTables          []string // filenames, newest first
}

type DB struct {
	dir  string
	opts *Options

	mu          sync.RWMutex
	mt          *memtable.Memtable
	immutable   *memtable.Memtable // non-nil only during a flush
	wal         *wal.Writer
	sstables    []*sstable.Reader // newest first
	nextFileNum uint64
	seqno       uint64
	closed      bool
}

var sstNameRe = regexp.MustCompile(`^(\d{6})\.sst$`)

func Open(dir string, opts *Options) (*DB, error) {
	if opts == nil {
		opts = defaultOptions()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	db := &DB{
		dir:  dir,
		opts: opts,
		mt:   memtable.New(),
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	// Drop any leftover .tmp files from an interrupted flush.
	for _, ent := range entries {
		if strings.HasSuffix(ent.Name(), sstSuffix+tmpSuffix) {
			_ = os.Remove(filepath.Join(dir, ent.Name()))
		}
	}

	var sstNames []string
	var maxNum uint64
	for _, ent := range entries {
		m := sstNameRe.FindStringSubmatch(ent.Name())
		if m == nil {
			continue
		}
		n, err := strconv.ParseUint(m[1], 10, 64)
		if err != nil {
			continue
		}
		sstNames = append(sstNames, ent.Name())
		if n > maxNum {
			maxNum = n
		}
	}
	sort.Strings(sstNames) // ascending by name == by file number
	var readers []*sstable.Reader
	for _, name := range sstNames {
		r, err := sstable.Open(filepath.Join(dir, name))
		if err != nil {
			for _, rr := range readers {
				_ = rr.Close()
			}
			return nil, fmt.Errorf("open sstable %s: %w", name, err)
		}
		readers = append(readers, r)
	}
	// Reverse so the newest SSTable is at index 0.
	for i, j := 0, len(readers)-1; i < j; i, j = i+1, j-1 {
		readers[i], readers[j] = readers[j], readers[i]
	}
	db.sstables = readers
	db.nextFileNum = maxNum + 1

	walPath := filepath.Join(dir, walFilename)
	if err := wal.Replay(walPath, func(e kv.Entry) error {
		switch e.Kind {
		case kv.KindPut:
			db.mt.Put(e.Key, e.Value, e.Seqno)
		case kv.KindDelete:
			db.mt.Delete(e.Key, e.Seqno)
		default:
			return fmt.Errorf("wal: unknown kind %d", e.Kind)
		}
		if e.Seqno > db.seqno {
			db.seqno = e.Seqno
		}
		return nil
	}); err != nil {
		for _, r := range db.sstables {
			_ = r.Close()
		}
		return nil, fmt.Errorf("wal replay: %w", err)
	}

	w, err := wal.Create(walPath)
	if err != nil {
		for _, r := range db.sstables {
			_ = r.Close()
		}
		return nil, err
	}
	db.wal = w
	return db, nil
}

func (db *DB) Put(key, value []byte) error { return db.write(key, value, kv.KindPut) }
func (db *DB) Delete(key []byte) error     { return db.write(key, nil, kv.KindDelete) }

func (db *DB) write(key, value []byte, kind kv.Kind) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return errors.New("kdb: closed")
	}

	db.seqno++
	e := kv.Entry{Key: key, Value: value, Seqno: db.seqno, Kind: kind}
	if err := db.wal.Append(e); err != nil {
		return err
	}
	if err := db.wal.Sync(); err != nil {
		return err
	}
	if kind == kv.KindPut {
		db.mt.Put(key, value, db.seqno)
	} else {
		db.mt.Delete(key, db.seqno)
	}

	if db.mt.Count() > 0 && db.mt.SizeBytes() >= db.opts.MemtableSizeBytes {
		if err := db.flushLocked(); err != nil {
			return fmt.Errorf("flush: %w", err)
		}
	}
	return nil
}

// flushLocked freezes the active memtable, writes a new SSTable, rotates
// the WAL, and discards the immutable memtable. Must be called with
// db.mu held for writing.
func (db *DB) flushLocked() error {
	db.immutable = db.mt
	db.mt = memtable.New()

	fileNum := db.nextFileNum
	db.nextFileNum++
	name := fmt.Sprintf("%06d%s", fileNum, sstSuffix)
	path := filepath.Join(db.dir, name)

	w, err := sstable.NewWriter(path)
	if err != nil {
		return err
	}
	it := db.immutable.NewIterator()
	for ; it.Valid(); it.Next() {
		add := kv.Entry{
			Key:   it.Key(),
			Value: it.Value(),
			Seqno: it.Seqno(),
			Kind:  it.Kind(),
		}
		if err := w.Add(add); err != nil {
			it.Close()
			w.Abort()
			return err
		}
	}
	_ = it.Close()
	if err := w.Finish(); err != nil {
		w.Abort()
		return err
	}
	if err := syncDir(db.dir); err != nil {
		return err
	}

	r, err := sstable.Open(path)
	if err != nil {
		return err
	}
	db.sstables = append([]*sstable.Reader{r}, db.sstables...)

	walPath := filepath.Join(db.dir, walFilename)
	if err := db.wal.Close(); err != nil {
		return err
	}
	if err := os.Remove(walPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	newWAL, err := wal.Create(walPath)
	if err != nil {
		return err
	}
	db.wal = newWAL
	db.immutable = nil
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, errors.New("kdb: closed")
	}

	if e, ok := db.mt.Get(key); ok {
		return resolve(e)
	}
	if db.immutable != nil {
		if e, ok := db.immutable.Get(key); ok {
			return resolve(e)
		}
	}
	for _, r := range db.sstables {
		e, ok, err := r.Get(key)
		if err != nil {
			return nil, err
		}
		if ok {
			return resolve(e)
		}
	}
	return nil, ErrNotFound
}

func resolve(e kv.Entry) ([]byte, error) {
	if e.Kind == kv.KindDelete {
		return nil, ErrNotFound
	}
	return bytes.Clone(e.Value), nil
}

func (db *DB) Stats() Stats {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var names []string
	for _, r := range db.sstables {
		names = append(names, filepath.Base(r.Path()))
	}
	return Stats{
		MemtableCount:     db.mt.Count(),
		MemtableSizeBytes: db.mt.SizeBytes(),
		SSTables:          names,
	}
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true

	var firstErr error
	if db.wal != nil {
		if err := db.wal.Close(); err != nil {
			firstErr = err
		}
	}
	for _, r := range db.sstables {
		if err := r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
