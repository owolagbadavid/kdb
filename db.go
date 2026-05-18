// Package kdb is a small LSM-tree key/value store.
//
// Writes go through a per-call fsynced WAL into an in-memory skiplist
// memtable. When the memtable crosses Options.MemtableSizeBytes it is
// frozen and synchronously flushed to a sorted on-disk SSTable; the WAL
// is then rotated. Reads consult the active memtable, then the immutable
// memtable (if a flush is in flight), then SSTables newest-first.
//
// Size-tiered compaction runs synchronously after each flush: when a
// tier accumulates Options.CompactionTrigger SSTables, they merge into a
// single SSTable in the next tier. The manifest file is the on-disk
// source of truth for which SSTables are live.
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

	"github.com/owolagbadavid/kdb/internal/compaction"
	"github.com/owolagbadavid/kdb/internal/kv"
	"github.com/owolagbadavid/kdb/internal/manifest"
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
	MemtableSizeBytes int64
	CompactionTrigger int
}

func (o *Options) withDefaults() *Options {
	out := Options{MemtableSizeBytes: 4 << 20, CompactionTrigger: 4}
	if o != nil {
		if o.MemtableSizeBytes != 0 {
			out.MemtableSizeBytes = o.MemtableSizeBytes
		}
		if o.CompactionTrigger != 0 {
			out.CompactionTrigger = o.CompactionTrigger
		}
	}
	return &out
}

type SSTableInfo struct {
	FileNum uint64
	Tier    int
}

type Stats struct {
	MemtableCount     int
	MemtableSizeBytes int64
	SSTables          []SSTableInfo // newest first
}

type sstableEntry struct {
	reader   *sstable.Reader
	fileNum  uint64
	tier     int
	smallest []byte
	largest  []byte
}

type DB struct {
	dir  string
	opts *Options

	mu          sync.RWMutex
	mt          *memtable.Memtable
	immutable   *memtable.Memtable
	wal         *wal.Writer
	sstables    []*sstableEntry // sorted by fileNum descending
	nextFileNum uint64
	seqno       uint64
	closed      bool
}

var sstNameRe = regexp.MustCompile(`^(\d{6})\.sst$`)

func Open(dir string, opts *Options) (*DB, error) {
	opts = opts.withDefaults()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	db := &DB{
		dir:  dir,
		opts: opts,
		mt:   memtable.New(),
	}

	dirEntries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	// 1. Remove leftover .tmp files from interrupted writes.
	for _, ent := range dirEntries {
		name := ent.Name()
		if strings.HasSuffix(name, sstSuffix+tmpSuffix) || name == "MANIFEST.tmp" {
			_ = os.Remove(filepath.Join(dir, name))
		}
	}

	// 2. Index all .sst files present on disk by file number.
	onDisk := map[uint64]string{}
	for _, ent := range dirEntries {
		m := sstNameRe.FindStringSubmatch(ent.Name())
		if m == nil {
			continue
		}
		n, err := strconv.ParseUint(m[1], 10, 64)
		if err != nil {
			continue
		}
		onDisk[n] = ent.Name()
	}

	manifestEntries, err := manifest.Load(dir)
	if err != nil {
		return nil, fmt.Errorf("load manifest: %w", err)
	}
	if manifestEntries == nil && len(onDisk) > 0 {
		nums := make([]uint64, 0, len(onDisk))
		for n := range onDisk {
			nums = append(nums, n)
		}
		sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
		for _, n := range nums {
			r, err := sstable.Open(filepath.Join(dir, onDisk[n]))
			if err != nil {
				return nil, fmt.Errorf("migrate: open sstable %s: %w", onDisk[n], err)
			}
			manifestEntries = append(manifestEntries, manifest.Entry{
				FileNum: n, Tier: 0,
				Smallest: append([]byte(nil), r.SmallestKey()...),
				Largest:  append([]byte(nil), r.LargestKey()...),
			})
			_ = r.Close()
		}
		if err := manifest.Save(dir, manifestEntries); err != nil {
			return nil, fmt.Errorf("migrate manifest: %w", err)
		}
	}

	// 4. Delete orphan SSTable files (present but not in manifest).
	inManifest := map[uint64]bool{}
	for _, me := range manifestEntries {
		inManifest[me.FileNum] = true
	}
	for n, name := range onDisk {
		if !inManifest[n] {
			_ = os.Remove(filepath.Join(dir, name))
		}
	}

	// 5. Open Reader per manifest entry.
	var maxNum uint64
	for _, me := range manifestEntries {
		path := filepath.Join(dir, fmt.Sprintf("%06d.sst", me.FileNum))
		r, err := sstable.Open(path)
		if err != nil {
			for _, s := range db.sstables {
				_ = s.reader.Close()
			}
			return nil, fmt.Errorf("open sstable %d: %w", me.FileNum, err)
		}
		db.sstables = append(db.sstables, &sstableEntry{
			reader:   r,
			fileNum:  me.FileNum,
			tier:     me.Tier,
			smallest: me.Smallest,
			largest:  me.Largest,
		})
		if me.FileNum > maxNum {
			maxNum = me.FileNum
		}
	}
	sort.Slice(db.sstables, func(i, j int) bool {
		return db.sstables[i].fileNum > db.sstables[j].fileNum
	})
	db.nextFileNum = maxNum + 1

	// 6. Replay WAL.
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
		for _, s := range db.sstables {
			_ = s.reader.Close()
		}
		return nil, fmt.Errorf("wal replay: %w", err)
	}

	// 7. Fresh WAL writer.
	w, err := wal.Create(walPath)
	if err != nil {
		for _, s := range db.sstables {
			_ = s.reader.Close()
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
		add := kv.Entry{Key: it.Key(), Value: it.Value(), Seqno: it.Seqno(), Kind: it.Kind()}
		if err := w.Add(add); err != nil {
			_ = it.Close()
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
	entry := &sstableEntry{
		reader: r, fileNum: fileNum, tier: 0,
		smallest: r.SmallestKey(), largest: r.LargestKey(),
	}
	db.sstables = append([]*sstableEntry{entry}, db.sstables...)

	if err := db.saveManifestLocked(); err != nil {
		return err
	}

	// Rotate WAL only after the new SSTable is durable AND referenced
	// in the manifest — otherwise a crash here could lose the writes.
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

	return db.maybeCompactLocked()
}

func (db *DB) saveManifestLocked() error {
	entries := make([]manifest.Entry, 0, len(db.sstables))
	for _, s := range db.sstables {
		entries = append(entries, manifest.Entry{
			FileNum: s.fileNum, Tier: s.tier,
			Smallest: s.smallest, Largest: s.largest,
		})
	}
	return manifest.Save(db.dir, entries)
}

// maybeCompactLocked runs compactions in a loop until no tier exceeds
// the trigger. Cascading is desirable: compacting tier 0 may push tier 1
// over the trigger, and so on.
func (db *DB) maybeCompactLocked() error {
	for {
		inputs := make([]compaction.Input, 0, len(db.sstables))
		for _, s := range db.sstables {
			inputs = append(inputs, compaction.Input{
				Reader: s.reader, FileNum: s.fileNum, Tier: s.tier,
			})
		}
		plan := compaction.Pick(inputs, db.opts.CompactionTrigger)
		if plan == nil {
			return nil
		}
		if err := db.runCompactionLocked(plan); err != nil {
			return err
		}
	}
}

func (db *DB) runCompactionLocked(plan *compaction.Plan) error {
	newNum := db.nextFileNum
	db.nextFileNum++

	out, err := compaction.Run(plan, db.dir, newNum)
	if err != nil {
		return err
	}
	if err := syncDir(db.dir); err != nil {
		return err
	}

	inputSet := map[uint64]bool{}
	for _, in := range plan.Inputs {
		inputSet[in.FileNum] = true
	}
	keep := db.sstables[:0:0]
	for _, s := range db.sstables {
		if !inputSet[s.fileNum] {
			keep = append(keep, s)
		}
	}
	keep = append(keep, &sstableEntry{
		reader: out.Reader, fileNum: out.FileNum, tier: out.Tier,
		smallest: out.Smallest, largest: out.Largest,
	})
	sort.Slice(keep, func(i, j int) bool { return keep[i].fileNum > keep[j].fileNum })
	db.sstables = keep

	if err := db.saveManifestLocked(); err != nil {
		return err
	}

	// Manifest committed — inputs are now orphans. Safe to dispose.
	for _, in := range plan.Inputs {
		_ = in.Reader.Close()
		_ = os.Remove(filepath.Join(db.dir, fmt.Sprintf("%06d.sst", in.FileNum)))
	}
	_ = syncDir(db.dir)
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
	for _, s := range db.sstables {
		e, ok, err := s.reader.Get(key)
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
	infos := make([]SSTableInfo, 0, len(db.sstables))
	for _, s := range db.sstables {
		infos = append(infos, SSTableInfo{FileNum: s.fileNum, Tier: s.tier})
	}
	return Stats{
		MemtableCount:     db.mt.Count(),
		MemtableSizeBytes: db.mt.SizeBytes(),
		SSTables:          infos,
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
	for _, s := range db.sstables {
		if err := s.reader.Close(); err != nil && firstErr == nil {
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
