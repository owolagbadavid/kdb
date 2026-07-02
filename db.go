// Package kdb is a small LSM-tree key/value store.
//
// Writes go through a per-call fsynced WAL into an in-memory skiplist
// memtable. When the memtable crosses Options.MemtableSizeBytes it is
// rotated into an immutable queue, a fresh WAL is opened, and a fresh
// active memtable starts taking writes. A background flusher drains the
// queue, writing each immutable to an SSTable and advancing the
// manifest's MinLogNum so the obsolete WAL can be deleted.
//
// Reads consult the active memtable, then the immutable queue
// newest-first, then SSTables in (tier asc, fileNum desc) order. The
// live SSTable set is a refcounted version.Version: readers Ref it and
// iterate lock-free, so compaction can retire files concurrently.
// Compaction also runs in a background goroutine.
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
	"sync/atomic"

	"github.com/owolagbadavid/kdb/internal/kv"
	"github.com/owolagbadavid/kdb/internal/manifest"
	"github.com/owolagbadavid/kdb/internal/memtable"
	"github.com/owolagbadavid/kdb/internal/sstable"
	"github.com/owolagbadavid/kdb/internal/version"
	"github.com/owolagbadavid/kdb/internal/wal"
)

var ErrNotFound = errors.New("kdb: key not found")

var errClosed = errors.New("kdb: closed")

const (
	sstSuffix = ".sst"
	tmpSuffix = ".tmp"
)

type Options struct {
	MemtableSizeBytes int64
	CompactionTrigger int
	// MaxImmutableMemtables bounds how many memtables may be queued for
	// flushing before writers stall on rotation.
	MaxImmutableMemtables int
}

func (o *Options) withDefaults() *Options {
	out := Options{
		MemtableSizeBytes:     4 << 20,
		CompactionTrigger:     4,
		MaxImmutableMemtables: 2,
	}
	if o != nil {
		if o.MemtableSizeBytes > 0 {
			out.MemtableSizeBytes = o.MemtableSizeBytes
		}
		if o.CompactionTrigger > 0 {
			out.CompactionTrigger = o.CompactionTrigger
		}
		if o.MaxImmutableMemtables > 0 {
			out.MaxImmutableMemtables = o.MaxImmutableMemtables
		}
	}
	return &out
}

type SSTableInfo struct {
	FileNum uint64
	Tier    int
}

type Stats struct {
	MemtableCount      int
	MemtableSizeBytes  int64
	ImmutableCount     int
	ImmutableSizeBytes int64
	SSTables           []SSTableInfo // newest first
}

type DB struct {
	dir  string
	opts *Options

	mu        sync.RWMutex
	flushCond *sync.Cond
	mt        *memtable.Memtable
	// immutables is the FIFO queue of memtables awaiting flush. Head
	// (index 0) is the oldest. immutableLogs[i] is the WAL number that
	// holds immutables[i]'s writes.
	immutables    []*memtable.Memtable
	immutableLogs []uint64
	wal           *wal.Writer
	activeLogNum  uint64
	// current is the live SSTable snapshot; its handles are sorted by
	// (tier asc, fileNum desc). Get correctness depends on that order.
	current     *version.Version
	nextFileNum uint64
	minLogNum   uint64 // mirror of manifest's MinLogNum
	// seqno is the monotonically increasing per-write counter. Atomic
	// because GetSnapshot reads it without holding db.mu (snapshot
	// acquisition must not contend with writes on the slow lock).
	seqno  atomic.Uint64
	closed bool

	activeReaders sync.WaitGroup // in-flight Gets holding a version ref

	compactCh   chan struct{} // capacity 1; coalesced compaction signal
	compactDone chan struct{}

	flushCh   chan struct{} // capacity 1; coalesced flush signal
	flushDone chan struct{}

	disposeCh   chan *version.Handle
	disposeDone chan struct{}

	flushFault func() error // test-only; runs inside flushOne before manifest save

	snapList *SnapshotList
	snMu     sync.Mutex
}

var (
	sstNameRe = regexp.MustCompile(`^(\d{6})\.sst$`)
	walNameRe = regexp.MustCompile(`^wal-(\d{6})\.log$`)
)

func walFileName(num uint64) string { return fmt.Sprintf("wal-%06d.log", num) }
func walPath(dir string, num uint64) string {
	return filepath.Join(dir, walFileName(num))
}

func parseWalName(name string) (uint64, bool) {
	m := walNameRe.FindStringSubmatch(name)
	if m == nil {
		return 0, false
	}
	n, err := strconv.ParseUint(m[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func Open(dir string, opts *Options) (*DB, error) {
	opts = opts.withDefaults()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	db := &DB{
		dir:         dir,
		opts:        opts,
		mt:          memtable.New(),
		compactCh:   make(chan struct{}, 1),
		compactDone: make(chan struct{}),
		flushCh:     make(chan struct{}, 1),
		flushDone:   make(chan struct{}),
		disposeCh:   make(chan *version.Handle, 64),
		disposeDone: make(chan struct{}),
		snapList:    newSnapshotList(),
	}
	db.flushCond = sync.NewCond(&db.mu)

	dirEntries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	// 1. Remove leftover *.tmp / *.new files from interrupted writes.
	for _, ent := range dirEntries {
		name := ent.Name()
		if strings.HasSuffix(name, sstSuffix+tmpSuffix) ||
			name == "MANIFEST.tmp" ||
			strings.HasSuffix(name, ".new") {
			_ = os.Remove(filepath.Join(dir, name))
		}
	}

	// 2. Index .sst files on disk and discover live WALs.
	onDiskSST := map[uint64]string{}
	var walsOnDisk []uint64
	for _, ent := range dirEntries {
		name := ent.Name()
		if m := sstNameRe.FindStringSubmatch(name); m != nil {
			if n, err := strconv.ParseUint(m[1], 10, 64); err == nil {
				onDiskSST[n] = name
			}
			continue
		}
		if n, ok := parseWalName(name); ok {
			walsOnDisk = append(walsOnDisk, n)
		}
	}
	sort.Slice(walsOnDisk, func(i, j int) bool { return walsOnDisk[i] < walsOnDisk[j] })

	// 3. Load manifest.
	snap, err := manifest.Load(dir)
	if err != nil {
		return nil, fmt.Errorf("load manifest: %w", err)
	}
	db.minLogNum = snap.MinLogNum

	// 4. Delete orphan SSTables (on disk but not in manifest).
	inManifest := map[uint64]bool{}
	for _, me := range snap.Entries {
		inManifest[me.FileNum] = true
	}
	for n, name := range onDiskSST {
		if !inManifest[n] {
			_ = os.Remove(filepath.Join(dir, name))
		}
	}

	// 5. Delete obsolete WALs (logNum < MinLogNum) and keep the rest.
	liveWals := walsOnDisk[:0]
	for _, n := range walsOnDisk {
		if n < snap.MinLogNum {
			_ = os.Remove(walPath(dir, n))
		} else {
			liveWals = append(liveWals, n)
		}
	}

	// 6. Open Readers + Handles per manifest entry, build the initial version.
	var maxFileNum uint64
	handles := make([]*version.Handle, 0, len(snap.Entries))
	for _, me := range snap.Entries {
		path := filepath.Join(dir, fmt.Sprintf("%06d.sst", me.FileNum))
		r, err := sstable.Open(path)
		if err != nil {
			for _, h := range handles {
				_ = h.Reader.Close()
			}
			return nil, fmt.Errorf("open sstable %d: %w", me.FileNum, err)
		}
		handles = append(handles, version.NewHandle(
			r, me.FileNum, me.Tier, me.Smallest, me.Largest, db.onHandleZero))
		if me.FileNum > maxFileNum {
			maxFileNum = me.FileNum
		}
	}
	sortHandles(handles)
	db.current = version.NewVersion(handles)
	db.nextFileNum = maxFileNum + 1

	// 7. Replay each live WAL into its own immutable memtable, in ascending
	//    log-num order. A WAL whose replay yields an empty memtable is
	//    skipped (the file lingers until the next flush bumps MinLogNum
	//    past it, when the disposer-equivalent loop will catch it).
	for _, n := range liveWals {
		mt := memtable.New()
		if err := wal.Replay(walPath(dir, n), func(e kv.Entry) error {
			switch e.Kind {
			case kv.KindPut:
				mt.Put(e.Key, e.Value, e.Seqno)
			case kv.KindDelete:
				mt.Delete(e.Key, e.Seqno)
			default:
				return fmt.Errorf("wal %d: unknown kind %d", n, e.Kind)
			}
			if e.Seqno > db.seqno.Load() {
				db.seqno.Store(e.Seqno)
			}
			return nil
		}); err != nil {
			for _, h := range handles {
				_ = h.Reader.Close()
			}
			return nil, fmt.Errorf("wal replay %d: %w", n, err)
		}
		if mt.Count() == 0 {
			continue
		}
		db.immutables = append(db.immutables, mt)
		db.immutableLogs = append(db.immutableLogs, n)
	}

	// 8. Open a fresh active WAL. Its number is one past the highest live
	//    log num (or 0 if there were no live WALs).
	if len(liveWals) > 0 {
		db.activeLogNum = liveWals[len(liveWals)-1] + 1
	} else {
		db.activeLogNum = snap.MinLogNum
	}
	w, err := wal.Create(walPath(dir, db.activeLogNum))
	if err != nil {
		for _, h := range handles {
			_ = h.Reader.Close()
		}
		return nil, err
	}
	db.wal = w

	// 9. Start background goroutines and signal the flusher in case Open
	//    queued immutables for recovery.
	go db.disposerLoop()
	go db.compactionLoop()
	go db.flusherLoop()
	if len(db.immutables) > 0 {
		db.signalFlush()
	}
	return db, nil
}

func (db *DB) GetSnapshot() uint64 {
	seq := db.seqno.Load()
	db.snMu.Lock()
	db.snapList.Insert(seq)
	db.snMu.Unlock()
	return seq
}

// ReleaseSnapshot is idempotent: a double-release or unknown seq is a
// silent no-op (the bool from SnapshotList.Remove is intentionally
// discarded so callers don't have to track release state).
func (db *DB) ReleaseSnapshot(seq uint64) error {
	db.snMu.Lock()
	db.snapList.Remove(seq)
	db.snMu.Unlock()
	return nil
}

func (db *DB) Put(key, value []byte) error { return db.write(key, value, kv.KindPut) }
func (db *DB) Delete(key []byte) error     { return db.write(key, nil, kv.KindDelete) }

func (db *DB) write(key, value []byte, kind kv.Kind) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return errClosed
	}

	seq := db.seqno.Add(1)

	e := kv.Entry{Key: key, Value: value, Seqno: seq, Kind: kind}
	if err := db.wal.Append(e); err != nil {
		return err
	}
	if err := db.wal.Sync(); err != nil {
		return err
	}
	if kind == kv.KindPut {
		db.mt.Put(key, value, seq)
	} else {
		db.mt.Delete(key, seq)
	}

	if db.mt.Count() > 0 && db.mt.SizeBytes() >= db.opts.MemtableSizeBytes {
		if err := db.rotateMemtableLocked(); err != nil {
			return fmt.Errorf("rotate: %w", err)
		}
	}
	return nil
}

// rotateMemtableLocked moves the active memtable to the immutable queue,
// opens a fresh WAL, and signals the flusher. Stalls on flushCond when
// the queue is already at MaxImmutableMemtables.
func (db *DB) rotateMemtableLocked() error {
	for len(db.immutables) >= db.opts.MaxImmutableMemtables {
		db.flushCond.Wait()
		if db.closed {
			return errClosed
		}
	}

	// Queue the active memtable along with the WAL number that holds its writes.
	db.immutables = append(db.immutables, db.mt)
	db.immutableLogs = append(db.immutableLogs, db.activeLogNum)
	db.mt = memtable.New()

	// Open a new WAL and close the old one. Atomic create-rename-fsync
	// so a crash leaves either the old or the new WAL on disk.
	if err := db.rotateWALLocked(); err != nil {
		return err
	}

	db.signalFlush()
	return nil
}

// rotateWALLocked closes the current active WAL and opens a new one at
// activeLogNum+1.
func (db *DB) rotateWALLocked() error {
	newNum := db.activeLogNum + 1
	finalPath := walPath(db.dir, newNum)
	tmpPath := finalPath + ".new"
	newWAL, err := wal.Create(tmpPath)
	if err != nil {
		return err
	}
	if err := db.wal.Close(); err != nil {
		_ = newWAL.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = newWAL.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := syncDir(db.dir); err != nil {
		_ = newWAL.Close()
		return err
	}
	db.wal = newWAL
	db.activeLogNum = newNum
	return nil
}

// installVersionLocked persists a manifest snapshot (SSTable list +
// MinLogNum) and swaps in the new version. The previous version is
// Unref'd. On manifest failure nothing is swapped.
func (db *DB) installVersionLocked(handles []*version.Handle, minLogNum uint64) error {
	entries := make([]manifest.Entry, 0, len(handles))
	for _, h := range handles {
		entries = append(entries, manifest.Entry{
			FileNum: h.FileNum, Tier: h.Tier,
			Smallest: h.Smallest, Largest: h.Largest,
		})
	}
	if err := manifest.Save(db.dir, manifest.Snapshot{
		MinLogNum: minLogNum,
		Entries:   entries,
	}); err != nil {
		return err
	}
	old := db.current
	db.current = version.NewVersion(handles)
	db.minLogNum = minLogNum
	old.Unref()
	return nil
}

func (db *DB) signalCompaction() {
	select {
	case db.compactCh <- struct{}{}:
	default:
	}
}

func (db *DB) signalFlush() {
	select {
	case db.flushCh <- struct{}{}:
	default:
	}
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, errClosed
	}
	if e, ok := db.mt.Get(key); ok {
		db.mu.RUnlock()
		return resolve(e)
	}
	// Immutables are FIFO oldest-first. Read newest-first by iterating in
	// reverse, so a newer rotation shadows an older one for the same key.
	for i := len(db.immutables) - 1; i >= 0; i-- {
		if e, ok := db.immutables[i].Get(key); ok {
			db.mu.RUnlock()
			return resolve(e)
		}
	}
	// Grab a refcounted snapshot of the live SSTables and release the
	// lock — the iteration below runs without blocking writers, flusher,
	// or compactor.
	v := db.current
	v.Ref()
	db.activeReaders.Add(1)
	db.mu.RUnlock()
	defer func() {
		v.Unref()
		db.activeReaders.Done()
	}()

	for _, h := range v.Handles {
		e, ok, err := h.Reader.Get(key)
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
	infos := make([]SSTableInfo, 0, len(db.current.Handles))
	for _, h := range db.current.Handles {
		infos = append(infos, SSTableInfo{FileNum: h.FileNum, Tier: h.Tier})
	}
	var immBytes int64
	for _, mt := range db.immutables {
		immBytes += mt.SizeBytes()
	}
	return Stats{
		MemtableCount:      db.mt.Count(),
		MemtableSizeBytes:  db.mt.SizeBytes(),
		ImmutableCount:     len(db.immutables),
		ImmutableSizeBytes: immBytes,
		SSTables:           infos,
	}
}

func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true
	db.flushCond.Broadcast() // wake any stalled writers
	db.mu.Unlock()

	// Final signal so the flusher drains any queued immutables before
	// exiting. With db.closed=true, no more rotations can add to the queue.
	db.signalFlush()

	// Close flushCh first and wait for the flusher to finish. The
	// flusher may signal the compactor as part of its final drain, so
	// compactCh must stay open until the flusher exits.
	db.mu.Lock()
	close(db.flushCh)
	db.mu.Unlock()
	<-db.flushDone

	db.mu.Lock()
	close(db.compactCh)
	db.mu.Unlock()
	<-db.compactDone
	db.activeReaders.Wait()

	db.mu.Lock()
	cur := db.current
	walErr := db.wal.Close()
	db.mu.Unlock()

	// Close the live SSTable readers directly. Routing them through the
	// disposer would delete the (still-live) files; only compacted-away
	// SSTables get deleted.
	for _, h := range cur.Handles {
		if err := h.Reader.Close(); err != nil && walErr == nil {
			walErr = err
		}
	}

	close(db.disposeCh)
	<-db.disposeDone
	return walErr
}

// onHandleZero is the disposal callback for every SSTable handle. It
// fires when a handle's last reference drops, which during normal
// operation means the SSTable was compacted away and is safe to delete.
func (db *DB) onHandleZero(h *version.Handle) {
	db.disposeCh <- h
}

func (db *DB) disposerLoop() {
	for h := range db.disposeCh {
		_ = h.Reader.Close()
		_ = os.Remove(filepath.Join(db.dir, fmt.Sprintf("%06d.sst", h.FileNum)))
	}
	close(db.disposeDone)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
