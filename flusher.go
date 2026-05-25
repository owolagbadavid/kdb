package kdb

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/owolagbadavid/kdb/internal/kv"
	"github.com/owolagbadavid/kdb/internal/sstable"
	"github.com/owolagbadavid/kdb/internal/version"
)

// flusherLoop is the background flusher. Each signal triggers a drain
// of the immutable queue: it pops the head, writes an SSTable, commits
// a new version (with MinLogNum advanced past the flushed memtable's
// WAL), and deletes the now-obsolete WAL file.
func (db *DB) flusherLoop() {
	for range db.flushCh {
		for {
			done, err := db.flushOne()
			if err != nil {
				// Best-effort: the immutable stays at the head of the
				// queue and the WAL stays on disk. The next signal (or
				// Open after a restart) will retry it.
				break
			}
			if done {
				break
			}
		}
	}
	close(db.flushDone)
}

// flushOne flushes the head of the immutable queue. Returns done=true
// when there is nothing to flush (queue empty).
func (db *DB) flushOne() (done bool, err error) {
	db.mu.Lock()
	if len(db.immutables) == 0 {
		db.mu.Unlock()
		return true, nil
	}
	mt := db.immutables[0]
	logNum := db.immutableLogs[0]
	fileNum := db.nextFileNum
	db.nextFileNum++
	fault := db.flushFault
	db.flushFault = nil
	db.mu.Unlock()

	path := filepath.Join(db.dir, fmt.Sprintf("%06d%s", fileNum, sstSuffix))
	w, err := sstable.NewWriter(path)
	if err != nil {
		return false, err
	}
	// The skiplist iterator's Key/Value slices are stable until Close
	// (deferred until after w.Finish), so the raw slices may be passed
	// to the SSTable writer without copying.
	it := mt.NewIterator()
	for ; it.Valid(); it.Next() {
		add := kv.Entry{Key: it.Key(), Value: it.Value(), Seqno: it.Seqno(), Kind: it.Kind()}
		if err := w.Add(add); err != nil {
			_ = it.Close()
			w.Abort()
			return false, err
		}
	}
	_ = it.Close()
	if err := w.Finish(); err != nil {
		w.Abort()
		return false, err
	}
	if err := syncDir(db.dir); err != nil {
		_ = os.Remove(path)
		return false, err
	}

	// Test-only fault injection: fires between SSTable rename and
	// manifest save. May return an error (fail this flush) or block.
	if fault != nil {
		if err := fault(); err != nil {
			_ = os.Remove(path)
			return false, err
		}
	}

	r, err := sstable.Open(path)
	if err != nil {
		_ = os.Remove(path)
		return false, err
	}
	newHandle := version.NewHandle(r, fileNum, 0, r.SmallestKey(), r.LargestKey(), db.onHandleZero)

	db.mu.Lock()
	// Re-read the current version: a compaction may have committed
	// while we were writing the SSTable.
	handles := append([]*version.Handle{newHandle}, db.current.Handles...)
	sortHandles(handles)
	// New MinLogNum: if there is another immutable behind this one in
	// the queue, MinLogNum = its log num; otherwise MinLogNum is the
	// active WAL — every WAL before it has been flushed.
	var newMinLogNum uint64
	if len(db.immutableLogs) > 1 {
		newMinLogNum = db.immutableLogs[1]
	} else {
		newMinLogNum = db.activeLogNum
	}
	if err := db.installVersionLocked(handles, newMinLogNum); err != nil {
		db.mu.Unlock()
		_ = r.Close()
		_ = os.Remove(path)
		return false, err
	}
	// Manifest committed — pop the queue.
	db.immutables = db.immutables[1:]
	db.immutableLogs = db.immutableLogs[1:]
	// Wake any writers stalled on the queue-full backpressure.
	db.flushCond.Broadcast()
	// Signal compactor: a new SSTable may have pushed tier 0 over the trigger.
	db.signalCompaction()
	db.mu.Unlock()

	// Delete the WAL that backed the just-flushed memtable. Best-effort:
	// if it fails, the next Open's MinLogNum-based cleanup catches it.
	_ = os.Remove(walPath(db.dir, logNum))
	_ = syncDir(db.dir)
	return false, nil
}
