// Package version provides refcounted snapshots of the live SSTable set.
//
// A Version is an immutable list of SSTable Handles. Readers Ref the
// current Version, iterate it lock-free, then Unref it. Writers (flush,
// compaction) build a new Version and swap it in; the old Version is
// Unref'd and, once no reader still holds it, releases its handles.
//
// A Handle owns one SSTable file. It is alive as long as some Version
// references it; when its last reference drops, its onZero callback
// fires — the DB layer uses that to close the reader and delete the file.
// This is the mechanism that makes it safe to retire SSTables during
// compaction while concurrent reads are in flight.
package version

import (
	"sync/atomic"

	"github.com/owolagbadavid/kdb/internal/sstable"
)

// Handle owns a single live SSTable.
type Handle struct {
	Reader   *sstable.Reader
	FileNum  uint64
	Tier     int
	Smallest []byte
	Largest  []byte

	refs   atomic.Int32
	onZero func(*Handle)
}

// NewHandle creates a handle with a reference count of zero. The caller
// — typically NewVersion — performs the initial Ref. onZero is invoked
// once, when the count returns to zero.
func NewHandle(r *sstable.Reader, fileNum uint64, tier int, smallest, largest []byte, onZero func(*Handle)) *Handle {
	return &Handle{
		Reader:   r,
		FileNum:  fileNum,
		Tier:     tier,
		Smallest: smallest,
		Largest:  largest,
		onZero:   onZero,
	}
}

func (h *Handle) Ref() { h.refs.Add(1) }

func (h *Handle) Unref() {
	switch n := h.refs.Add(-1); {
	case n == 0:
		if h.onZero != nil {
			h.onZero(h)
		}
	case n < 0:
		panic("version: Handle Unref'd below zero")
	}
}

// Version is an immutable, refcounted snapshot of the live SSTable set.
type Version struct {
	// Handles is sorted by FileNum descending (newest first). Reads
	// depend on that order to resolve a key to its newest entry.
	Handles []*Handle
	refs    atomic.Int32
}

// NewVersion builds a Version that owns handles: it Refs each handle
// once and starts with its own reference count at one.
func NewVersion(handles []*Handle) *Version {
	v := &Version{Handles: handles}
	v.refs.Store(1)
	for _, h := range handles {
		h.Ref()
	}
	return v
}

func (v *Version) Ref() { v.refs.Add(1) }

// Unref drops one reference. When the last reference is released, every
// handle in the Version is Unref'd in turn — handles no longer present
// in any newer Version then reach zero and dispose themselves.
func (v *Version) Unref() {
	switch n := v.refs.Add(-1); {
	case n == 0:
		for _, h := range v.Handles {
			h.Unref()
		}
	case n < 0:
		panic("version: Version Unref'd below zero")
	}
}
