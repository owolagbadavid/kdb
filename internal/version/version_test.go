package version

import "testing"

// newTestHandle makes a Handle with no real SSTable behind it; the tests
// here only exercise reference counting, not file I/O.
func newTestHandle(fileNum uint64, onZero func(*Handle)) *Handle {
	return NewHandle(nil, fileNum, 0, nil, nil, onZero)
}

func TestHandleOnZeroFiresOnce(t *testing.T) {
	var fired int
	h := newTestHandle(1, func(*Handle) { fired++ })
	h.Ref() // 0 -> 1
	h.Ref() // 1 -> 2
	h.Unref()
	if fired != 0 {
		t.Fatalf("onZero fired early (refs still > 0)")
	}
	h.Unref()
	if fired != 1 {
		t.Fatalf("onZero fired %d times, want 1", fired)
	}
}

func TestHandleUnrefBelowZeroPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on Unref below zero")
		}
	}()
	h := newTestHandle(1, nil)
	h.Ref()
	h.Unref()
	h.Unref() // -1 -> panic
}

func TestVersionUnrefCascadesToHandles(t *testing.T) {
	disposed := map[uint64]bool{}
	onZero := func(h *Handle) { disposed[h.FileNum] = true }
	h1 := newTestHandle(1, onZero)
	h2 := newTestHandle(2, onZero)

	v := NewVersion([]*Handle{h2, h1}) // Refs each handle to 1, v.refs = 1
	v.Unref()                          // v.refs -> 0, cascades

	if !disposed[1] || !disposed[2] {
		t.Fatalf("expected both handles disposed, got %v", disposed)
	}
}

func TestHandleSharedAcrossVersions(t *testing.T) {
	var disposed bool
	shared := newTestHandle(1, func(*Handle) { disposed = true })

	// Two versions both reference the shared handle.
	v1 := NewVersion([]*Handle{shared}) // shared.refs = 1
	v2 := NewVersion([]*Handle{shared}) // shared.refs = 2

	v1.Unref() // shared.refs = 1 — still alive
	if disposed {
		t.Fatal("handle disposed while still in v2")
	}
	v2.Unref() // shared.refs = 0 — now disposed
	if !disposed {
		t.Fatal("handle not disposed after last version released")
	}
}

func TestVersionOutlivesSwapForActiveReader(t *testing.T) {
	var disposed bool
	old := newTestHandle(1, func(*Handle) { disposed = true })

	cur := NewVersion([]*Handle{old}) // old.refs = 1, cur.refs = 1

	// A reader grabs the current version.
	reader := cur
	reader.Ref() // cur.refs = 2

	// A compaction swaps in a new version that drops `old`.
	cur.Unref() // cur.refs = 1 — db pointer released, reader still holds it
	if disposed {
		t.Fatal("handle disposed while a reader still holds the old version")
	}

	// Reader finishes.
	reader.Unref() // cur.refs = 0 -> old.Unref -> disposed
	if !disposed {
		t.Fatal("handle not disposed after reader released the old version")
	}
}
