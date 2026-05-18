package sstable

import (
	"testing"

	"github.com/owolagbadavid/kdb/internal/kv"
)

// sliceIter is a tiny in-memory kv.Iterator for tests. Entries must be
// sorted ascending by key.
type sliceIter struct {
	entries []kv.Entry
	i       int
}

func newSliceIter(es ...kv.Entry) *sliceIter { return &sliceIter{entries: es} }

func (s *sliceIter) Valid() bool   { return s.i < len(s.entries) }
func (s *sliceIter) Key() []byte   { return s.entries[s.i].Key }
func (s *sliceIter) Value() []byte { return s.entries[s.i].Value }
func (s *sliceIter) Seqno() uint64 { return s.entries[s.i].Seqno }
func (s *sliceIter) Kind() kv.Kind { return s.entries[s.i].Kind }
func (s *sliceIter) Next()         { s.i++ }
func (s *sliceIter) Close() error  { return nil }
func (s *sliceIter) Seek(key []byte) {
	for s.i = 0; s.i < len(s.entries); s.i++ {
		if string(s.entries[s.i].Key) >= string(key) {
			return
		}
	}
}

func collect(it kv.Iterator) []kv.Entry {
	var out []kv.Entry
	for ; it.Valid(); it.Next() {
		out = append(out, kv.Entry{
			Key: append([]byte(nil), it.Key()...), Value: append([]byte(nil), it.Value()...),
			Seqno: it.Seqno(), Kind: it.Kind(),
		})
	}
	return out
}

func TestMergeDisjoint(t *testing.T) {
	a := newSliceIter(
		kv.Entry{Key: []byte("a"), Value: []byte("1"), Seqno: 1, Kind: kv.KindPut},
		kv.Entry{Key: []byte("c"), Value: []byte("3"), Seqno: 3, Kind: kv.KindPut},
	)
	b := newSliceIter(
		kv.Entry{Key: []byte("b"), Value: []byte("2"), Seqno: 2, Kind: kv.KindPut},
		kv.Entry{Key: []byte("d"), Value: []byte("4"), Seqno: 4, Kind: kv.KindPut},
	)
	got := collect(NewMergeIterator([]kv.Iterator{a, b}))
	wantKeys := []string{"a", "b", "c", "d"}
	if len(got) != len(wantKeys) {
		t.Fatalf("len = %d, want %d (%+v)", len(got), len(wantKeys), got)
	}
	for i, k := range wantKeys {
		if string(got[i].Key) != k {
			t.Fatalf("at %d: got %q, want %q", i, got[i].Key, k)
		}
	}
}

func TestMergeOverlapNewestWins(t *testing.T) {
	older := newSliceIter(
		kv.Entry{Key: []byte("k"), Value: []byte("old"), Seqno: 1, Kind: kv.KindPut},
	)
	newer := newSliceIter(
		kv.Entry{Key: []byte("k"), Value: []byte("new"), Seqno: 5, Kind: kv.KindPut},
	)
	// Order shouldn't matter — winner is whichever has higher seqno.
	got := collect(NewMergeIterator([]kv.Iterator{older, newer}))
	if len(got) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(got))
	}
	if string(got[0].Value) != "new" || got[0].Seqno != 5 {
		t.Fatalf("got %+v, want value=new seqno=5", got[0])
	}
}

func TestMergeTombstoneShadowsOldPut(t *testing.T) {
	put := newSliceIter(
		kv.Entry{Key: []byte("k"), Value: []byte("v"), Seqno: 1, Kind: kv.KindPut},
	)
	tomb := newSliceIter(
		kv.Entry{Key: []byte("k"), Value: nil, Seqno: 2, Kind: kv.KindDelete},
	)
	got := collect(NewMergeIterator([]kv.Iterator{put, tomb}))
	if len(got) != 1 || got[0].Kind != kv.KindDelete || got[0].Seqno != 2 {
		t.Fatalf("got %+v, want a single tombstone seqno=2", got)
	}
}

func TestMergeThreeSourcesWithDuplicates(t *testing.T) {
	a := newSliceIter(
		kv.Entry{Key: []byte("a"), Value: []byte("a1"), Seqno: 1, Kind: kv.KindPut},
		kv.Entry{Key: []byte("c"), Value: []byte("c1"), Seqno: 2, Kind: kv.KindPut},
	)
	b := newSliceIter(
		kv.Entry{Key: []byte("b"), Value: []byte("b3"), Seqno: 3, Kind: kv.KindPut},
		kv.Entry{Key: []byte("c"), Value: []byte("c4"), Seqno: 4, Kind: kv.KindPut},
	)
	c := newSliceIter(
		kv.Entry{Key: []byte("c"), Value: []byte("c6"), Seqno: 6, Kind: kv.KindPut},
		kv.Entry{Key: []byte("d"), Value: []byte("d5"), Seqno: 5, Kind: kv.KindPut},
	)
	got := collect(NewMergeIterator([]kv.Iterator{a, b, c}))
	if len(got) != 4 {
		t.Fatalf("len = %d, want 4 (%+v)", len(got), got)
	}
	// "c" should resolve to seqno 6.
	for _, e := range got {
		if string(e.Key) == "c" && (e.Seqno != 6 || string(e.Value) != "c6") {
			t.Fatalf("c resolved to %+v, want seqno=6 value=c6", e)
		}
	}
}

func TestMergeEmptySources(t *testing.T) {
	m := NewMergeIterator([]kv.Iterator{newSliceIter(), newSliceIter()})
	if m.Valid() {
		t.Fatal("empty merge should be invalid")
	}
}
