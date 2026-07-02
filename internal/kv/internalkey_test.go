package kv

import (
	"bytes"
	"sort"
	"testing"
)

func TestInternalKeyRoundtrip(t *testing.T) {
	ik := MakeInternalKey([]byte("hello"), 42, KindPut)
	if got := UserKeyOf(ik); !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("UserKeyOf = %q, want hello", got)
	}
	seq, kind := DecodeTrailer(ik)
	if seq != 42 || kind != KindPut {
		t.Fatalf("DecodeTrailer = (%d, %d), want (42, %d)", seq, kind, KindPut)
	}
}

func TestInternalKeyTombstone(t *testing.T) {
	ik := MakeInternalKey([]byte("k"), 7, KindDelete)
	seq, kind := DecodeTrailer(ik)
	if seq != 7 || kind != KindDelete {
		t.Fatalf("got (%d, %d), want (7, %d)", seq, kind, KindDelete)
	}
}

func TestInternalKeySortOrder(t *testing.T) {
	// Three user keys, multiple seqnos each. Expected byte-sort order:
	// user key ascending, then seqno descending within the same user key.
	type rec struct {
		k    []byte
		s    uint64
		kind Kind
	}
	in := []rec{
		{[]byte("apple"), 1, KindPut},
		{[]byte("apple"), 5, KindPut},
		{[]byte("apple"), 3, KindDelete},
		{[]byte("banana"), 10, KindPut},
		{[]byte("banana"), 2, KindPut},
		{[]byte("cherry"), 9, KindPut},
	}
	want := []rec{
		{[]byte("apple"), 5, KindPut},
		{[]byte("apple"), 3, KindDelete},
		{[]byte("apple"), 1, KindPut},
		{[]byte("banana"), 10, KindPut},
		{[]byte("banana"), 2, KindPut},
		{[]byte("cherry"), 9, KindPut},
	}

	iks := make([][]byte, len(in))
	for i, r := range in {
		iks[i] = MakeInternalKey(r.k, r.s, r.kind)
	}
	sort.Slice(iks, func(i, j int) bool { return bytes.Compare(iks[i], iks[j]) < 0 })

	if len(iks) != len(want) {
		t.Fatalf("len = %d, want %d", len(iks), len(want))
	}
	for i, w := range want {
		uk := UserKeyOf(iks[i])
		s, k := DecodeTrailer(iks[i])
		if !bytes.Equal(uk, w.k) || s != w.s || k != w.kind {
			t.Errorf("at %d: got (%q, %d, %d), want (%q, %d, %d)",
				i, uk, s, k, w.k, w.s, w.kind)
		}
	}
}

func TestCompareInternalPrefixOverlap(t *testing.T) {
	// "key-0049" < "key-0049a" user-key-wise, regardless of trailer.
	// Plain bytes.Compare on the full internal key would get this
	// backwards because the shorter key's trailer byte (~0xFF) ranks
	// against the longer key's user-key byte ('a' = 0x61).
	short := MakeInternalKey([]byte("key-0049"), 1, KindPut)
	long := MakeInternalKey([]byte("key-0049a"), 1, KindPut)
	if CompareInternal(short, long) >= 0 {
		t.Fatalf("CompareInternal(short, long) = %d, want negative", CompareInternal(short, long))
	}
}

func TestCompareInternalSameUserKey(t *testing.T) {
	// Higher seqno sorts smaller (newest first).
	newer := MakeInternalKey([]byte("K"), 10, KindPut)
	older := MakeInternalKey([]byte("K"), 5, KindPut)
	if CompareInternal(newer, older) >= 0 {
		t.Fatalf("expected newer < older, got %d", CompareInternal(newer, older))
	}
}

func TestInternalKeySeekBoundary(t *testing.T) {
	// A seek key built with KindMax must sort BEFORE any real entry
	// with the same user key and seqno <= ceiling, and AFTER any real
	// entry with seqno > ceiling. That's the lookup invariant.
	const userKey = "K"
	const ceiling = 12
	seek := MakeInternalKey([]byte(userKey), ceiling, KindMax)

	cases := []struct {
		name      string
		seq       uint64
		kind      Kind
		seekFirst bool // expected: seek < real (i.e., real comes after seek)
	}{
		{"older than ceiling", 5, KindPut, true},
		{"at the ceiling", 12, KindPut, true},
		{"newer than ceiling", 15, KindPut, false},
		{"max-1 vs ceiling", 11, KindDelete, true},
	}
	for _, c := range cases {
		real := MakeInternalKey([]byte(userKey), c.seq, c.kind)
		got := bytes.Compare(seek, real) < 0
		if got != c.seekFirst {
			t.Errorf("%s: seek<real = %v, want %v", c.name, got, c.seekFirst)
		}
	}
}
