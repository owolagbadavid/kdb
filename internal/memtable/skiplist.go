package memtable

import (
	"bytes"
	"math/rand/v2"

	"github.com/owolagbadavid/kdb/internal/kv"
)

const (
	maxHeight = 12
	pInv      = 4 // 1/p, where p is the per-level promotion probability
)

type node struct {
	key   []byte
	value []byte
	next  []*node
}

// Skiplist is a probabilistic ordered map over opaque byte keys. It is
// not safe for concurrent use; callers must serialise writes (concurrent
// reads without writers are fine).
//
// In the memtable's MVCC layout the keys are internal keys (user key +
// 8-byte trailer encoding seqno+kind). The skiplist itself does not
// interpret keys; the memtable wrapper is responsible for building and
// decoding internal keys.
type Skiplist struct {
	head      *node
	height    int
	sizeBytes int64
	count     int
}

func NewSkiplist() *Skiplist {
	return &Skiplist{
		head:   &node{next: make([]*node, maxHeight)},
		height: 1,
	}
}

// Insert sets key to value. If key already exists (exact byte match) the
// value is updated in place; otherwise a new node is created. WAL replay
// can re-deliver the same internal key, so overwriting is idempotent.
func (s *Skiplist) Insert(key, value []byte) {
	var prev [maxHeight]*node
	x := s.head
	for i := s.height - 1; i >= 0; i-- {
		for x.next[i] != nil && kv.CompareInternal(x.next[i].key, key) < 0 {
			x = x.next[i]
		}
		prev[i] = x
	}

	if n := x.next[0]; n != nil && bytes.Equal(n.key, key) {
		s.sizeBytes += int64(len(value)) - int64(len(n.value))
		n.value = value
		return
	}

	h := randomHeight()
	if h > s.height {
		for i := s.height; i < h; i++ {
			prev[i] = s.head
		}
		s.height = h
	}

	n := &node{
		key:   key,
		value: value,
		next:  make([]*node, h),
	}
	for i := 0; i < h; i++ {
		n.next[i] = prev[i].next[i]
		prev[i].next[i] = n
	}
	s.sizeBytes += int64(len(key) + len(value))
	s.count++
}

// Get returns the value associated with key, if present (exact match).
func (s *Skiplist) Get(key []byte) ([]byte, bool) {
	x := s.head
	for i := s.height - 1; i >= 0; i-- {
		for x.next[i] != nil && kv.CompareInternal(x.next[i].key, key) < 0 {
			x = x.next[i]
		}
	}
	n := x.next[0]
	if n == nil || !bytes.Equal(n.key, key) {
		return nil, false
	}
	return n.value, true
}

func (s *Skiplist) SizeBytes() int64 { return s.sizeBytes }
func (s *Skiplist) Count() int       { return s.count }

func (s *Skiplist) NewIterator() *Iterator {
	return &Iterator{s: s, cur: s.head.next[0]}
}

func randomHeight() int {
	h := 1
	for h < maxHeight && rand.IntN(pInv) == 0 {
		h++
	}
	return h
}

// Iterator walks a Skiplist in ascending byte-order. A fresh iterator
// is positioned at the first key (Valid() reports whether the source is
// non-empty).
type Iterator struct {
	s   *Skiplist
	cur *node
}

func (it *Iterator) Valid() bool   { return it.cur != nil }
func (it *Iterator) Key() []byte   { return it.cur.key }
func (it *Iterator) Value() []byte { return it.cur.value }
func (it *Iterator) Next()         { it.cur = it.cur.next[0] }

// Seek positions the iterator at the first key >= target.
func (it *Iterator) Seek(target []byte) {
	x := it.s.head
	for i := it.s.height - 1; i >= 0; i-- {
		for x.next[i] != nil && kv.CompareInternal(x.next[i].key, target) < 0 {
			x = x.next[i]
		}
	}
	it.cur = x.next[0]
}

func (it *Iterator) Close() error { return nil }
