package kdb

// SnapshotList is the in-memory registry of live snapshot seqnos used
// by compaction's GC horizon. It is NOT safe for concurrent use; the
// caller (db.snMu) is responsible for serialising access.
//
// The list is a doubly-linked chain ordered by insertion (which, given
// seqnos are monotonic, is also ordered ascending by seq). A side
// index maps seq to the live nodes carrying that seq, so Insert and
// Remove are both O(1) even when two snapshots share a seqno (which
// happens whenever two GetSnapshot calls land between writes).
type SnapshotList struct {
	head, tail *snapNode
	index      map[uint64][]*snapNode
}

type snapNode struct {
	seq        uint64
	prev, next *snapNode
}

func newSnapshotList() *SnapshotList {
	return &SnapshotList{index: make(map[uint64][]*snapNode)}
}

// Insert appends a node at the tail. Seqnos only grow, so the chain
// stays sorted ascending without any explicit comparison.
func (s *SnapshotList) Insert(seq uint64) {
	n := &snapNode{seq: seq, prev: s.tail}
	if s.tail != nil {
		s.tail.next = n
	} else {
		s.head = n
	}
	s.tail = n
	s.index[seq] = append(s.index[seq], n)
}

// Remove unlinks one node carrying seq. If multiple snapshots share
// seq, an arbitrary one is removed (the ordering doesn't matter for
// GC: the surviving duplicate keeps the floor at the same value).
// Returns true if a node was removed; false on double-release or
// unknown seq, which is treated as a benign no-op.
func (s *SnapshotList) Remove(seq uint64) bool {
	bucket := s.index[seq]
	if len(bucket) == 0 {
		return false
	}
	n := bucket[len(bucket)-1]
	bucket = bucket[:len(bucket)-1]
	if len(bucket) == 0 {
		delete(s.index, seq)
	} else {
		s.index[seq] = bucket
	}
	s.unlink(n)
	return true
}

func (s *SnapshotList) unlink(n *snapNode) {
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		s.head = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else {
		s.tail = n.prev
	}
}

// Oldest returns the smallest live snapshot seqno, or (0, false) if
// no snapshots are live. Compaction uses this as the GC horizon: any
// version with seqno > Oldest may still be needed and cannot be
// dropped from compaction output.
func (s *SnapshotList) Oldest() (uint64, bool) {
	if s.head == nil {
		return 0, false
	}
	return s.head.seq, true
}

// GetAll returns every live seqno in ascending order. Compaction can
// use this for per-snapshot GC bucketing; Oldest is sufficient for
// a simpler "preserve anything above the floor" policy.
func (s *SnapshotList) GetAll() []uint64 {
	out := make([]uint64, 0, len(s.index))
	for n := s.head; n != nil; n = n.next {
		out = append(out, n.seq)
	}
	return out
}

// Len reports the number of live snapshots.
func (s *SnapshotList) Len() int {
	n := 0
	for cur := s.head; cur != nil; cur = cur.next {
		n++
	}
	return n
}
