package memtable

import (
	"bytes"

	"github.com/owolagbadavid/kdb/internal/kv"
)

// Memtable is the in-memory ordered map that absorbs writes before they
// are flushed to immutable SSTables. Internally it stores entries keyed
// by internal key (user key + 8-byte trailer), so a single user key may
// hold multiple versions distinguishable by seqno. Not safe for
// concurrent use; the DB serialises writers via a higher-level RWMutex.
type Memtable struct {
	sl *Skiplist
}

func New() *Memtable {
	return &Memtable{sl: NewSkiplist()}
}

// Put records a value for userKey at seqno. Multiple Put calls with the
// same userKey at different seqnos coexist; the most recent (highest
// seqno) one is returned by Get.
func (m *Memtable) Put(userKey, value []byte, seqno uint64) {
	ik := kv.MakeInternalKey(userKey, seqno, kv.KindPut)
	m.sl.Insert(ik, value)
}

// Delete records a tombstone for userKey at seqno.
func (m *Memtable) Delete(userKey []byte, seqno uint64) {
	ik := kv.MakeInternalKey(userKey, seqno, kv.KindDelete)
	m.sl.Insert(ik, nil)
}

// Get returns the newest version of userKey, regardless of seqno.
// Callers that need a snapshot ceiling should use GetAt.
func (m *Memtable) Get(userKey []byte) (kv.Entry, bool) {
	return m.GetAt(userKey, kv.SeqnoMax)
}

// GetAt returns the newest version of userKey with seqno <= snapSeq, or
// (Entry{}, false) if no such version is present. A tombstone hit
// returns true with Kind == KindDelete; callers translate that into
// "deleted in this snapshot."
func (m *Memtable) GetAt(userKey []byte, snapSeq uint64) (kv.Entry, bool) {
	seek := kv.MakeInternalKey(userKey, snapSeq, kv.KindMax)
	it := m.sl.NewIterator()
	it.Seek(seek)
	if !it.Valid() {
		return kv.Entry{}, false
	}
	ik := it.Key()
	if len(ik) < kv.InternalKeyTrailerLen {
		return kv.Entry{}, false
	}
	uk := kv.UserKeyOf(ik)
	if !bytes.Equal(uk, userKey) {
		return kv.Entry{}, false
	}
	seqno, kind := kv.DecodeTrailer(ik)
	return kv.Entry{
		Key:   append([]byte(nil), uk...),
		Value: append([]byte(nil), it.Value()...),
		Seqno: seqno,
		Kind:  kind,
	}, true
}

func (m *Memtable) SizeBytes() int64 { return m.sl.SizeBytes() }
func (m *Memtable) Count() int       { return m.sl.Count() }

// NewIterator returns an iterator over all versions in internal-key
// order: user key ascending, then seqno descending within the same
// user key. Key() returns the user key; Seqno()/Kind() decode the
// trailer.
func (m *Memtable) NewIterator() kv.Iterator {
	return &mtIterator{slIt: m.sl.NewIterator()}
}

type mtIterator struct {
	slIt *Iterator
}

func (it *mtIterator) Valid() bool   { return it.slIt.Valid() }
func (it *mtIterator) Key() []byte   { return kv.UserKeyOf(it.slIt.Key()) }
func (it *mtIterator) Value() []byte { return it.slIt.Value() }
func (it *mtIterator) Seqno() uint64 {
	seqno, _ := kv.DecodeTrailer(it.slIt.Key())
	return seqno
}
func (it *mtIterator) Kind() kv.Kind {
	_, kind := kv.DecodeTrailer(it.slIt.Key())
	return kind
}
func (it *mtIterator) Next() { it.slIt.Next() }

// Seek positions at the first version (any seqno) of the smallest user
// key >= target.
func (it *mtIterator) Seek(target []byte) {
	it.slIt.Seek(kv.MakeInternalKey(target, kv.SeqnoMax, kv.KindMax))
}

func (it *mtIterator) Close() error { return it.slIt.Close() }
