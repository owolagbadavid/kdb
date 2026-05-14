package memtable

import "github.com/owolagbadavid/kdb/internal/kv"

// Memtable is the in-memory ordered map that absorbs writes before they
// are flushed to immutable SSTables. Not safe for concurrent use; the DB
// serialises writers via a higher-level RWMutex.
type Memtable struct {
	sl *Skiplist
}

func New() *Memtable {
	return &Memtable{sl: NewSkiplist()}
}

func (m *Memtable) Put(key, value []byte, seqno uint64) {
	m.sl.Insert(key, value, seqno, kv.KindPut)
}

func (m *Memtable) Delete(key []byte, seqno uint64) {
	m.sl.Insert(key, nil, seqno, kv.KindDelete)
}

// Get returns the entry for key, if present. Callers must inspect Kind to
// distinguish a value from a tombstone — a tombstone hit means the key has
// been deleted in this memtable's window.
func (m *Memtable) Get(key []byte) (kv.Entry, bool) {
	return m.sl.Get(key)
}

func (m *Memtable) SizeBytes() int64 { return m.sl.SizeBytes() }
func (m *Memtable) Count() int       { return m.sl.Count() }

func (m *Memtable) NewIterator() kv.Iterator {
	return m.sl.NewIterator()
}
