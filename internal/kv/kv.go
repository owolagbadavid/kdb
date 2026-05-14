// Package kv defines shared types used by the memtable, WAL, and SSTable
// layers: the operation kind, the entry shape moved between layers, and
// the iterator contract every sorted source implements.
package kv

type Kind uint8

const (
	KindPut    Kind = 1
	KindDelete Kind = 2
)

// Entry is the unit of value moved between layers. Value is meaningful
// only when Kind == KindPut; for KindDelete it is nil. Seqno is a
// monotonically increasing per-write counter used to break ties when the
// same key appears in multiple layers.
type Entry struct {
	Key   []byte
	Value []byte
	Seqno uint64
	Kind  Kind
}

// Iterator walks a sorted source in ascending key order. Implementations
// include the memtable, individual SSTables, and merged views across
// multiple sources.
type Iterator interface {
	Valid() bool
	Key() []byte
	Value() []byte
	Seqno() uint64
	Kind() Kind
	Next()
	Seek(key []byte)
	Close() error
}
