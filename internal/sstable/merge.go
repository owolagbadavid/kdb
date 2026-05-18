package sstable

import (
	"bytes"

	"github.com/owolagbadavid/kdb/internal/kv"
)

// MergeIterator merges N sorted kv.Iterators into a single ascending
// stream. When the same key appears in multiple sources, the entry with
// the highest seqno wins and older versions are dropped silently.
//
// Inputs must each be in ascending key order and unique-keyed (memtable
// and SSTable both satisfy this). The merge iterator copies key/value
// bytes on each step, so callers may safely advance underlying iterators
// that reuse buffers.
type MergeIterator struct {
	its   []kv.Iterator
	cur   kv.Entry
	valid bool
}

func NewMergeIterator(its []kv.Iterator) *MergeIterator {
	m := &MergeIterator{its: its}
	m.advance()
	return m
}

func (m *MergeIterator) Valid() bool   { return m.valid }
func (m *MergeIterator) Key() []byte   { return m.cur.Key }
func (m *MergeIterator) Value() []byte { return m.cur.Value }
func (m *MergeIterator) Seqno() uint64 { return m.cur.Seqno }
func (m *MergeIterator) Kind() kv.Kind { return m.cur.Kind }
func (m *MergeIterator) Next()         { m.advance() }

func (m *MergeIterator) Seek(key []byte) {
	for _, it := range m.its {
		it.Seek(key)
	}
	m.advance()
}

func (m *MergeIterator) Close() error {
	var firstErr error
	for _, it := range m.its {
		if err := it.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (m *MergeIterator) advance() {
	winner := -1
	for i, it := range m.its {
		if !it.Valid() {
			continue
		}
		if winner < 0 {
			winner = i
			continue
		}
		switch bytes.Compare(it.Key(), m.its[winner].Key()) {
		case -1:
			winner = i
		case 0:
			if it.Seqno() > m.its[winner].Seqno() {
				winner = i
			}
		}
	}
	if winner < 0 {
		m.valid = false
		return
	}

	w := m.its[winner]
	m.cur = kv.Entry{
		Key:   append([]byte(nil), w.Key()...),
		Value: append([]byte(nil), w.Value()...),
		Seqno: w.Seqno(),
		Kind:  w.Kind(),
	}
	winnerKey := m.cur.Key

	w.Next()
	for i, it := range m.its {
		if i == winner {
			continue
		}
		for it.Valid() && bytes.Equal(it.Key(), winnerKey) {
			it.Next()
		}
	}
	m.valid = true
}
