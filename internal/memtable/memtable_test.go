package memtable

import (
	"bytes"
	"testing"

	"github.com/owolagbadavid/kdb/internal/kv"
)

func TestPutGetRoundtrip(t *testing.T) {
	m := New()
	m.Put([]byte("hello"), []byte("world"), 1)
	e, ok := m.Get([]byte("hello"))
	if !ok {
		t.Fatal("expected key present")
	}
	if !bytes.Equal(e.Value, []byte("world")) {
		t.Fatalf("value = %q", e.Value)
	}
	if e.Kind != kv.KindPut {
		t.Fatalf("kind = %d, want KindPut", e.Kind)
	}
	if e.Seqno != 1 {
		t.Fatalf("seqno = %d, want 1", e.Seqno)
	}
}

func TestEmbeddedZeros(t *testing.T) {
	m := New()
	key := []byte{1, 0, 2, 0, 3}
	val := []byte{0, 0, 0}
	m.Put(key, val, 1)
	e, ok := m.Get(key)
	if !ok || !bytes.Equal(e.Value, val) {
		t.Fatalf("ok=%v val=%v", ok, e.Value)
	}
}

func TestOverwriteUsesNewest(t *testing.T) {
	m := New()
	m.Put([]byte("k"), []byte("v1"), 1)
	m.Put([]byte("k"), []byte("v2"), 2)
	e, _ := m.Get([]byte("k"))
	if !bytes.Equal(e.Value, []byte("v2")) || e.Seqno != 2 {
		t.Fatalf("got value=%q seqno=%d, want v2/2", e.Value, e.Seqno)
	}
}

func TestDeleteLeavesTombstone(t *testing.T) {
	m := New()
	m.Put([]byte("k"), []byte("v"), 1)
	m.Delete([]byte("k"), 2)
	e, ok := m.Get([]byte("k"))
	if !ok {
		t.Fatal("delete should leave a tombstone, not vanish")
	}
	if e.Kind != kv.KindDelete {
		t.Fatalf("kind = %d, want KindDelete", e.Kind)
	}
}

func TestIteratorSorted(t *testing.T) {
	m := New()
	for i, k := range []string{"banana", "apple", "cherry", "date"} {
		m.Put([]byte(k), []byte("v"), uint64(i+1))
	}
	want := []string{"apple", "banana", "cherry", "date"}
	it := m.NewIterator()
	defer it.Close()

	var got []string
	for ; it.Valid(); it.Next() {
		got = append(got, string(it.Key()))
	}
	if len(got) != len(want) {
		t.Fatalf("got %d keys, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("at %d: got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestIteratorSeek(t *testing.T) {
	m := New()
	for _, k := range []string{"a", "c", "e", "g"} {
		m.Put([]byte(k), []byte("v"), 1)
	}
	it := m.NewIterator()
	defer it.Close()

	it.Seek([]byte("d"))
	if !it.Valid() || string(it.Key()) != "e" {
		t.Fatalf("seek(d): valid=%v key=%q", it.Valid(), it.Key())
	}
	it.Seek([]byte("a"))
	if string(it.Key()) != "a" {
		t.Fatalf("seek(a): key=%q", it.Key())
	}
	it.Seek([]byte("z"))
	if it.Valid() {
		t.Fatalf("seek(z) should be invalid")
	}
}
