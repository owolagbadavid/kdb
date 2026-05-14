package sstable

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/owolagbadavid/kdb/internal/kv"
)

func TestWriteReadRoundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "001.sst")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}

	entries := []kv.Entry{
		{Key: []byte("apple"), Value: []byte("red"), Seqno: 1, Kind: kv.KindPut},
		{Key: []byte("banana"), Value: []byte("yellow"), Seqno: 2, Kind: kv.KindPut},
		{Key: []byte("cherry"), Value: nil, Seqno: 3, Kind: kv.KindDelete},
		{Key: []byte("date"), Value: []byte{0, 1, 2}, Seqno: 4, Kind: kv.KindPut},
	}
	for _, e := range entries {
		if err := w.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	for _, want := range entries {
		got, ok, err := r.Get(want.Key)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Errorf("missing key %q", want.Key)
			continue
		}
		if !bytes.Equal(got.Key, want.Key) || !bytes.Equal(got.Value, want.Value) ||
			got.Kind != want.Kind || got.Seqno != want.Seqno {
			t.Errorf("got %+v, want %+v", got, want)
		}
	}
}

func TestGetMissingBeforeAndAfter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "001.sst")
	w, _ := NewWriter(path)
	for _, k := range []string{"m", "n", "o"} {
		_ = w.Add(kv.Entry{Key: []byte(k), Value: []byte("v"), Seqno: 1, Kind: kv.KindPut})
	}
	_ = w.Finish()

	r, _ := Open(path)
	defer r.Close()

	for _, k := range []string{"a", "z"} {
		_, ok, err := r.Get([]byte(k))
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Errorf("expected miss for %q", k)
		}
	}

	for _, k := range []string{"ma", "nz"} { // between/around existing keys
		_, ok, _ := r.Get([]byte(k))
		if ok {
			t.Errorf("expected miss for %q", k)
		}
	}
}

func TestSparseIndexAcrossManyKeys(t *testing.T) {
	// Force more than `indexInterval` records so we exercise the sparse
	// index (multiple entries) and the linear-scan-within-block path.
	path := filepath.Join(t.TempDir(), "001.sst")
	w, _ := NewWriter(path)
	const n = 200
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key-%04d", i))
		v := []byte(fmt.Sprintf("val-%d", i))
		if err := w.Add(kv.Entry{Key: k, Value: v, Seqno: uint64(i + 1), Kind: kv.KindPut}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	r, _ := Open(path)
	defer r.Close()
	if len(r.index) < 2 {
		t.Fatalf("expected sparse index with multiple entries, got %d", len(r.index))
	}

	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key-%04d", i))
		e, ok, err := r.Get(k)
		if err != nil || !ok {
			t.Fatalf("get %q: ok=%v err=%v", k, ok, err)
		}
		if string(e.Value) != fmt.Sprintf("val-%d", i) {
			t.Fatalf("get %q: value %q", k, e.Value)
		}
	}
}

func TestIteratorWalksAllInOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "001.sst")
	w, _ := NewWriter(path)
	keys := []string{"a", "b", "c", "d", "e"}
	for i, k := range keys {
		_ = w.Add(kv.Entry{Key: []byte(k), Value: []byte{byte(i)}, Seqno: uint64(i + 1), Kind: kv.KindPut})
	}
	_ = w.Finish()

	r, _ := Open(path)
	defer r.Close()

	it := r.NewIterator()
	defer it.Close()
	var got []string
	for ; it.Valid(); it.Next() {
		got = append(got, string(it.Key()))
	}
	if len(got) != len(keys) {
		t.Fatalf("got %d keys, want %d", len(got), len(keys))
	}
	for i := range keys {
		if got[i] != keys[i] {
			t.Fatalf("at %d: %q vs %q", i, got[i], keys[i])
		}
	}
}

func TestIteratorSeek(t *testing.T) {
	path := filepath.Join(t.TempDir(), "001.sst")
	w, _ := NewWriter(path)
	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("key-%04d", i))
		_ = w.Add(kv.Entry{Key: k, Value: []byte("v"), Seqno: 1, Kind: kv.KindPut})
	}
	_ = w.Finish()

	r, _ := Open(path)
	defer r.Close()

	it := r.NewIterator()
	defer it.Close()

	it.Seek([]byte("key-0050"))
	if !it.Valid() || string(it.Key()) != "key-0050" {
		t.Fatalf("seek exact: valid=%v key=%q", it.Valid(), it.Key())
	}
	it.Seek([]byte("key-0049a")) // between 0049 and 0050
	if string(it.Key()) != "key-0050" {
		t.Fatalf("seek between: key=%q", it.Key())
	}
	it.Seek([]byte("zzz"))
	if it.Valid() {
		t.Fatalf("seek past end should be invalid")
	}
}

func TestAddRejectsNonAscending(t *testing.T) {
	path := filepath.Join(t.TempDir(), "001.sst")
	w, _ := NewWriter(path)
	defer w.Abort()
	_ = w.Add(kv.Entry{Key: []byte("b"), Value: []byte("1"), Seqno: 1, Kind: kv.KindPut})
	if err := w.Add(kv.Entry{Key: []byte("a"), Value: []byte("2"), Seqno: 2, Kind: kv.KindPut}); err == nil {
		t.Fatal("expected ascending-key error")
	}
}
