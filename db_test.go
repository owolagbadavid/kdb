package kdb_test

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/owolagbadavid/kdb"
)

func TestPutGet(t *testing.T) {
	db, err := kdb.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	got, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("got %q, want v", got)
	}
}

func TestGetMissing(t *testing.T) {
	db, _ := kdb.Open(t.TempDir(), nil)
	defer db.Close()

	_, err := db.Get([]byte("nope"))
	if !errors.Is(err, kdb.ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestDeleteHidesKey(t *testing.T) {
	db, _ := kdb.Open(t.TempDir(), nil)
	defer db.Close()

	_ = db.Put([]byte("k"), []byte("v"))
	_ = db.Delete([]byte("k"))
	_, err := db.Get([]byte("k"))
	if !errors.Is(err, kdb.ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestOverwriteThenGet(t *testing.T) {
	db, _ := kdb.Open(t.TempDir(), nil)
	defer db.Close()

	_ = db.Put([]byte("k"), []byte("v1"))
	_ = db.Put([]byte("k"), []byte("v2"))
	got, _ := db.Get([]byte("k"))
	if !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("got %q, want v2", got)
	}
}

// TestCrashRecovery writes a batch, closes cleanly, reopens, and verifies
// every key is recoverable from the WAL.
func TestCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	db, err := kdb.Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	const n = 1000
	for i := 0; i < n; i++ {
		k := []byte{byte(i >> 8), byte(i)}
		v := []byte{byte(i)}
		if err := db.Put(k, v); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := kdb.Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	for i := 0; i < n; i++ {
		k := []byte{byte(i >> 8), byte(i)}
		got, err := db2.Get(k)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if len(got) != 1 || got[0] != byte(i) {
			t.Fatalf("get %d: got %v", i, got)
		}
	}
}

// TestRecoveryWithoutClose simulates a crash: the WAL fsyncs per write, so
// abandoning the *DB without Close must still leave every synced record
// recoverable on reopen.
func TestRecoveryWithoutClose(t *testing.T) {
	dir := t.TempDir()
	db, _ := kdb.Open(dir, nil)
	const n = 500
	for i := 0; i < n; i++ {
		k := []byte{byte(i >> 8), byte(i)}
		v := []byte{byte(i)}
		_ = db.Put(k, v)
	}
	// Intentionally no Close — simulate a crash. db1 is abandoned.
	_ = db

	db2, err := kdb.Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	for i := 0; i < n; i++ {
		k := []byte{byte(i >> 8), byte(i)}
		got, err := db2.Get(k)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if got[0] != byte(i) {
			t.Fatalf("get %d: got %v", i, got)
		}
	}
}

func TestRecoveryReplaysDeletes(t *testing.T) {
	dir := t.TempDir()
	db, _ := kdb.Open(dir, nil)
	_ = db.Put([]byte("a"), []byte("1"))
	_ = db.Put([]byte("b"), []byte("2"))
	_ = db.Delete([]byte("a"))
	_ = db.Close()

	db2, _ := kdb.Open(dir, nil)
	defer db2.Close()
	if _, err := db2.Get([]byte("a")); !errors.Is(err, kdb.ErrNotFound) {
		t.Fatalf("a: got %v, want ErrNotFound", err)
	}
	got, err := db2.Get([]byte("b"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("2")) {
		t.Fatalf("b: got %q, want 2", got)
	}
}

// TestFlushAndReopen forces multiple flushes by lowering the memtable
// threshold, then verifies every key survives through SSTables on reopen.
func TestFlushAndReopen(t *testing.T) {
	dir := t.TempDir()
	opts := &kdb.Options{MemtableSizeBytes: 256}

	db, err := kdb.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	const n = 500
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key-%04d", i))
		v := []byte(fmt.Sprintf("val-%d", i))
		if err := db.Put(k, v); err != nil {
			t.Fatal(err)
		}
	}
	s := db.Stats()
	if len(s.SSTables) == 0 {
		t.Fatalf("expected at least one SSTable, got 0 (stats=%+v)", s)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := kdb.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key-%04d", i))
		want := fmt.Sprintf("val-%d", i)
		got, err := db2.Get(k)
		if err != nil {
			t.Fatalf("get %q: %v", k, err)
		}
		if string(got) != want {
			t.Fatalf("get %q: got %q, want %q", k, got, want)
		}
	}
}

// TestFlushTombstoneShadowsOlder ensures a tombstone in a newer SSTable
// hides a value in an older SSTable.
func TestFlushTombstoneShadowsOlder(t *testing.T) {
	dir := t.TempDir()
	opts := &kdb.Options{MemtableSizeBytes: 1} // flush after almost every write

	db, _ := kdb.Open(dir, opts)
	if err := db.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	db2, _ := kdb.Open(dir, opts)
	defer db2.Close()
	if _, err := db2.Get([]byte("k")); !errors.Is(err, kdb.ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound (tombstone should shadow)", err)
	}
}

// TestFlushNewerValueShadowsOlder ensures the newest SSTable wins for the
// same key across multiple flushes.
func TestFlushNewerValueShadowsOlder(t *testing.T) {
	dir := t.TempDir()
	opts := &kdb.Options{MemtableSizeBytes: 1}

	db, _ := kdb.Open(dir, opts)
	_ = db.Put([]byte("k"), []byte("old"))
	_ = db.Put([]byte("k"), []byte("new"))
	got, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("new")) {
		t.Fatalf("got %q, want new", got)
	}
	_ = db.Close()

	db2, _ := kdb.Open(dir, opts)
	defer db2.Close()
	got, err = db2.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("new")) {
		t.Fatalf("after reopen: got %q, want new", got)
	}
}
