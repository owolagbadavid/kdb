package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/owolagbadavid/kdb/internal/kv"
)

func TestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}

	entries := []kv.Entry{
		{Key: []byte("a"), Value: []byte("1"), Seqno: 1, Kind: kv.KindPut},
		{Key: []byte("b"), Value: nil, Seqno: 2, Kind: kv.KindDelete},
		{Key: []byte{0, 1, 2}, Value: []byte{3, 4, 5}, Seqno: 3, Kind: kv.KindPut},
	}
	for _, e := range entries {
		if err := w.Append(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	var got []kv.Entry
	if err := Replay(path, func(e kv.Entry) error {
		got = append(got, e)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(entries) {
		t.Fatalf("got %d entries, want %d", len(got), len(entries))
	}
	for i, want := range entries {
		if !bytes.Equal(got[i].Key, want.Key) || !bytes.Equal(got[i].Value, want.Value) ||
			got[i].Seqno != want.Seqno || got[i].Kind != want.Kind {
			t.Errorf("entry %d: got %+v, want %+v", i, got[i], want)
		}
	}
}

func TestReplayMissingFile(t *testing.T) {
	if err := Replay(filepath.Join(t.TempDir(), "nope"), func(kv.Entry) error {
		t.Fatal("callback should not run for missing file")
		return nil
	}); err != nil {
		t.Fatalf("missing file should return nil, got %v", err)
	}
}

func TestTornTrailingPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, _ := Create(path)
	_ = w.Append(kv.Entry{Key: []byte("a"), Value: []byte("1"), Seqno: 1, Kind: kv.KindPut})
	_ = w.Append(kv.Entry{Key: []byte("b"), Value: []byte("2"), Seqno: 2, Kind: kv.KindPut})
	_ = w.Sync()
	_ = w.Close()

	info, _ := os.Stat(path)
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatal(err)
	}

	var got []string
	if err := Replay(path, func(e kv.Entry) error {
		got = append(got, string(e.Key))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "a" {
		t.Fatalf("expected [a], got %v", got)
	}
}

func TestTornTrailingHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, _ := Create(path)
	_ = w.Append(kv.Entry{Key: []byte("a"), Value: []byte("1"), Seqno: 1, Kind: kv.KindPut})
	_ = w.Sync()
	_ = w.Close()

	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	_, _ = f.Write([]byte{0xff, 0xff, 0xff, 0xff})
	_ = f.Close()

	var n int
	if err := Replay(path, func(kv.Entry) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("got %d entries, want 1", n)
	}
}
