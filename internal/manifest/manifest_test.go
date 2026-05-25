package manifest

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadMissingFile(t *testing.T) {
	got, err := Load(t.TempDir())
	if err != nil {
		t.Fatalf("missing file should return nil error, got %v", err)
	}
	if got.Entries != nil || got.MinLogNum != 0 {
		t.Fatalf("missing file should return zero snapshot, got %+v", got)
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	want := Snapshot{
		MinLogNum: 7,
		Entries: []Entry{
			{FileNum: 1, Tier: 0, Smallest: []byte("a"), Largest: []byte("m")},
			{FileNum: 2, Tier: 0, Smallest: []byte("n"), Largest: []byte("z")},
			{FileNum: 5, Tier: 1, Smallest: []byte("a"), Largest: []byte("z")},
		},
	}
	if err := Save(dir, want); err != nil {
		t.Fatal(err)
	}
	got, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got.MinLogNum != want.MinLogNum {
		t.Errorf("MinLogNum = %d, want %d", got.MinLogNum, want.MinLogNum)
	}
	if len(got.Entries) != len(want.Entries) {
		t.Fatalf("len = %d, want %d", len(got.Entries), len(want.Entries))
	}
	for i := range want.Entries {
		w, g := want.Entries[i], got.Entries[i]
		if g.FileNum != w.FileNum || g.Tier != w.Tier ||
			!bytes.Equal(g.Smallest, w.Smallest) || !bytes.Equal(g.Largest, w.Largest) {
			t.Errorf("entry %d: got %+v, want %+v", i, g, w)
		}
	}
}

func TestSaveEmpty(t *testing.T) {
	dir := t.TempDir()
	if err := Save(dir, Snapshot{}); err != nil {
		t.Fatal(err)
	}
	got, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Entries) != 0 || got.MinLogNum != 0 {
		t.Fatalf("got %+v, want zero snapshot", got)
	}
}

func TestLoadCorruptCRC(t *testing.T) {
	dir := t.TempDir()
	_ = Save(dir, Snapshot{Entries: []Entry{{FileNum: 1, Smallest: []byte("a"), Largest: []byte("b")}}})

	path := filepath.Join(dir, filename)
	data, _ := os.ReadFile(path)
	data[len(data)-1] ^= 0xff
	_ = os.WriteFile(path, data, 0o644)

	if _, err := Load(dir); err == nil {
		t.Fatal("expected CRC error")
	}
}

func TestSaveOverwritesPrevious(t *testing.T) {
	dir := t.TempDir()
	_ = Save(dir, Snapshot{MinLogNum: 1, Entries: []Entry{{FileNum: 1, Smallest: []byte("a"), Largest: []byte("b")}}})
	_ = Save(dir, Snapshot{MinLogNum: 5, Entries: []Entry{{FileNum: 7, Tier: 2, Smallest: []byte("x"), Largest: []byte("y")}}})
	got, _ := Load(dir)
	if len(got.Entries) != 1 || got.Entries[0].FileNum != 7 || got.Entries[0].Tier != 2 {
		t.Fatalf("entries = %+v, want single FileNum=7 Tier=2", got.Entries)
	}
	if got.MinLogNum != 5 {
		t.Fatalf("MinLogNum = %d, want 5", got.MinLogNum)
	}
}

