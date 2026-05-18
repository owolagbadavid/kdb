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
	if got != nil {
		t.Fatalf("missing file should return nil slice, got %v", got)
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	want := []Entry{
		{FileNum: 1, Tier: 0, Smallest: []byte("a"), Largest: []byte("m")},
		{FileNum: 2, Tier: 0, Smallest: []byte("n"), Largest: []byte("z")},
		{FileNum: 5, Tier: 1, Smallest: []byte("a"), Largest: []byte("z")},
	}
	if err := Save(dir, want); err != nil {
		t.Fatal(err)
	}
	got, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].FileNum != want[i].FileNum || got[i].Tier != want[i].Tier ||
			!bytes.Equal(got[i].Smallest, want[i].Smallest) ||
			!bytes.Equal(got[i].Largest, want[i].Largest) {
			t.Errorf("entry %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestSaveEmpty(t *testing.T) {
	dir := t.TempDir()
	if err := Save(dir, nil); err != nil {
		t.Fatal(err)
	}
	got, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d entries, want 0", len(got))
	}
}

func TestLoadCorruptCRC(t *testing.T) {
	dir := t.TempDir()
	_ = Save(dir, []Entry{{FileNum: 1, Tier: 0, Smallest: []byte("a"), Largest: []byte("b")}})

	path := filepath.Join(dir, filename)
	data, _ := os.ReadFile(path)
	data[len(data)-1] ^= 0xff // flip a byte in the payload → CRC mismatch
	_ = os.WriteFile(path, data, 0o644)

	if _, err := Load(dir); err == nil {
		t.Fatal("expected CRC error")
	}
}

func TestSaveOverwritesPrevious(t *testing.T) {
	dir := t.TempDir()
	_ = Save(dir, []Entry{{FileNum: 1, Tier: 0, Smallest: []byte("a"), Largest: []byte("b")}})
	_ = Save(dir, []Entry{{FileNum: 7, Tier: 2, Smallest: []byte("x"), Largest: []byte("y")}})
	got, _ := Load(dir)
	if len(got) != 1 || got[0].FileNum != 7 || got[0].Tier != 2 {
		t.Fatalf("got %+v, want single entry FileNum=7 Tier=2", got)
	}
}
