package bloom

import (
	"fmt"
	"testing"
)

func TestHashIsDeterministic(t *testing.T) {
	key := []byte("hello world")
	a := Hash(key)
	b := Hash(key)
	if a != b {
		t.Fatalf("Hash not deterministic: %x vs %x", a, b)
	}
	if Hash([]byte("hello world!")) == a {
		t.Fatalf("different keys hashed to the same value")
	}
}

func TestNewSizing(t *testing.T) {
	f := New(1000, 0.01)
	if f.K < 1 {
		t.Fatalf("K must be at least 1, got %d", f.K)
	}
	if f.M < 1000 {
		t.Fatalf("M (%d) suspiciously small for n=1000", f.M)
	}
	bitsPerKey := float64(f.M) / 1000.0
	if bitsPerKey < 8 || bitsPerKey > 12 {
		t.Errorf("bits/key = %.2f, expected ~9.6", bitsPerKey)
	}
	if want := (f.M + 7) / 8; uint64(len(f.Bits)) != want {
		t.Errorf("Bits length = %d, want %d", len(f.Bits), want)
	}
}

func TestAddContainsRoundtrip(t *testing.T) {
	f := New(100, 0.01)
	for i := 0; i < 100; i++ {
		f.Add([]byte(fmt.Sprintf("key-%04d", i)))
	}
	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("key-%04d", i))
		if !f.Contains(k) {
			t.Errorf("Contains(%q) = false, want true (no false negatives allowed)", k)
		}
	}
}

func TestEmptyFilterRejectsAll(t *testing.T) {
	// A New'd filter with nothing added is all zero bits, so Contains
	// must return false for any key (no probes can hit a set bit).
	f := New(100, 0.01)
	for i := 0; i < 50; i++ {
		k := []byte(fmt.Sprintf("absent-%d", i))
		if f.Contains(k) {
			t.Errorf("empty filter accepted %q", k)
		}
	}
}

func TestNilFilterAccepts(t *testing.T) {
	var f *Filter
	if !f.Contains([]byte("any")) {
		t.Fatal("nil filter should accept any key")
	}
}

func TestFalsePositiveRateInRange(t *testing.T) {
	const n = 5000
	f := New(n, 0.01)
	for i := 0; i < n; i++ {
		f.Add([]byte(fmt.Sprintf("present-%d", i)))
	}
	fp := 0
	const trials = 10000
	for i := 0; i < trials; i++ {
		k := []byte(fmt.Sprintf("absent-%d", i))
		if f.Contains(k) {
			fp++
		}
	}
	rate := float64(fp) / trials
	if rate > 0.05 {
		t.Errorf("false-positive rate %.4f (%d/%d), expected <0.05", rate, fp, trials)
	}
}
