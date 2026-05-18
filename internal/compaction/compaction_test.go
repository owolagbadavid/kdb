package compaction

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/owolagbadavid/kdb/internal/kv"
	"github.com/owolagbadavid/kdb/internal/sstable"
)

func TestPickEmpty(t *testing.T) {
	if Pick(nil, 4) != nil {
		t.Fatal("empty list should return nil")
	}
}

func TestPickBelowTrigger(t *testing.T) {
	infos := []Input{{Tier: 0, FileNum: 1}, {Tier: 0, FileNum: 2}, {Tier: 0, FileNum: 3}}
	if Pick(infos, 4) != nil {
		t.Fatal("below trigger should return nil")
	}
}

func TestPickAtTrigger(t *testing.T) {
	infos := []Input{
		{Tier: 0, FileNum: 1}, {Tier: 0, FileNum: 2},
		{Tier: 0, FileNum: 3}, {Tier: 0, FileNum: 4},
	}
	plan := Pick(infos, 4)
	if plan == nil {
		t.Fatal("nil plan")
	}
	if len(plan.Inputs) != 4 || plan.OutTier != 1 {
		t.Fatalf("plan = %+v", plan)
	}
}

func TestPickPrefersLowerTier(t *testing.T) {
	// Both tier 0 and tier 1 hit the trigger; tier 0 wins.
	var infos []Input
	for i := 0; i < 5; i++ {
		infos = append(infos, Input{Tier: 0, FileNum: uint64(i)})
	}
	for i := 0; i < 4; i++ {
		infos = append(infos, Input{Tier: 1, FileNum: uint64(100 + i)})
	}
	plan := Pick(infos, 4)
	if plan == nil {
		t.Fatal("nil plan")
	}
	if plan.OutTier != 1 {
		t.Fatalf("expected OutTier=1 (from tier 0), got %d", plan.OutTier)
	}
	if len(plan.Inputs) != 5 {
		t.Fatalf("expected 5 tier-0 inputs, got %d", len(plan.Inputs))
	}
	for _, in := range plan.Inputs {
		if in.Tier != 0 {
			t.Fatalf("unexpected tier %d in plan", in.Tier)
		}
	}
}

func TestPickOnlyHigherTierTriggers(t *testing.T) {
	// tier 0: 2 files (below); tier 1: 4 files (triggers).
	infos := []Input{
		{Tier: 0, FileNum: 1}, {Tier: 0, FileNum: 2},
		{Tier: 1, FileNum: 10}, {Tier: 1, FileNum: 11},
		{Tier: 1, FileNum: 12}, {Tier: 1, FileNum: 13},
	}
	plan := Pick(infos, 4)
	if plan == nil {
		t.Fatal("nil plan")
	}
	if plan.OutTier != 2 || len(plan.Inputs) != 4 {
		t.Fatalf("plan = %+v", plan)
	}
}

// buildSSTable writes a small SSTable file at <dir>/<fileNum>.sst with
// the given entries and returns an opened Reader.
func buildSSTable(t *testing.T, dir string, fileNum uint64, entries []kv.Entry) *sstable.Reader {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("%06d.sst", fileNum))
	w, err := sstable.NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if err := w.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	r, err := sstable.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRunMergesAndDeduplicates(t *testing.T) {
	dir := t.TempDir()
	r1 := buildSSTable(t, dir, 1, []kv.Entry{
		{Key: []byte("a"), Value: []byte("a-old"), Seqno: 1, Kind: kv.KindPut},
		{Key: []byte("c"), Value: []byte("c1"), Seqno: 2, Kind: kv.KindPut},
	})
	r2 := buildSSTable(t, dir, 2, []kv.Entry{
		{Key: []byte("a"), Value: []byte("a-new"), Seqno: 5, Kind: kv.KindPut},
		{Key: []byte("b"), Value: []byte("b1"), Seqno: 3, Kind: kv.KindPut},
	})
	r3 := buildSSTable(t, dir, 3, []kv.Entry{
		{Key: []byte("b"), Value: nil, Seqno: 6, Kind: kv.KindDelete},
		{Key: []byte("d"), Value: []byte("d1"), Seqno: 4, Kind: kv.KindPut},
	})

	plan := &Plan{
		Inputs: []Input{
			{Reader: r1, FileNum: 1, Tier: 0},
			{Reader: r2, FileNum: 2, Tier: 0},
			{Reader: r3, FileNum: 3, Tier: 0},
		},
		OutTier: 1,
	}
	out, err := Run(plan, dir, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Reader.Close()

	if out.Tier != 1 || out.FileNum != 4 {
		t.Fatalf("out = %+v", out)
	}

	// "a" must resolve to "a-new" (seqno 5 > 1).
	e, ok, _ := out.Reader.Get([]byte("a"))
	if !ok || string(e.Value) != "a-new" || e.Seqno != 5 {
		t.Fatalf("a: ok=%v got %+v", ok, e)
	}
	// "b" must be a tombstone (seqno 6 > 3).
	e, ok, _ = out.Reader.Get([]byte("b"))
	if !ok || e.Kind != kv.KindDelete || e.Seqno != 6 {
		t.Fatalf("b: ok=%v got %+v", ok, e)
	}
	// "c" and "d" preserved.
	e, ok, _ = out.Reader.Get([]byte("c"))
	if !ok || string(e.Value) != "c1" {
		t.Fatalf("c: ok=%v got %+v", ok, e)
	}
	e, ok, _ = out.Reader.Get([]byte("d"))
	if !ok || string(e.Value) != "d1" {
		t.Fatalf("d: ok=%v got %+v", ok, e)
	}

	// Smallest/Largest should span the merged set.
	if string(out.Smallest) != "a" || string(out.Largest) != "d" {
		t.Fatalf("range: [%s, %s], want [a, d]", out.Smallest, out.Largest)
	}

	r1.Close()
	r2.Close()
	r3.Close()
}
