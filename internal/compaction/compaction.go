// Package compaction selects and executes a size-tiered SSTable merge.
//
// Pick groups SSTables by tier and returns the lowest tier that has
// accumulated `trigger` files (typically 4). Run merges the planned
// inputs into a single new SSTable in the next tier, dropping older
// versions of keys via the merge iterator's seqno-wins rule.
//
// The caller is responsible for the manifest swap and for closing /
// deleting the input SSTables after Run succeeds.
package compaction

import (
	"fmt"
	"path/filepath"

	"github.com/owolagbadavid/kdb/internal/kv"
	"github.com/owolagbadavid/kdb/internal/sstable"
)

type Input struct {
	Reader  *sstable.Reader
	FileNum uint64
	Tier    int
}

type Plan struct {
	Inputs  []Input
	OutTier int
}

type Output struct {
	Reader   *sstable.Reader
	FileNum  uint64
	Tier     int
	Smallest []byte
	Largest  []byte
}

// Pick returns a Plan for the lowest tier whose file count is at least
// `trigger`, or nil if no tier qualifies.
func Pick(infos []Input, trigger int) *Plan {
	if trigger <= 0 || len(infos) == 0 {
		return nil
	}
	counts := map[int]int{}
	for _, in := range infos {
		counts[in.Tier]++
	}
	chosen := -1
	for tier, c := range counts {
		if c < trigger {
			continue
		}
		if chosen < 0 || tier < chosen {
			chosen = tier
		}
	}
	if chosen < 0 {
		return nil
	}
	var inputs []Input
	for _, in := range infos {
		if in.Tier == chosen {
			inputs = append(inputs, in)
		}
	}
	return &Plan{Inputs: inputs, OutTier: chosen + 1}
}

// Run merges plan.Inputs into a single SSTable in dir named
// `<newFileNum>.sst`. The caller installs Output (manifest swap) and
// disposes Inputs (close readers, remove files).
func Run(plan *Plan, dir string, newFileNum uint64) (*Output, error) {
	if plan == nil || len(plan.Inputs) == 0 {
		return nil, fmt.Errorf("compaction: empty plan")
	}

	its := make([]kv.Iterator, 0, len(plan.Inputs))
	for _, in := range plan.Inputs {
		its = append(its, in.Reader.NewIterator())
	}
	merge := sstable.NewMergeIterator(its)
	defer merge.Close()

	name := fmt.Sprintf("%06d.sst", newFileNum)
	path := filepath.Join(dir, name)
	w, err := sstable.NewWriter(path)
	if err != nil {
		return nil, err
	}
	for ; merge.Valid(); merge.Next() {
		e := kv.Entry{
			Key:   merge.Key(),
			Value: merge.Value(),
			Seqno: merge.Seqno(),
			Kind:  merge.Kind(),
		}
		if err := w.Add(e); err != nil {
			w.Abort()
			return nil, err
		}
	}
	if err := w.Finish(); err != nil {
		w.Abort()
		return nil, err
	}

	r, err := sstable.Open(path)
	if err != nil {
		return nil, err
	}
	return &Output{
		Reader:   r,
		FileNum:  newFileNum,
		Tier:     plan.OutTier,
		Smallest: r.SmallestKey(),
		Largest:  r.LargestKey(),
	}, nil
}
