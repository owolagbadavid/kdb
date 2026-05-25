package kdb

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/owolagbadavid/kdb/internal/compaction"
	"github.com/owolagbadavid/kdb/internal/version"
)

// sortHandles orders SSTable handles for the read path: lower tier first
// (a lower tier holds newer data, since a higher tier was produced by
// compacting lower-tier files), and within a tier highest fileNum first
// (a newer flush/compaction has a larger fileNum). Get relies on this
// order to resolve a key to its newest entry.
func sortHandles(handles []*version.Handle) {
	sort.Slice(handles, func(i, j int) bool {
		if handles[i].Tier != handles[j].Tier {
			return handles[i].Tier < handles[j].Tier
		}
		return handles[i].FileNum > handles[j].FileNum
	})
}

// compactionLoop is the background compactor. Each signal triggers a
// drain: it compacts repeatedly until no tier exceeds the trigger, so a
// tier 0 → 1 → 2 cascade completes without waiting for more signals.
func (db *DB) compactionLoop() {
	for range db.compactCh {
		for {
			done, err := db.compactOnce()
			if err != nil {
				// Best-effort: a failed compaction leaves the inputs
				// intact and the output as an orphan (removed on the
				// next Open). Stop this round; a later signal retries.
				break
			}
			if done {
				break
			}
		}
	}
	close(db.compactDone)
}

// compactOnce runs at most one compaction, returning done=true when
// there is nothing left to compact. The merge runs without db.mu held
// so reads and writes proceed concurrently; a reference on the current
// version keeps the input SSTable readers alive for the duration.
func (db *DB) compactOnce() (done bool, err error) {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return true, nil
	}
	v := db.current
	v.Ref()
	inputs := make([]compaction.Input, 0, len(v.Handles))
	for _, h := range v.Handles {
		inputs = append(inputs, compaction.Input{
			Reader: h.Reader, FileNum: h.FileNum, Tier: h.Tier,
		})
	}
	plan := compaction.Pick(inputs, db.opts.CompactionTrigger)
	if plan == nil {
		v.Unref()
		db.mu.Unlock()
		return true, nil
	}
	newNum := db.nextFileNum
	db.nextFileNum++
	db.mu.Unlock()

	out, err := compaction.Run(plan, db.dir, newNum)
	if err != nil {
		v.Unref()
		return false, err
	}
	if err := syncDir(db.dir); err != nil {
		_ = out.Reader.Close()
		_ = os.Remove(filepath.Join(db.dir, fmt.Sprintf("%06d.sst", out.FileNum)))
		v.Unref()
		return false, err
	}

	db.mu.Lock()
	err = db.applyCompactionLocked(plan, out)
	db.mu.Unlock()
	v.Unref()
	return false, err
}

// applyCompactionLocked installs a version with the compaction inputs
// removed and the merged output added. db.current is re-read here rather
// than reusing the snapshot the merge started from, because flushes may
// have added new SSTables while the merge ran.
func (db *DB) applyCompactionLocked(plan *compaction.Plan, out *compaction.Output) error {
	inputSet := map[uint64]bool{}
	for _, in := range plan.Inputs {
		inputSet[in.FileNum] = true
	}
	newHandle := version.NewHandle(
		out.Reader, out.FileNum, out.Tier, out.Smallest, out.Largest, db.onHandleZero)

	keep := make([]*version.Handle, 0, len(db.current.Handles)+1)
	for _, h := range db.current.Handles {
		if !inputSet[h.FileNum] {
			keep = append(keep, h)
		}
	}
	keep = append(keep, newHandle)
	sortHandles(keep)

	// Compaction doesn't change which WAL is the oldest live one, so
	// MinLogNum is preserved.
	if err := db.installVersionLocked(keep, db.minLogNum); err != nil {
		_ = out.Reader.Close()
		_ = os.Remove(filepath.Join(db.dir, fmt.Sprintf("%06d.sst", out.FileNum)))
		return err
	}
	return nil
}
