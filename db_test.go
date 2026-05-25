package kdb_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/owolagbadavid/kdb"
)

// waitForStats polls db.Stats() until cond holds or a timeout elapses.
// Needed because compaction runs in a background goroutine, so SSTable
// counts settle asynchronously after writes.
func waitForStats(t *testing.T, db *kdb.DB, cond func(kdb.Stats) bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond(db.Stats()) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s (last stats: %+v)", what, db.Stats())
}

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

// TestCompactionShrinksSSTables: with a trigger of 4, several flushes
// should collapse into fewer files than the number of flushes performed.
func TestCompactionShrinksSSTables(t *testing.T) {
	dir := t.TempDir()
	opts := &kdb.Options{MemtableSizeBytes: 1, CompactionTrigger: 4}

	db, _ := kdb.Open(dir, opts)
	defer db.Close()
	const n = 20
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%02d", i))
		_ = db.Put(k, []byte(fmt.Sprintf("v%d", i)))
	}
	waitForStats(t, db, func(s kdb.Stats) bool {
		return len(s.SSTables) < n
	}, "background compaction to shrink the sstable count")
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%02d", i))
		got, err := db.Get(k)
		if err != nil {
			t.Fatalf("get %s: %v", k, err)
		}
		if string(got) != fmt.Sprintf("v%d", i) {
			t.Fatalf("get %s: got %q", k, got)
		}
	}
}

// TestCompactionDropsOlderVersion: two writes of the same key separated
// by a flush, then a compaction merges them and the older value is gone.
func TestCompactionDropsOlderVersion(t *testing.T) {
	dir := t.TempDir()
	opts := &kdb.Options{MemtableSizeBytes: 1, CompactionTrigger: 2}

	db, _ := kdb.Open(dir, opts)
	defer db.Close()
	_ = db.Put([]byte("k"), []byte("v1"))
	_ = db.Put([]byte("k"), []byte("v2"))
	// CompactionTrigger=2: the two flushed SSTables merge in the background.
	waitForStats(t, db, func(s kdb.Stats) bool {
		return len(s.SSTables) == 1 && s.SSTables[0].Tier == 1
	}, "a single tier-1 sstable")
	got, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("got %q, want v2", got)
	}
}

// TestCompactionPropagatesTombstone: a tombstone written after a value
// must continue to shadow that value after the two SSTables are merged.
func TestCompactionPropagatesTombstone(t *testing.T) {
	dir := t.TempDir()
	opts := &kdb.Options{MemtableSizeBytes: 1, CompactionTrigger: 2}

	db, _ := kdb.Open(dir, opts)
	_ = db.Put([]byte("k"), []byte("v"))
	_ = db.Delete([]byte("k"))
	if _, err := db.Get([]byte("k")); !errors.Is(err, kdb.ErrNotFound) {
		t.Fatalf("post-delete: got %v, want ErrNotFound", err)
	}
	_ = db.Close()

	db2, _ := kdb.Open(dir, opts)
	defer db2.Close()
	if _, err := db2.Get([]byte("k")); !errors.Is(err, kdb.ErrNotFound) {
		t.Fatalf("after reopen: got %v, want ErrNotFound", err)
	}
}

// TestCompactionCascadesAcrossFlushes: with trigger=2, the background
// compactor drains each round fully — a single signal compacts tier 0,
// then tier 1, then tier 2 — so a multi-tier file emerges from a burst
// of flushes.
func TestCompactionCascadesAcrossFlushes(t *testing.T) {
	dir := t.TempDir()
	opts := &kdb.Options{MemtableSizeBytes: 1, CompactionTrigger: 2}

	db, _ := kdb.Open(dir, opts)
	defer db.Close()
	const n = 8
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%d", i))
		_ = db.Put(k, []byte(fmt.Sprintf("v%d", i)))
	}
	waitForStats(t, db, func(s kdb.Stats) bool {
		for _, info := range s.SSTables {
			if info.Tier >= 2 {
				return true
			}
		}
		return false
	}, "a tier-2 (or higher) sstable from the cascade")
	for i := 0; i < n; i++ {
		got, err := db.Get([]byte(fmt.Sprintf("k%d", i)))
		if err != nil {
			t.Fatalf("get k%d: %v", i, err)
		}
		if string(got) != fmt.Sprintf("v%d", i) {
			t.Fatalf("get k%d: got %q", i, got)
		}
	}
}

// TestOpenRemovesOrphanSSTable: a .sst file on disk but not in the manifest
// is treated as an orphan from an interrupted compaction and is deleted.
func TestOpenRemovesOrphanSSTable(t *testing.T) {
	dir := t.TempDir()
	opts := &kdb.Options{MemtableSizeBytes: 1}

	db, _ := kdb.Open(dir, opts)
	_ = db.Put([]byte("k"), []byte("v"))
	_ = db.Close()

	orphan := filepath.Join(dir, "000099.sst")
	if err := os.WriteFile(orphan, []byte("junk"), 0o644); err != nil {
		t.Fatal(err)
	}

	db2, err := kdb.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan should have been removed, got err=%v", err)
	}
}

// TestFlushRetryDoesNotLoseData: the flusher retries a failed flush
// rather than dropping the immutable. The one-shot fault fails the
// first flushOne attempt; subsequent attempts succeed and the data is
// readable + survives reopen.
func TestFlushRetryDoesNotLoseData(t *testing.T) {
	dir := t.TempDir()
	opts := &kdb.Options{MemtableSizeBytes: 1, MaxImmutableMemtables: 4, CompactionTrigger: 1024}

	db, err := kdb.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Arm a one-shot fault. The first flushOne call fails; the
	// immutable stays at the head of the queue. The next rotation's
	// signal (or our explicit one) drives a retry.
	db.SetFlushFaultForTest(func() error { return errors.New("inject") })

	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("put a: %v", err)
	}
	if err := db.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("put b: %v", err)
	}

	// Wait for the queue to drain — the second flushOne (after the fault
	// cleared) and any subsequent ones succeed.
	waitForStats(t, db, func(s kdb.Stats) bool {
		return s.ImmutableCount == 0 && len(s.SSTables) >= 1
	}, "flusher to drain the queue after the one-shot fault")

	for k, want := range map[string]string{"a": "1", "b": "2"} {
		got, err := db.Get([]byte(k))
		if err != nil {
			t.Fatalf("get %s: %v", k, err)
		}
		if string(got) != want {
			t.Fatalf("get %s: got %q, want %q", k, got, want)
		}
	}
	_ = db.Close()

	db2, _ := kdb.Open(dir, opts)
	defer db2.Close()
	for k, want := range map[string]string{"a": "1", "b": "2"} {
		got, err := db2.Get([]byte(k))
		if err != nil {
			t.Fatalf("reopen get %s: %v", k, err)
		}
		if string(got) != want {
			t.Fatalf("reopen get %s: got %q, want %q", k, got, want)
		}
	}
}

// TestGetSurvivesConcurrentMutation: readers hammer Get on a key set
// while a writer continuously rewrites those same keys, driving flushes
// and background compactions. Every key is always present, so every Get
// must succeed. Run under -race to validate the version refcounting.
func TestGetSurvivesConcurrentMutation(t *testing.T) {
	dir := t.TempDir()
	opts := &kdb.Options{MemtableSizeBytes: 64, CompactionTrigger: 3}
	db, err := kdb.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const keys = 50
	for i := 0; i < keys; i++ {
		if err := db.Put([]byte(fmt.Sprintf("key-%03d", i)), []byte(fmt.Sprintf("v-%03d", i))); err != nil {
			t.Fatal(err)
		}
	}

	var readerWG, writerWG sync.WaitGroup
	stop := make(chan struct{})

	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		for n := 0; ; n++ {
			select {
			case <-stop:
				return
			default:
			}
			i := n % keys
			_ = db.Put([]byte(fmt.Sprintf("key-%03d", i)), []byte(fmt.Sprintf("v-%03d", i)))
		}
	}()

	for r := 0; r < 4; r++ {
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			for j := 0; j < 2000; j++ {
				i := j % keys
				got, err := db.Get([]byte(fmt.Sprintf("key-%03d", i)))
				if err != nil {
					t.Errorf("get key-%03d: %v", i, err)
					return
				}
				if string(got) != fmt.Sprintf("v-%03d", i) {
					t.Errorf("get key-%03d: got %q", i, got)
					return
				}
			}
		}()
	}

	readerWG.Wait()
	close(stop)
	writerWG.Wait()
}

// TestReadDuringCompaction: readers verify a fixed, never-modified key
// set while a writer floods distinct new keys, churning the SSTable set
// through flushes and compactions. Retired SSTables must stay readable
// until in-flight Gets release them.
func TestReadDuringCompaction(t *testing.T) {
	dir := t.TempDir()
	opts := &kdb.Options{MemtableSizeBytes: 64, CompactionTrigger: 3}
	db, err := kdb.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const fixed = 40
	for i := 0; i < fixed; i++ {
		if err := db.Put([]byte(fmt.Sprintf("fixed-%03d", i)), []byte(fmt.Sprintf("val-%03d", i))); err != nil {
			t.Fatal(err)
		}
	}

	var readerWG, writerWG sync.WaitGroup
	stop := make(chan struct{})

	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		for n := 0; ; n++ {
			select {
			case <-stop:
				return
			default:
			}
			_ = db.Put([]byte(fmt.Sprintf("churn-%08d", n)), []byte("x"))
		}
	}()

	for r := 0; r < 4; r++ {
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			for j := 0; j < 3000; j++ {
				i := j % fixed
				got, err := db.Get([]byte(fmt.Sprintf("fixed-%03d", i)))
				if err != nil {
					t.Errorf("get fixed-%03d: %v", i, err)
					return
				}
				if string(got) != fmt.Sprintf("val-%03d", i) {
					t.Errorf("get fixed-%03d: got %q", i, got)
					return
				}
			}
		}()
	}

	readerWG.Wait()
	close(stop)
	writerWG.Wait()
}

// TestCloseWaitsForCompactor: Close is called while compactions are in
// flight. It must block until the compactor has stopped cleanly — no
// interrupted-write temp files left behind, all data intact on reopen.
func TestCloseWaitsForCompactor(t *testing.T) {
	dir := t.TempDir()
	opts := &kdb.Options{MemtableSizeBytes: 32, CompactionTrigger: 2}
	db, err := kdb.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}

	const n = 400
	for i := 0; i < n; i++ {
		if err := db.Put([]byte(fmt.Sprintf("key-%05d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	for _, pat := range []string{"*" + ".sst.tmp", "*.new", "MANIFEST.tmp"} {
		m, _ := filepath.Glob(filepath.Join(dir, pat))
		if len(m) != 0 {
			t.Fatalf("leftover temp files matching %q: %v", pat, m)
		}
	}

	db2, err := kdb.Open(dir, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	for i := 0; i < n; i++ {
		got, err := db2.Get([]byte(fmt.Sprintf("key-%05d", i)))
		if err != nil {
			t.Fatalf("get key-%05d after reopen: %v", i, err)
		}
		if string(got) != "value" {
			t.Fatalf("get key-%05d: got %q", i, got)
		}
	}
}

// TestRotateDoesNotBlockOnSlowFlush: a slow flush in the background
// must not block subsequent rotations as long as the immutable queue
// has room. Measures Put latency under a deliberately stalled flush.
func TestRotateDoesNotBlockOnSlowFlush(t *testing.T) {
	dir := t.TempDir()
	opts := &kdb.Options{MemtableSizeBytes: 1, MaxImmutableMemtables: 8, CompactionTrigger: 1024}
	db, err := kdb.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	release := make(chan struct{})
	db.SetFlushFaultForTest(func() error {
		<-release
		return nil
	})

	// First Put rotates; the flusher starts but blocks at the fault.
	if err := db.Put([]byte("k0"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	// More Puts each rotate (memtable=1 byte) and queue. Queue cap=8
	// has plenty of room while the in-flight flush is stalled, so Puts
	// should return quickly.
	for i := 1; i < 5; i++ {
		start := time.Now()
		if err := db.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		if d := time.Since(start); d > 200*time.Millisecond {
			t.Errorf("put %d took %v; expected fast progress while flush is blocked", i, d)
		}
	}

	close(release) // let the flusher finish
}

// TestMemtableBackpressureBlocksAtLimit: when the queue is at
// MaxImmutableMemtables, the next rotation stalls until a flush
// completes and frees a slot. The seam is now real, not dormant.
func TestMemtableBackpressureBlocksAtLimit(t *testing.T) {
	dir := t.TempDir()
	opts := &kdb.Options{MemtableSizeBytes: 1, MaxImmutableMemtables: 1, CompactionTrigger: 1024}
	db, err := kdb.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	release := make(chan struct{})
	db.SetFlushFaultForTest(func() error {
		<-release
		return nil
	})

	// First Put: rotates → queue=1=max. Flusher starts → blocks at the fault.
	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}

	// Second Put: writes into the new mt, hits threshold, tries to
	// rotate → stalls because queue is at limit. Run in a goroutine.
	done := make(chan error, 1)
	go func() { done <- db.Put([]byte("b"), []byte("2")) }()
	select {
	case err := <-done:
		t.Fatalf("Put B should have stalled, but returned err=%v", err)
	case <-time.After(80 * time.Millisecond):
		// expected: stalled
	}

	close(release) // unblock the flush; queue clears; Put B's rotation proceeds
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Put B after release: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Put B never unblocked after flush release")
	}

	// Both keys readable.
	for k, v := range map[string]string{"a": "1", "b": "2"} {
		got, err := db.Get([]byte(k))
		if err != nil {
			t.Fatalf("get %s: %v", k, err)
		}
		if string(got) != v {
			t.Fatalf("get %s: got %q, want %q", k, got, v)
		}
	}
}

// TestMultiWALReplay: simulate a crash with several unflushed
// immutables by closing the DB while a permanently-failing flush leaves
// the queue intact. On reopen, every WAL file is replayed and the data
// is recovered.
func TestMultiWALReplay(t *testing.T) {
	dir := t.TempDir()
	opts := &kdb.Options{MemtableSizeBytes: 1, MaxImmutableMemtables: 8, CompactionTrigger: 1024}
	db, err := kdb.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Re-arming fault: every flush attempt fails. The queue fills up
	// across N Puts and stays full until Close.
	var fault func() error
	fault = func() error {
		db.SetFlushFaultForTest(fault)
		return errors.New("inject")
	}
	db.SetFlushFaultForTest(fault)

	const n = 5
	for i := 0; i < n; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("put k%d: %v", i, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// On reopen (no fault), each WAL is replayed into the immutable
	// queue; the flusher drains it normally.
	db2, err := kdb.Open(dir, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	waitForStats(t, db2, func(s kdb.Stats) bool {
		return s.ImmutableCount == 0
	}, "flusher to drain the recovered queue")
	for i := 0; i < n; i++ {
		got, err := db2.Get([]byte(fmt.Sprintf("k%d", i)))
		if err != nil {
			t.Fatalf("get k%d: %v", i, err)
		}
		if string(got) != fmt.Sprintf("v%d", i) {
			t.Fatalf("get k%d: got %q", i, got)
		}
	}
}
