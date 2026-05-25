# kdb

LSM-tree key/value store in Go. Writes go through a write-ahead log into an in-memory skiplist memtable; when the memtable fills, it gets frozen and a background goroutine flushes it to a sorted on-disk SSTable. A separate background goroutine compacts SSTables size-tiered (every N similarly-sized files merge into one larger file) to keep read amplification bounded. Reads consult the active memtable, then the queue of immutable memtables awaiting flush, then the live SSTables newest-first, all without blocking writes, by way of a refcounted "Version" snapshot the read path holds for the duration of a Get.

The interesting parts to read are the way live SSTables are kept alive across concurrent reads and compaction (refcounted handles + a disposer goroutine), and the way numbered WAL files coordinate with the manifest's `MinLogNum` to make crash recovery a straightforward replay.

## Quick start

Install the CLI:

```
go install github.com/owolagbadavid/kdb/cmd/kdb-cli@latest
```

Open a database directory and poke at it:

```
$ kdb-cli -dir ./data
kdb-cli on ./data — type 'help' for commands
> put hello world
OK
> get hello
"world"
> delete hello
OK
> get hello
(nil)
> stats
memtable: 1 entries, 5 bytes
sstables: 0
> quit
```

As a library:

```go
import "github.com/owolagbadavid/kdb"

db, err := kdb.Open("./data", nil)
if err != nil { log.Fatal(err) }
defer db.Close()

if err := db.Put([]byte("k"), []byte("v")); err != nil { log.Fatal(err) }

v, err := db.Get([]byte("k"))
if errors.Is(err, kdb.ErrNotFound) {
    // key missing
}
```

The DB is single-process and not thread-safe across processes, but it is safe under concurrent goroutines.

## How it works

### Write path

`Put` and `Delete` take the write lock, append a length-prefixed CRC'd record to the active WAL file, `fsync`, then insert into the active memtable (a skiplist). If the memtable's size crosses `Options.MemtableSizeBytes`, the writer rotates: it appends the active memtable to the immutable queue (recording the WAL number that backed it), atomically opens a fresh WAL at the next number, allocates a fresh active memtable, and signals the flusher goroutine. The slow work (writing the SSTable, saving the manifest, deleting the obsolete WAL) happens off the write critical path. If the queue is already at `MaxImmutableMemtables`, the writer stalls on a condition variable until the flusher pops the head.

### Read path

`Get` checks the active memtable first, then the immutable queue newest-first (reverse FIFO so a more recent rotation shadows an older one for the same key), then the SSTables. For the SSTables, it grabs a refcounted snapshot of the current Version and releases the lock, so the iteration is lock-free, and flushes and compactions can mutate the live set under it without disturbing the in-flight Get. SSTables are visited in `(tier ascending, fileNum descending)` order: lower tier means newer data (a higher tier was produced by compacting lower-tier files at some earlier point), and within a tier the higher fileNum is the more recent file.

### Flush

A background goroutine drains the immutable queue head-first. For each immutable: write a new SSTable from its sorted iterator, fsync, save a new manifest that includes the new SSTable AND an advanced `MinLogNum` (= the next live WAL after this one), swap in the new Version under the lock, pop the queue, broadcast the flush condition so any stalled writers wake, signal the compactor (a new tier-0 file may have crossed the trigger), and finally delete the obsolete WAL file. If anything fails before the manifest commits, the immutable stays at the head of the queue and the WAL stays on disk, and the next signal retries.

### Compaction

Size-tiered, in a separate background goroutine. The picker finds the lowest tier whose file count is at least `CompactionTrigger` (default 4) and selects every file in that tier as the merge input. A k-way merge iterator with seqno-wins-on-ties walks all inputs and writes a single new SSTable into the next tier (drops older versions of overlapping keys; tombstones propagate). The compactor holds a Version reference for the duration of the merge so the input SSTable readers can't be retired under it. On commit, the new Version excludes the inputs; their refcounts drop to zero and the disposer goroutine closes each reader and deletes its file. A reader still iterating the old Version keeps the input handles alive until it releases.

### Recovery

On `Open`: remove any leftover `.tmp` / `.new` files from interrupted writes or rotations, scan for `*.sst` and `wal-NNNNNN.log` files, load the manifest. Delete orphan SSTables (on disk but not in the manifest, from a compaction that wrote its output but crashed before the manifest commit). Delete WAL files with number `< MinLogNum` (already covered by an SSTable). Replay each remaining WAL into its own immutable memtable, queued oldest-first; this is what handles a crash with multiple unflushed memtables. Open a fresh active WAL at the next number. Spawn the disposer, compactor, and flusher goroutines; if Open queued immutables, signal the flusher.

## Repo layout

```
db.go            - public API: DB, Open/Put/Get/Delete/Close/Stats, write path
compactor.go     - background compactor loop, picker glue, Version sort helper
flusher.go       - background flusher loop
export_test.go   - test-only hooks (e.g. SetFlushFaultForTest)
db_test.go       - end-to-end and concurrency tests

cmd/kdb-cli/     - small REPL: put/get/delete/stats/help/quit

internal/kv         - shared Entry / Kind / Iterator types used everywhere
internal/memtable   - skiplist + memtable wrapper
internal/wal        - append-only WAL with CRC + torn-record handling
internal/sstable    - SSTable Writer, Reader, sparse index, k-way merge iterator
internal/manifest   - atomic snapshot of live SSTables + MinLogNum
internal/version    - refcounted Version + Handle (the read-safety primitive)
internal/compaction - Pick (which files) and Run (do the merge)
```

## Design choices

- **Skiplist memtable.** Ordered and probabilistic. Ordered iteration is free, which makes flush trivial; the SSTable writer just streams the iterator.
- **Size-tiered compaction (by count).** Lowest tier with ≥N files merges into one tier-up file. Lower write amplification than leveled at the cost of higher read amplification.
- **Refcounted Versions.** Instead of holding `db.mu` across the entire Get, readers Ref a Version snapshot under the lock and release it. Compaction can retire SSTables while a reader is mid-iteration because the handle's refcount keeps the file (and its open reader) alive until every Version that named it is gone.
- **Background flush + compaction.** Both run as goroutines signalled via cap-1 coalesced channels. Writers only do WAL fsync + memtable insert + (occasional) pointer rotation; the slow disk work is off-path. `MaxImmutableMemtables` bounds how far the writer can outrun the flusher before stalling.
- **Numbered WAL files + `MinLogNum`.** Each active memtable owns one WAL. On crash, the manifest's `MinLogNum` tells recovery exactly which WAL files contain data not yet in any SSTable; older WALs are deletable. Without it, recovery would either have to re-replay already-flushed data (wasted work) or delete WALs synchronously inside the flush before manifest commit (data loss on crash between).

## Options

| Field                   | Default | Meaning                                                                                                                 |
| ----------------------- | ------- | ----------------------------------------------------------------------------------------------------------------------- |
| `MemtableSizeBytes`     | 4 MiB   | Active memtable byte threshold above which the writer rotates.                                                          |
| `CompactionTrigger`     | 4       | A tier with this many SSTables triggers a compaction into the next tier.                                                |
| `MaxImmutableMemtables` | 2       | Cap on the immutable flush queue. One is being flushed; one can queue. The next rotation stalls until the flusher pops. |

A `nil` `*Options` passed to `Open` uses the defaults above.

## Testing

```
go test ./...
go test -race ./...
go vet ./...
```

The concurrency-sensitive coverage lives in [db_test.go](db_test.go): reads racing with rotations + compactions, the flush backpressure stall, multi-WAL recovery after a crash with unflushed data, and an in-flight compaction surviving `Close`. The race detector earns its keep here; run `-race` at least once after any change to the flush, compaction, or Version-swap paths.
