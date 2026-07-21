# Single-Node In-Memory Kernel Design

Date: 2026-07-21
Status: Approved design, awaiting written-spec review
Related: `docs/architecture-decisions.md`

## 1. Purpose

This milestone builds the storage kernel that will later sit behind a MiniKV
storage node. It is a pure, in-process, in-memory byte cache. The kernel owns
object storage, indexing, expiration, eviction, capacity accounting, concurrent
access, and lifecycle management.

The implementation must exercise the hard parts of the future distributed
system without coupling them to networking or cluster control. In particular,
it retains the production defaults of 1024 logical shards, 128 MiB maximum
objects, and 64 GiB node capacity while allowing smaller configurations in
tests.

This milestone replaces the scaffold `MemoryStore`. Backward compatibility with
the scaffold API is not required because it has no production consumers.

## 2. Goals

- Store binary keys and values with clear ownership semantics.
- Stream values into and out of the cache without constructing a second
  contiguous copy of a large object.
- Keep committed-object capacity and in-progress-write capacity bounded.
- Provide atomic Put, snapshot-style Get, Delete, TTL, and byte-based SLRU
  eviction.
- Remain race-free under concurrent access to the same and different keys.
- Use the same 1024-way logical shard mapping planned for the distributed data
  plane.
- Keep payload allocation behind an `Arena` interface so a later mmap-backed
  arena can be added without changing Store callers.
- Provide deterministic tests and useful counters before networking is added.

## 3. Non-Goals

This milestone does not include:

- gRPC, HTTP, Protobuf, gateways, or client libraries;
- etcd, placement, epochs, ownership checks, or replication;
- WAL, snapshots, restart recovery, or any durability promise;
- mmap, disk, compression, GPU memory, or remote memory;
- CAS, transactions, batch operations, or range scans;
- LLM-specific tensor, model, prefix, or token-range metadata;
- admission policies based on frequency estimation or tenant quotas.

## 4. Public Kernel Contract

The kernel remains in `internal/store` for this milestone.

```go
type Store interface {
    Put(ctx context.Context, key []byte, src io.Reader, opts PutOptions) (ObjectInfo, error)
    Get(ctx context.Context, key []byte) (Object, error)
    Delete(ctx context.Context, key []byte) (bool, error)
    Stats() Stats
    Close() error
}

type PutOptions struct {
    Size int64
    TTL  time.Duration
}

type Object interface {
    io.ReadCloser
    Info() ObjectInfo
}

type ObjectInfo struct {
    Size      int64
    ExpiresAt time.Time
}
```

`ExpiresAt` is zero when the object has no TTL. Returned metadata is an immutable
snapshot. Internal generation numbers and storage references are not exposed.

### 4.1 Key and Value Rules

- Keys are arbitrary binary data from 1 through 1024 bytes.
- Keys are copied before commit; later caller mutation cannot change the stored
  key.
- Empty values are valid.
- `PutOptions.Size` is the exact declared payload size. A negative size or a
  reader that produces fewer or more bytes returns `ErrSizeMismatch`.
- Values larger than `MaxObjectBytes` are rejected before reading the source.
- The production default for `MaxObjectBytes` is 128 MiB.
- TTL zero means no expiration. Negative TTL returns `ErrInvalidTTL`.
- TTL starts at the successful commit, not when source reading starts.

### 4.2 Operation Semantics

Put stages the complete value before it becomes visible. A failed, canceled,
short, or long source never exposes a partial object. Its linearization point is
the entry replacement under the target shard lock. Concurrent successful Puts
to one key use last-commit-wins semantics.

Get returns an immutable snapshot reader. Its linearization point is acquiring a
reader reference to the current value while holding the shard read lock. A reader
that has already been returned remains valid after overwrite, Delete, TTL
expiration, eviction, or Store Close. Each returned Object must be closed to
release its Arena reference.

Delete returns `(false, nil)` when no live object exists. Its linearization point
is removing the matching entry under the shard write lock. An expired entry is
logically absent and therefore also returns `(false, nil)` after cleanup.

Context cancellation is returned unchanged when observed before an operation's
linearization point. Once a Put or Delete has committed, it returns the committed
result even if cancellation races with the return path.

Close is idempotent. It rejects new Put, Get, and Delete calls with `ErrClosed`,
cancels the Store's internal lifecycle context, wakes staging waiters and
capacity-retry loops, stops maintenance, waits for operations that already
entered the Store, removes index ownership, and closes the Arena. It cannot
interrupt an arbitrary source Reader that is blocked inside its own Read call;
Close waits for such a call to return. Arena reclamation for an already-open
Object is deferred until that Object closes. Stats remains available as a final
snapshot after Close.

### 4.3 Stable Errors

The package exposes errors usable with `errors.Is`:

- `ErrNotFound`: Get found no live object.
- `ErrInvalidKey`: key length is outside 1 through 1024 bytes.
- `ErrInvalidTTL`: TTL is negative.
- `ErrObjectTooLarge`: declared size exceeds `MaxObjectBytes`.
- `ErrSizeMismatch`: declared size is negative or differs from bytes read.
- `ErrNoCapacity`: a value cannot be admitted after bounded eviction, or its
  requested staging reservation can never fit.
- `ErrClosed`: the Store no longer accepts operations.

Errors from `context.Canceled` and `context.DeadlineExceeded` are returned
unchanged. Other source-reader errors are wrapped while preserving `errors.Is`.

## 5. Configuration

`DefaultConfig()` returns production-oriented defaults:

```go
type Config struct {
    CapacityBytes   int64
    MaxObjectBytes  int64
    MaxStagingBytes int64
    ChunkBytes      int
    ShardCount      uint32
    TTLResolution   time.Duration
    TouchBuffer     int
}
```

- `CapacityBytes`: 64 GiB.
- `MaxObjectBytes`: 128 MiB.
- `ChunkBytes`: 1 MiB.
- `ShardCount`: 1024 and required to be a power of two.
- `TTLResolution`: 1 second.
- `MaxStagingBytes`:
  `max(MaxObjectBytes, min(CapacityBytes/8, 512 MiB))`.
- `TouchBuffer`: a bounded implementation default sized for asynchronous hit
  updates; tests may set it to a small value.

`New(Config)` validates all fields. Capacity must fit one maximum-size object,
its maximum key, and entry overhead. `MaxStagingBytes` must be at least
`MaxObjectBytes`. Test configurations may reduce every byte limit and shard
count while preserving the same behavior.

The 95 percent high watermark and 90 percent low watermark are fixed kernel
policy constants in this milestone. They can become configuration only after a
benchmark demonstrates a need.

## 6. Component Architecture

```text
CoreStore
|-- HeapArena        chunked payload ownership and readers
|-- ShardIndex       1024 logical shards and immutable entries
|-- Expirer          one timing wheel, generation-checked events
|-- Evictor          per-shard byte SLRU and global pressure scans
|-- StagingBudget    context-aware reservation for in-progress Puts
|-- Statistics       atomic counters and gauges
`-- Lifecycle        operation admission, maintenance stop, and Close
```

There is one Store-level maintenance goroutine. It drains hit events, advances
expiration, and performs signaled background eviction. There is no goroutine per
shard, object, or TTL.

## 7. Arena and Payload Ownership

Payload storage is isolated behind this internal contract:

```go
type Arena interface {
    Write(ctx context.Context, src io.Reader, size int64) (ValueRef, error)
    Open(ref ValueRef) (io.ReadCloser, error)
    Release(ref ValueRef)
    Close() error
}
```

HeapArena represents a value as independently allocated chunks. Every payload
allocation is at most `ChunkBytes`; the final chunk is allocated at its exact
remaining size. An empty value owns no payload chunk. Arena.Write reads exactly
the declared size and probes for one extra byte to detect a long source.

A committed entry owns one ValueRef reference. Arena.Open adds a reader
reference, and the returned reader releases it on Close. Arena.Release removes
the entry or staging owner's reference. Chunks are reclaimed only when the last
reference closes, which gives Get its snapshot semantics without copying or
concatenating the object.

Arena.Write has no knowledge of Store admission or eviction. CoreStore reserves
staging bytes before calling it and releases that reservation on every exit
path.

## 8. Index, Entries, and Sharding

Entries are immutable after publication:

```go
type entry struct {
    key        string
    value      ValueRef
    size       int64
    cost       int64
    expiresAt  int64
    generation uint64
}
```

`expiresAt` is expressed in nanoseconds in the injected Clock's time domain and
is zero for no TTL. A Store-wide monotonic generation counter distinguishes
overwrites and invalidates stale expiration and hit events.

Admission cost is deterministic rather than an attempt to report Go process RSS:

```text
entry cost = payload size + key size + 128 bytes
```

Keys map to shards with the protocol-selected XXH3-64 hash and fixed seed zero:

```text
shard_id = XXH3_64(key) & (ShardCount - 1)
```

With the production `ShardCount` of 1024 this is equivalent to modulo 1024. Each
shard stores the full binary key, represented internally as an immutable string,
so hash collisions cannot alias entries.

```go
type shard struct {
    mu      sync.RWMutex
    entries map[string]*entry
    policy  slru
    bytes   int64
}
```

No normal operation holds two shard locks simultaneously. Reads take no global
policy or capacity mutex.

## 9. Write, Read, and Delete Flows

### 9.1 Put

1. Enter lifecycle admission and validate context, key, size, and TTL.
2. Wait for an exact staging-byte reservation, respecting context cancellation.
3. Stream the source into unpublished HeapArena chunks.
4. Verify exact length. On failure, release chunks and staging reservation.
5. Lock the target shard and read the current entry for the key.
6. Compute the live-cost delta and atomically reserve any positive delta.
7. If the hard live-capacity limit prevents reservation, unlock the shard,
   perform serialized global-pressure eviction, and retry from step 5. A Put
   makes at most three admission attempts before returning `ErrNoCapacity`.
8. Replace the map entry and local SLRU state under the shard lock. This is the
   commit and visibility point.
9. Unlock, release the old entry's Arena ownership, and release staging bytes.
10. Schedule TTL and background-pressure signals, then return ObjectInfo.

The old entry is re-read on every retry because another writer or eviction may
have changed it while the shard was unlocked. A synchronous eviction pass takes
the current entry count as its victim-attempt budget, rotates across shards, and
removes at most that many generation-validated candidates. It stops earlier when
the required bytes have been freed or when a complete shard rotation makes no
progress. The entry being replaced is excluded from that Put's pressure pass.
Failure after the admission-attempt limit returns `ErrNoCapacity` and leaves the
old value unchanged.

### 9.2 Get

1. Validate and locate the shard.
2. Under `RLock`, find the entry and compare its expiration with Clock now.
3. If live, call Arena.Open before releasing `RLock`, pinning the value for the
   returned reader.
4. Unlock and enqueue a non-blocking `(shard, key, generation)` hit event.
5. Return the reader and metadata.

If the entry is expired, Get releases `RLock`, removes it under the shard write
lock only if generation still matches, and returns `ErrNotFound`. If the hit
queue is full, Get increments `TouchDrops` and still succeeds.

### 9.3 Delete

Delete removes a live matching entry, its SLRU node, and its accounted cost under
one shard write lock. It unlocks before releasing Arena ownership. A generation
check prevents expired-entry cleanup from deleting a concurrent replacement.

## 10. TTL

The Expirer uses a Store-level timing wheel at one-second resolution. A commit
with TTL schedules an event containing shard ID, immutable key, generation, and
expiration time. Processing locks only the referenced shard and removes the
entry if both generation and expiration still match and Clock reports it due.
Stale events are ignored.

Get performs lazy expiration using the exact timestamp, so timing-wheel
resolution affects reclamation latency but never permits stale reads. Normal
Delete and overwrite need not remove old wheel events because generation checks
make them harmless.

Time is accessed through an internal Clock. Tests use a manually advanced fake
clock and direct maintenance ticks; correctness tests do not sleep.

## 11. Byte SLRU Eviction

Every shard has probation and protected LRU lists measured in accounted bytes.

- New entries enter the probation MRU position.
- A validated hit moves a probation entry to protected MRU or refreshes a
  protected entry.
- Protected overflow demotes its LRU entry to probation MRU.
- A shard may keep one object larger than its nominal protected target in the
  protected list; this prevents a single large hot object from being immediately
  demoted.
- Eviction chooses probation LRU first, then protected LRU.

The protected target is 80 percent of the shard's nominal share of Store
capacity; probation represents the remaining 20 percent. This division affects
recency policy only. It is not a per-shard capacity limit, so uneven shard loads
remain admissible.

Hit processing validates key and generation under the target shard lock before
changing policy. Dropped or stale hits can reduce hit rate but cannot change
value correctness or accounting.

Global pressure eviction is serialized by one eviction mutex. It visits shards
from a rotating cursor, first removing due expired entries and then one local
SLRU victim at a time. It never holds more than one shard lock. This is an
intentionally approximate global LRU that avoids a lock on the Get path.

## 12. Capacity and Staging

Live capacity is the sum of committed entry costs. Staging capacity is the sum
of declared payload bytes for Puts that have reserved but not yet released their
write budget. Both are maintained with atomic gauges and have independent hard
limits.

Staging reservation waits when concurrent writes temporarily consume the
budget, and wakes on release or context cancellation. A request larger than the
entire staging budget fails immediately with `ErrNoCapacity`. A failed or
canceled staged Put never evicts a live object.

A successful stage then competes for live admission:

- At or above 95 percent live usage, Put signals background eviction.
- Background eviction attempts to reduce usage to 90 percent.
- A Put that would exceed 100 percent performs synchronous eviction only for the
  bytes needed to admit its current delta, then retries its commit.
- If no eligible entry can provide enough room, Put returns `ErrNoCapacity` and
  preserves the previous value for that key.

After every completed operation, committed live cost must be at most
`CapacityBytes`, and staging cost must be at most `MaxStagingBytes`. Temporary
positive live reservations are made only while holding the committing shard lock
and are either published or rolled back before it is released.

## 13. Statistics

Stats is a weakly consistent, lock-free snapshot built from atomic values. It
contains at least:

- configured capacity;
- accounted live bytes and payload bytes;
- current staging bytes and entry count;
- Get, hit, and miss totals;
- committed Put and successful Delete totals;
- eviction and expiration totals;
- rejected Put total;
- dropped hit-event total.

An expired Get counts as one Get and one miss. A Put increments the committed
counter only after publication; every non-context Put failure increments rejected
Puts. Gauges converge before the responsible operation returns.

## 14. Lifecycle and Locking Rules

- Lifecycle admission prevents Close from racing with operation setup.
- Close rejects new operations, cancels internal waits, stops and joins the
  maintenance goroutine, waits for entered operations, then removes entries one
  shard at a time.
- Outstanding Object readers are Arena references, not active Store operations;
  they remain readable and release memory when closed.
- Shard locks protect entries, local bytes, and local SLRU state.
- The eviction mutex serializes pressure scans but is never acquired by Get.
- Put releases its shard lock before acquiring the eviction mutex.
- The maintenance loop and synchronous eviction lock one shard at a time.
- Arena callbacks that can block are not made while holding a shard write lock,
  except Arena.Open under `RLock`, which only increments an existing reference.

These rules define the lock order and rule out shard-to-shard deadlocks.

## 15. Test Strategy

### 15.1 HeapArena

- zero-byte, one-byte, exact-chunk, and chunk-plus-one values;
- short source, long source, source error, and context cancellation;
- reader traversal across chunk boundaries;
- independent concurrent readers;
- reader remains valid after owner Release and Arena Close;
- chunks are reclaimed after the last reader closes.

### 15.2 Store Semantics

- every key, size, object-limit, and TTL validation boundary;
- binary keys including zero bytes;
- Put invisibility while staging and atomic visibility at commit;
- overwrite snapshot isolation and last-commit-wins behavior;
- Delete of live, missing, and expired entries;
- concurrent operations on one key and on different shards;
- full-key correctness with an injectable colliding test hasher;
- Close during operations and reads, repeated Close, and post-Close errors.

### 15.3 TTL and Policy

- exact lazy expiration before a wheel cleanup;
- deterministic wheel cleanup using a fake Clock;
- stale generation events after overwrite and Delete;
- SLRU insert, promotion, refresh, demotion, and byte-order eviction;
- expired-first pressure cleanup;
- full hit queue and stale hit events.

### 15.4 Capacity

- concurrent staging reservations never exceed their budget;
- canceled waiters release no bytes and leak no goroutines;
- failed streaming does not evict live data;
- replacement delta accounting for larger and smaller values;
- hard-cap concurrent commits never over-admit;
- synchronous and background eviction behavior;
- bounded failure when no object can be evicted.

### 15.5 Higher-Order Verification

- model-based randomized tests compare visible behavior with a simple reference
  map under a fake Clock;
- fuzz targets cover keys, values, operation sequences, and source boundaries;
- `go test -race ./internal/store/...` covers concurrency;
- benchmarks cover 1 KiB, 1 MiB, and 32 MiB streaming objects plus parallel
  read/write mixes and report allocations;
- a separately enabled large-object test exercises the 128 MiB production
  boundary without burdening routine race runs.

No correctness test depends on wall-clock sleeps. Initial benchmarks establish a
baseline and do not impose speculative throughput thresholds.

## 16. Acceptance Criteria

The milestone is complete when:

- all Store, Arena, policy, expiration, capacity, and lifecycle tests pass;
- `go test -race ./internal/store/...` reports no race;
- committed and staging gauges never exceed their configured hard limits after
  an operation returns;
- a failed or canceled Put never publishes a partial value;
- Get streams chunks and never concatenates a complete value internally;
- no HeapArena payload allocation exceeds configured `ChunkBytes`;
- readers opened before overwrite, Delete, expiration, eviction, or Close remain
  valid until closed;
- maintenance and cancellation tests show no leaked goroutines;
- the legacy map-backed scaffold implementation and tests have been replaced by
  the new kernel contract and behavior.

## 17. Deferred Follow-Up

After this kernel is measured and stable, later specs may add gRPC streaming,
WAL and snapshots, replica application, mmap-backed CPU-tier storage, etcd shard
ownership, and LLM cache adapters. Those layers must consume this kernel's
interfaces rather than weaken its ownership, capacity, or snapshot guarantees.
