# Single-Node In-Memory Kernel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Replace the map-backed scaffold with a bounded, chunked, concurrent in-memory Store supporting streaming objects, TTL, byte SLRU eviction, statistics, and safe lifecycle behavior.

**Architecture:** CoreStore maps binary keys into power-of-two logical shards using seeded XXH3-64. HeapArena owns immutable chunked payloads behind reference-counted ValueRefs; CoreStore coordinates staging admission, atomic index commits, TTL events, per-shard SLRU, pressure eviction, and lifecycle shutdown.

**Tech Stack:** Go 1.23, standard library concurrency primitives, github.com/zeebo/xxh3 for protocol-compatible hashing, Go testing/fuzzing/benchmark tooling.

---

## File Map

- Modify **go.mod** and create **go.sum**: add the XXH3 implementation.
- Replace **internal/store/store.go**: public kernel contract and metadata types.
- Delete **internal/store/memory.go** and **internal/store/memory_test.go**: remove the obsolete scaffold.
- Create **internal/store/errors.go**: stable operation errors and validation helpers.
- Create **internal/store/config.go**: defaults, watermarks, and validation.
- Create **internal/store/hash.go**: fixed-seed XXH3 shard mapping.
- Create **internal/store/arena.go**: Arena and opaque ValueRef contracts.
- Create **internal/store/heap_arena.go**: chunk allocation, exact streaming, reference-counted readers.
- Create **internal/store/budget.go**: context-aware staging-byte reservations.
- Create **internal/store/slru.go**: byte-accounted probation/protected policy.
- Create **internal/store/clock.go**: production and manual-test clocks.
- Create **internal/store/expiry.go**: generation-tagged timing wheel.
- Create **internal/store/stats.go**: public snapshot and internal atomic counters.
- Create **internal/store/entry.go**: immutable entry, shard, and removal helpers.
- Create **internal/store/core.go**: construction, dependency injection, maintenance startup.
- Create **internal/store/operations.go**: Put, Get, Delete, Object reader, and admission.
- Create **internal/store/eviction.go**: hard-cap and watermark eviction scans.
- Create **internal/store/maintenance.go**: touch drain, expiry processing, and background pressure.
- Create **internal/store/lifecycle.go**: operation admission and Close coordination.
- Create **internal/store/test_helpers_test.go**: deterministic clocks, compact configs, and Store assertions.
- Create focused tests beside each component, plus model, fuzz, race, and benchmark coverage.

## Task 1: Public Contract, Configuration, and Shard Hash

**Files:**
- Modify: **go.mod**
- Create: **go.sum**
- Replace: **internal/store/store.go**
- Delete: **internal/store/memory.go**
- Delete: **internal/store/memory_test.go**
- Create: **internal/store/errors.go**
- Create: **internal/store/config.go**
- Create: **internal/store/config_test.go**
- Create: **internal/store/hash.go**
- Create: **internal/store/hash_test.go**

- [ ] **Step 1: Add the maintained XXH3 dependency**

Run:

~~~bash
go get github.com/zeebo/xxh3@latest
~~~

Expected: go.mod contains github.com/zeebo/xxh3 and go.sum is created or updated.

- [ ] **Step 2: Write failing configuration and hash tests**

Create table-driven tests with these exact assertions:

~~~go
func TestDefaultConfig(t *testing.T) {
    cfg := DefaultConfig()
    if cfg.CapacityBytes != 64<<30 { t.Fatalf("capacity = %d", cfg.CapacityBytes) }
    if cfg.MaxObjectBytes != 128<<20 { t.Fatalf("max object = %d", cfg.MaxObjectBytes) }
    if cfg.MaxStagingBytes != 512<<20 { t.Fatalf("staging = %d", cfg.MaxStagingBytes) }
    if cfg.ChunkBytes != 1<<20 { t.Fatalf("chunk = %d", cfg.ChunkBytes) }
    if cfg.ShardCount != 1024 { t.Fatalf("shards = %d", cfg.ShardCount) }
    if cfg.TTLResolution != time.Second { t.Fatalf("ttl resolution = %s", cfg.TTLResolution) }
}

func TestConfigValidation(t *testing.T) {
    valid := Config{
        CapacityBytes: 4096, MaxObjectBytes: 1024, MaxStagingBytes: 2048,
        ChunkBytes: 256, ShardCount: 4, TTLResolution: time.Second, TouchBuffer: 8,
    }
    cases := map[string]func(*Config){
        "capacity": func(c *Config) { c.CapacityBytes = 0 },
        "object": func(c *Config) { c.MaxObjectBytes = 0 },
        "object does not fit": func(c *Config) { c.CapacityBytes = c.MaxObjectBytes + maxKeyBytes + entryOverheadBytes - 1 },
        "staging": func(c *Config) { c.MaxStagingBytes = c.MaxObjectBytes - 1 },
        "chunk": func(c *Config) { c.ChunkBytes = 0 },
        "chunk larger than object": func(c *Config) { c.ChunkBytes = int(c.MaxObjectBytes + 1) },
        "shards": func(c *Config) { c.ShardCount = 3 },
        "ttl": func(c *Config) { c.TTLResolution = 0 },
        "touch buffer": func(c *Config) { c.TouchBuffer = 0 },
    }
    if err := valid.validate(); err != nil { t.Fatalf("valid config: %v", err) }
    for name, mutate := range cases {
        t.Run(name, func(t *testing.T) {
            cfg := valid
            mutate(&cfg)
            if err := cfg.validate(); err == nil { t.Fatal("expected validation error") }
        })
    }
}

func TestShardIDUsesSeedZeroXXH3(t *testing.T) {
    const knownHelloXXH3 = uint64(0x9555e8555c62dcfd)
    if got := xxh3.HashSeed([]byte("hello"), shardHashSeed); got != knownHelloXXH3 {
        t.Fatalf("protocol hash = %#x", got)
    }
    if got, want := shardID([]byte("hello"), 1024), uint32(knownHelloXXH3&1023); got != want {
        t.Fatalf("shard = %d, want %d", got, want)
    }
}
~~~

- [ ] **Step 3: Run the tests and verify the expected failure**

Run:

~~~bash
go test ./internal/store -run "Test(DefaultConfig|ConfigValidation|ShardID)" -count=1
~~~

Expected: FAIL because Config, DefaultConfig, and shardID are undefined.

- [ ] **Step 4: Replace the scaffold contract and implement validation**

The public contract is:

~~~go
type Store interface {
    Put(context.Context, []byte, io.Reader, PutOptions) (ObjectInfo, error)
    Get(context.Context, []byte) (Object, error)
    Delete(context.Context, []byte) (bool, error)
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
~~~

Define these sentinels in errors.go:

~~~go
var (
    ErrNotFound      = errors.New("store: not found")
    ErrInvalidKey    = errors.New("store: invalid key")
    ErrInvalidTTL    = errors.New("store: invalid ttl")
    ErrObjectTooLarge = errors.New("store: object too large")
    ErrSizeMismatch  = errors.New("store: size mismatch")
    ErrNoCapacity    = errors.New("store: no capacity")
    ErrClosed        = errors.New("store: closed")
)

func validateKey(key []byte) error {
    if len(key) < minKeyBytes || len(key) > maxKeyBytes { return ErrInvalidKey }
    return nil
}
~~~

Implement Config with these constants and checks:

~~~go
const (
    minKeyBytes       = 1
    maxKeyBytes       = 1024
    entryOverheadBytes int64 = 128
    highWatermarkNumerator int64 = 95
    lowWatermarkNumerator  int64 = 90
    watermarkDenominator   int64 = 100
    maxAdmissionAttempts         = 3
)

func DefaultConfig() Config {
    const capacity = int64(64 << 30)
    const maxObject = int64(128 << 20)
    staging := capacity / 8
    if staging > 512<<20 { staging = 512 << 20 }
    if staging < maxObject { staging = maxObject }
    return Config{
        CapacityBytes: capacity, MaxObjectBytes: maxObject,
        MaxStagingBytes: staging, ChunkBytes: 1 << 20,
        ShardCount: 1024, TTLResolution: time.Second, TouchBuffer: 64 << 10,
    }
}

func (c Config) validate() error {
    if c.CapacityBytes <= 0 { return fmt.Errorf("capacity bytes must be positive") }
    if c.MaxObjectBytes <= 0 { return fmt.Errorf("max object bytes must be positive") }
    if c.CapacityBytes < c.MaxObjectBytes+maxKeyBytes+entryOverheadBytes {
        return fmt.Errorf("capacity cannot fit maximum object")
    }
    if c.MaxStagingBytes < c.MaxObjectBytes { return fmt.Errorf("staging cannot fit maximum object") }
    if c.ChunkBytes <= 0 || int64(c.ChunkBytes) > c.MaxObjectBytes {
        return fmt.Errorf("chunk bytes must be within object limit")
    }
    if c.ShardCount == 0 || c.ShardCount&(c.ShardCount-1) != 0 {
        return fmt.Errorf("shard count must be a power of two")
    }
    if c.TTLResolution <= 0 { return fmt.Errorf("ttl resolution must be positive") }
    if c.TouchBuffer <= 0 { return fmt.Errorf("touch buffer must be positive") }
    return nil
}
~~~

Hash with the protocol seed:

~~~go
const shardHashSeed uint64 = 0

type hashFunc func([]byte) uint64

func protocolHash(key []byte) uint64 { return xxh3.HashSeed(key, shardHashSeed) }

func shardIDWithHash(key []byte, shardCount uint32, hash hashFunc) uint32 {
    return uint32(hash(key) & uint64(shardCount-1))
}

func shardID(key []byte, shardCount uint32) uint32 {
    return shardIDWithHash(key, shardCount, protocolHash)
}
~~~

Remove memory.go and memory_test.go in the same patch because their API is intentionally replaced.

- [ ] **Step 5: Format and verify**

Run:

~~~bash
gofmt -w internal/store/store.go internal/store/errors.go internal/store/config.go internal/store/config_test.go internal/store/hash.go internal/store/hash_test.go
go test ./internal/store -count=1
~~~

Expected: PASS.

- [ ] **Step 6: Commit**

~~~bash
git add go.mod go.sum internal/store
git commit -m "feat: define single-node store contract"
~~~

## Task 2: Reference-Counted Chunked HeapArena

**Files:**
- Create: **internal/store/arena.go**
- Create: **internal/store/heap_arena.go**
- Create: **internal/store/heap_arena_test.go**

- [ ] **Step 1: Write failing Arena behavior tests**

Use a 4-byte chunk size and cover exact boundaries:

~~~go
func TestHeapArenaChunkingAndReadback(t *testing.T) {
    arena := NewHeapArena(4)
    for _, value := range [][]byte{nil, []byte("a"), []byte("abcd"), []byte("abcde")} {
        ref, err := arena.Write(context.Background(), bytes.NewReader(value), int64(len(value)))
        if err != nil { t.Fatalf("write %q: %v", value, err) }
        hv := ref.handle.(*heapValue)
        for _, chunk := range hv.chunks {
            if len(chunk) > 4 { t.Fatalf("chunk length = %d", len(chunk)) }
        }
        reader, err := arena.Open(ref)
        if err != nil { t.Fatal(err) }
        got, err := io.ReadAll(reader)
        if err != nil { t.Fatal(err) }
        if !bytes.Equal(got, value) { t.Fatalf("got %q, want %q", got, value) }
        if err := reader.Close(); err != nil { t.Fatal(err) }
        arena.Release(ref)
        if len(hv.chunks) != 0 { t.Fatal("chunks retained after final release") }
    }
}

func TestHeapArenaRejectsShortAndLongSources(t *testing.T) {
    arena := NewHeapArena(4)
    cases := []struct { size int64; value string }{{4, "abc"}, {3, "abcd"}}
    for _, tc := range cases {
        if _, err := arena.Write(context.Background(), strings.NewReader(tc.value), tc.size); !errors.Is(err, ErrSizeMismatch) {
            t.Fatalf("size=%d value=%q error=%v", tc.size, tc.value, err)
        }
    }
}

func TestHeapArenaReaderSurvivesOwnerReleaseAndArenaClose(t *testing.T) {
    arena := NewHeapArena(4)
    ref, _ := arena.Write(context.Background(), strings.NewReader("payload"), 7)
    reader, _ := arena.Open(ref)
    arena.Release(ref)
    if err := arena.Close(); err != nil { t.Fatal(err) }
    got, err := io.ReadAll(reader)
    if err != nil || string(got) != "payload" { t.Fatalf("got %q, err %v", got, err) }
    if err := reader.Close(); err != nil { t.Fatal(err) }
}
~~~

Use these concrete cancellation and source-error tests:

~~~go
func TestHeapArenaCancellation(t *testing.T) {
    arena := NewHeapArena(4)
    ctx, cancel := context.WithCancel(context.Background())
    cancel()
    if _, err := arena.Write(ctx, strings.NewReader("data"), 4); !errors.Is(err, context.Canceled) {
        t.Fatalf("error = %v", err)
    }
}

func TestHeapArenaPreservesSourceError(t *testing.T) {
    arena := NewHeapArena(4)
    sourceErr := errors.New("source failed")
    src := readerFunc(func(p []byte) (int, error) {
        copy(p, "ab")
        return 2, sourceErr
    })
    if _, err := arena.Write(context.Background(), src, 4); !errors.Is(err, sourceErr) {
        t.Fatalf("error = %v", err)
    }
}
~~~

- [ ] **Step 2: Run the tests and verify failure**

Run:

~~~bash
go test ./internal/store -run TestHeapArena -count=1
~~~

Expected: FAIL because Arena, ValueRef, and HeapArena are undefined.

- [ ] **Step 3: Implement the opaque Arena contract**

~~~go
type ValueRef struct{ handle any }

type Arena interface {
    Write(context.Context, io.Reader, int64) (ValueRef, error)
    Open(ValueRef) (io.ReadCloser, error)
    Release(ValueRef)
    Close() error
}
~~~

HeapArena and heapValue use immutable chunks and an atomic owner/reader count:

~~~go
type HeapArena struct {
    chunkBytes int
    mu         sync.RWMutex
    closed     bool
}

type heapValue struct {
    refs   atomic.Int64
    chunks [][]byte
    size   int64
}

func NewHeapArena(chunkBytes int) *HeapArena {
    return &HeapArena{chunkBytes: chunkBytes}
}

func (a *HeapArena) Write(ctx context.Context, src io.Reader, size int64) (ValueRef, error) {
    if size < 0 { return ValueRef{}, ErrSizeMismatch }
    a.mu.RLock()
    closed := a.closed
    a.mu.RUnlock()
    if closed { return ValueRef{}, ErrClosed }

    chunks := make([][]byte, 0, (size+int64(a.chunkBytes)-1)/int64(a.chunkBytes))
    remaining := size
    for remaining > 0 {
        if err := ctx.Err(); err != nil { return ValueRef{}, err }
        n := int64(a.chunkBytes)
        if remaining < n { n = remaining }
        chunk := make([]byte, int(n))
        if _, err := io.ReadFull(src, chunk); err != nil {
            if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
                return ValueRef{}, ErrSizeMismatch
            }
            return ValueRef{}, fmt.Errorf("read value: %w", err)
        }
        chunks = append(chunks, chunk)
        remaining -= n
    }
    if err := ctx.Err(); err != nil { return ValueRef{}, err }
    var probe [1]byte
    for {
        if err := ctx.Err(); err != nil { return ValueRef{}, err }
        n, err := src.Read(probe[:])
        if n != 0 { return ValueRef{}, ErrSizeMismatch }
        if errors.Is(err, io.EOF) { break }
        if err != nil { return ValueRef{}, fmt.Errorf("probe value: %w", err) }
    }
    value := &heapValue{chunks: chunks, size: size}
    value.refs.Store(1)
    return ValueRef{handle: value}, nil
}
~~~

Open must CAS-increment refs only while positive, then return a heapReader with chunk index and offset. Release decrements and nils chunks exactly when refs reaches zero. heapReader.Read copies across chunk boundaries and returns io.EOF after size bytes; Close is idempotent, releases one ref, and makes later Read return io.ErrClosedPipe. HeapArena.Close only marks the Arena closed, so already-open readers retain their heapValue.

- [ ] **Step 4: Format and run Arena tests**

Run:

~~~bash
gofmt -w internal/store/arena.go internal/store/heap_arena.go internal/store/heap_arena_test.go
go test ./internal/store -run TestHeapArena -count=1
~~~

Expected: PASS.

- [ ] **Step 5: Commit**

~~~bash
git add internal/store/arena.go internal/store/heap_arena.go internal/store/heap_arena_test.go
git commit -m "feat: add chunked heap arena"
~~~

## Task 3: Context-Aware Staging Budget

**Files:**
- Create: **internal/store/budget.go**
- Create: **internal/store/budget_test.go**

- [ ] **Step 1: Write failing reservation tests**

~~~go
func TestByteBudgetBlocksUntilRelease(t *testing.T) {
    b := newByteBudget(10)
    if err := b.reserve(context.Background(), 8); err != nil { t.Fatal(err) }
    acquired := make(chan error, 1)
    go func() { acquired <- b.reserve(context.Background(), 4) }()
    select {
    case err := <-acquired: t.Fatalf("reservation returned early: %v", err)
    default:
    }
    b.release(8)
    if err := <-acquired; err != nil { t.Fatal(err) }
    b.release(4)
    if got := b.usedBytes(); got != 0 { t.Fatalf("used = %d", got) }
}

func TestByteBudgetCancellationAndClose(t *testing.T) {
    b := newByteBudget(4)
    if err := b.reserve(context.Background(), 4); err != nil { t.Fatal(err) }
    ctx, cancel := context.WithCancel(context.Background())
    cancel()
    if err := b.reserve(ctx, 1); !errors.Is(err, context.Canceled) { t.Fatalf("error = %v", err) }
    b.close()
    if err := b.reserve(context.Background(), 1); !errors.Is(err, ErrClosed) { t.Fatalf("error = %v", err) }
}

func TestByteBudgetRejectsImpossibleReservation(t *testing.T) {
    b := newByteBudget(4)
    if err := b.reserve(context.Background(), 5); !errors.Is(err, ErrNoCapacity) {
        t.Fatalf("error = %v", err)
    }
}
~~~

- [ ] **Step 2: Run the tests and verify failure**

~~~bash
go test ./internal/store -run TestByteBudget -count=1
~~~

Expected: FAIL because newByteBudget is undefined.

- [ ] **Step 3: Implement wake-channel reservations**

~~~go
type byteBudget struct {
    mu     sync.Mutex
    limit  int64
    used   int64
    closed bool
    wake   chan struct{}
}

func newByteBudget(limit int64) *byteBudget {
    return &byteBudget{limit: limit, wake: make(chan struct{})}
}

func (b *byteBudget) reserve(ctx context.Context, n int64) error {
    if n < 0 { return ErrSizeMismatch }
    if n > b.limit { return ErrNoCapacity }
    for {
        b.mu.Lock()
        if b.closed { b.mu.Unlock(); return ErrClosed }
        if b.used+n <= b.limit {
            b.used += n
            b.mu.Unlock()
            return nil
        }
        wake := b.wake
        b.mu.Unlock()
        select {
        case <-ctx.Done(): return ctx.Err()
        case <-wake:
        }
    }
}

func (b *byteBudget) release(n int64) {
    if n == 0 { return }
    b.mu.Lock()
    b.used -= n
    b.signalLocked()
    b.mu.Unlock()
}

func (b *byteBudget) signalLocked() {
    wake := b.wake
    b.wake = make(chan struct{})
    close(wake)
}

func (b *byteBudget) close() {
    b.mu.Lock()
    if !b.closed {
        b.closed = true
        b.signalLocked()
    }
    b.mu.Unlock()
}
~~~

usedBytes reads under the mutex. Releasing after close remains valid because every signal replaces the wake channel before closing the previous one.

- [ ] **Step 4: Run tests, including a concurrent hard-limit test**

Add 32 goroutines repeatedly reserving and releasing variable byte counts. Track the maximum observed usedBytes and assert it never exceeds the limit.

Run:

~~~bash
gofmt -w internal/store/budget.go internal/store/budget_test.go
go test ./internal/store -run TestByteBudget -count=1
~~~

Expected: PASS.

- [ ] **Step 5: Commit**

~~~bash
git add internal/store/budget.go internal/store/budget_test.go
git commit -m "feat: bound in-progress writes"
~~~

## Task 4: Per-Shard Byte SLRU

**Files:**
- Create: **internal/store/slru.go**
- Create: **internal/store/slru_test.go**

- [ ] **Step 1: Write failing deterministic policy tests**

~~~go
func TestSLRUPromotionDemotionAndVictimOrder(t *testing.T) {
    p := newSLRU(8)
    p.insert("a", 1, 4)
    p.insert("b", 2, 4)
    if got := p.victim(policyExclusion{}); got.key != "a" { t.Fatalf("first victim = %q", got.key) }
    if !p.touch("a", 1) { t.Fatal("touch failed") }
    if !p.touch("b", 2) { t.Fatal("touch failed") }
    p.insert("c", 3, 4)
    if got := p.victim(policyExclusion{}); got.key != "c" {
        t.Fatalf("probation victim = %q", got.key)
    }
}

func TestSLRUIgnoresStaleGeneration(t *testing.T) {
    p := newSLRU(8)
    p.insert("a", 2, 4)
    if p.touch("a", 1) { t.Fatal("stale touch accepted") }
    if p.remove("a", 1) { t.Fatal("stale remove accepted") }
    if !p.remove("a", 2) { t.Fatal("current remove rejected") }
}

func TestSLRUVictimExclusion(t *testing.T) {
    p := newSLRU(8)
    p.insert("a", 1, 4)
    p.insert("b", 2, 4)
    got := p.victim(policyExclusion{key: "a", generation: 1, enabled: true})
    if got.key != "b" { t.Fatalf("victim = %q", got.key) }
}
~~~

- [ ] **Step 2: Run the tests and verify failure**

~~~bash
go test ./internal/store -run TestSLRU -count=1
~~~

Expected: FAIL because newSLRU is undefined.

- [ ] **Step 3: Implement the two-segment policy**

Use container/list with a key-to-element map:

~~~go
type policySegment uint8
const (
    probation policySegment = iota
    protected
)

type policyItem struct {
    key        string
    generation uint64
    cost       int64
    segment    policySegment
}

type policyCandidate struct {
    key        string
    generation uint64
    cost       int64
    ok         bool
}

type slru struct {
    probation      list.List
    protected      list.List
    nodes          map[string]*list.Element
    protectedBytes int64
    protectedLimit int64
}
~~~

insert removes an existing key, pushes the new item to probation front, and records it. touch validates generation, moves probation items to protected front, refreshes protected items, and repeatedly demotes protected back entries while protectedBytes exceeds protectedLimit and protected contains more than one item. victim scans probation back-to-front, then protected back-to-front, skipping the exact exclusion. remove validates generation, updates protectedBytes when needed, removes the list element, and deletes the map entry.

- [ ] **Step 4: Format and verify**

~~~bash
gofmt -w internal/store/slru.go internal/store/slru_test.go
go test ./internal/store -run TestSLRU -count=1
~~~

Expected: PASS.

- [ ] **Step 5: Commit**

~~~bash
git add internal/store/slru.go internal/store/slru_test.go
git commit -m "feat: add byte slru policy"
~~~

## Task 5: Clock and Generation-Safe Timing Wheel

**Files:**
- Create: **internal/store/clock.go**
- Create: **internal/store/expiry.go**
- Create: **internal/store/expiry_test.go**

- [ ] **Step 1: Write failing wheel tests**

~~~go
func TestTimingWheelReturnsOnlyDueEvents(t *testing.T) {
    start := time.Unix(100, 0)
    wheel := newTimingWheel(time.Second, start, 8)
    first := expirationEvent{key: "a", generation: 1, expiresAt: start.Add(500*time.Millisecond).UnixNano()}
    later := expirationEvent{key: "b", generation: 2, expiresAt: start.Add(10*time.Second).UnixNano()}
    wheel.schedule(first)
    wheel.schedule(later)
    if got := wheel.advance(start.Add(400 * time.Millisecond)); len(got) != 0 { t.Fatalf("early events = %v", got) }
    got := wheel.advance(start.Add(time.Second))
    if len(got) != 1 || got[0].key != "a" { t.Fatalf("due events = %v", got) }
    got = wheel.advance(start.Add(10 * time.Second))
    if len(got) != 1 || got[0].key != "b" { t.Fatalf("wrapped events = %v", got) }
}
~~~

The wheel preserves both generations for CoreStore to validate:

~~~go
func TestTimingWheelPreservesGenerations(t *testing.T) {
    start := time.Unix(100, 0)
    wheel := newTimingWheel(time.Second, start, 8)
    expiresAt := start.Add(time.Second).UnixNano()
    wheel.schedule(expirationEvent{key: "k", generation: 1, expiresAt: expiresAt})
    wheel.schedule(expirationEvent{key: "k", generation: 2, expiresAt: expiresAt})
    got := wheel.advance(start.Add(time.Second))
    if len(got) != 2 || got[0].generation != 1 || got[1].generation != 2 {
        t.Fatalf("events = %+v", got)
    }
}
~~~

- [ ] **Step 2: Run the test and verify failure**

~~~bash
go test ./internal/store -run TestTimingWheel -count=1
~~~

Expected: FAIL because newTimingWheel is undefined.

- [ ] **Step 3: Implement Clock and the fixed-slot wheel**

~~~go
type clock interface{ Now() time.Time }
type realClock struct{}
func (realClock) Now() time.Time { return time.Now() }

type expirationEvent struct {
    shardID   uint32
    key       string
    generation uint64
    expiresAt int64
    dueTick   int64
}

type timingWheel struct {
    mu         sync.Mutex
    resolution int64
    currentTick int64
    slots      [][]expirationEvent
}
~~~

newTimingWheel validates slotCount greater than zero and initializes currentTick as start.UnixNano()/resolution. schedule computes ceil(expiresAt/resolution), clamps it to currentTick+1, stores dueTick, and appends to slots[dueTick modulo slot count]. advance does nothing for backward or same-tick time, walks each elapsed tick, takes that slot, emits events whose dueTick is reached and expiresAt is not after now, and puts future wrapped events back into the same slot.

- [ ] **Step 4: Format and verify**

~~~bash
gofmt -w internal/store/clock.go internal/store/expiry.go internal/store/expiry_test.go
go test ./internal/store -run TestTimingWheel -count=1
~~~

Expected: PASS.

- [ ] **Step 5: Commit**

~~~bash
git add internal/store/clock.go internal/store/expiry.go internal/store/expiry_test.go
git commit -m "feat: add expiration timing wheel"
~~~

## Task 6: Atomic CoreStore CRUD and Snapshot Readers

**Files:**
- Create: **internal/store/stats.go**
- Create: **internal/store/entry.go**
- Create: **internal/store/core.go**
- Create: **internal/store/operations.go**
- Create: **internal/store/lifecycle.go**
- Create: **internal/store/test_helpers_test.go**
- Create: **internal/store/store_test.go**

- [ ] **Step 1: Write failing contract tests**

Create a small test Store with capacity 4096, max object 1024, staging 2048, chunk 4, four shards, one-second TTL resolution, and a 16-event touch buffer.

~~~go
func TestCoreStorePutGetDelete(t *testing.T) {
    s := newTestStore(t)
    info, err := s.Put(context.Background(), []byte("alpha"), strings.NewReader("value"), PutOptions{Size: 5})
    if err != nil { t.Fatal(err) }
    if info.Size != 5 || !info.ExpiresAt.IsZero() { t.Fatalf("info = %+v", info) }
    object, err := s.Get(context.Background(), []byte("alpha"))
    if err != nil { t.Fatal(err) }
    got, _ := io.ReadAll(object)
    object.Close()
    if string(got) != "value" { t.Fatalf("value = %q", got) }
    if deleted, err := s.Delete(context.Background(), []byte("alpha")); err != nil || !deleted {
        t.Fatalf("deleted=%v err=%v", deleted, err)
    }
    if _, err := s.Get(context.Background(), []byte("alpha")); !errors.Is(err, ErrNotFound) {
        t.Fatalf("error = %v", err)
    }
}

func TestCoreStoreReaderSurvivesOverwriteAndDelete(t *testing.T) {
    s := newTestStore(t)
    s.Put(context.Background(), []byte("k"), strings.NewReader("old"), PutOptions{Size: 3})
    old, err := s.Get(context.Background(), []byte("k"))
    if err != nil { t.Fatal(err) }
    s.Put(context.Background(), []byte("k"), strings.NewReader("new"), PutOptions{Size: 3})
    s.Delete(context.Background(), []byte("k"))
    got, err := io.ReadAll(old)
    if err != nil || string(got) != "old" { t.Fatalf("got=%q err=%v", got, err) }
    old.Close()
}

func TestCoreStoreValidatesBeforeReading(t *testing.T) {
    s := newTestStore(t)
    panicReader := readerFunc(func([]byte) (int, error) { panic("reader must not be called") })
    cases := []struct {
        key []byte
        opts PutOptions
        want error
    }{
        {nil, PutOptions{Size: 1}, ErrInvalidKey},
        {bytes.Repeat([]byte("k"), 1025), PutOptions{Size: 1}, ErrInvalidKey},
        {[]byte("k"), PutOptions{Size: -1}, ErrSizeMismatch},
        {[]byte("k"), PutOptions{Size: 1025}, ErrObjectTooLarge},
        {[]byte("k"), PutOptions{Size: 1, TTL: -1}, ErrInvalidTTL},
    }
    for _, tc := range cases {
        if _, err := s.Put(context.Background(), tc.key, panicReader, tc.opts); !errors.Is(err, tc.want) {
            t.Fatalf("error=%v want=%v", err, tc.want)
        }
    }
}
~~~

Use one table plus focused atomicity checks:

~~~go
func TestCoreStoreBinaryKeyEmptyValueAndKeyCopy(t *testing.T) {
    s := newTestStore(t)
    key := []byte{'a', 0, 'b'}
    if _, err := s.Put(context.Background(), key, bytes.NewReader(nil), PutOptions{Size: 0}); err != nil {
        t.Fatal(err)
    }
    key[0] = 'x'
    object, err := s.Get(context.Background(), []byte{'a', 0, 'b'})
    if err != nil { t.Fatal(err) }
    got, err := io.ReadAll(object)
    object.Close()
    if err != nil || len(got) != 0 { t.Fatalf("got=%v err=%v", got, err) }
}

func TestCoreStoreFailedPutIsInvisible(t *testing.T) {
    s := newTestStore(t)
    putString(t, s, "k", "old")
    for _, src := range []io.Reader{strings.NewReader("x"), strings.NewReader("toolong")} {
        if _, err := s.Put(context.Background(), []byte("k"), src, PutOptions{Size: 3}); !errors.Is(err, ErrSizeMismatch) {
            t.Fatalf("error = %v", err)
        }
        assertValue(t, s, "k", "old")
    }
}

func TestCoreStoreDeleteMissingAndLastCommitWins(t *testing.T) {
    s := newTestStore(t)
    if deleted, err := s.Delete(context.Background(), []byte("missing")); err != nil || deleted {
        t.Fatalf("deleted=%v err=%v", deleted, err)
    }
    putString(t, s, "k", "first")
    putString(t, s, "k", "second")
    assertValue(t, s, "k", "second")
}
~~~

- [ ] **Step 2: Run and verify failure**

~~~bash
go test ./internal/store -run TestCoreStore -count=1
~~~

Expected: FAIL because CoreStore and New are undefined.

- [ ] **Step 3: Implement entries, counters, and construction**

Use these core data structures:

~~~go
type entry struct {
    key        string
    value      ValueRef
    size       int64
    cost       int64
    expiresAt  int64
    generation uint64
}

type shard struct {
    mu      sync.RWMutex
    entries map[string]*entry
    policy  slru
    bytes   int64
}

type touchEvent struct {
    shardID uint32
    key string
    generation uint64
}

type CoreStore struct {
    cfg Config
    arena Arena
    clock clock
    hash hashFunc
    shards []shard
    staging *byteBudget
    wheel *timingWheel
    touches chan touchEvent
    pressure chan struct{}
    generation atomic.Uint64
    liveBytes atomic.Int64
    payloadBytes atomic.Int64
    entryCount atomic.Int64
    counters storeCounters
    gate operationGate
    stop chan struct{}
    maintenanceDone chan struct{}
    closeOnce sync.Once
    closeDone chan struct{}
    evictionMu sync.Mutex
    evictionCursor uint32
}
~~~

Stats exposes CapacityBytes, LiveBytes, PayloadBytes, StagingBytes, Entries, Gets, Hits, Misses, Puts, Deletes, Evictions, Expirations, RejectedPuts, and TouchDrops. storeCounters uses atomic.Uint64 fields. Stats reads atomics and staging.usedBytes without taking shard locks.

New validates Config and calls newCoreStore with NewHeapArena, realClock, protocolHash, and maintenance enabled. Tests call the same internal constructor with a fake clock or maintenance disabled. Initialize each shard map and SLRU protected limit as max(1, CapacityBytes/ShardCount*80/100).

Create the shared test support with concrete constructors:

~~~go
type readerFunc func([]byte) (int, error)
func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

type manualClock struct {
    mu sync.Mutex
    now time.Time
}
func newManualClock(now time.Time) *manualClock { return &manualClock{now: now} }
func (c *manualClock) Now() time.Time {
    c.mu.Lock()
    defer c.mu.Unlock()
    return c.now
}
func (c *manualClock) Advance(d time.Duration) {
    c.mu.Lock()
    c.now = c.now.Add(d)
    c.mu.Unlock()
}

func compactConfig() Config {
    return Config{
        CapacityBytes: 4096, MaxObjectBytes: 1024, MaxStagingBytes: 2048,
        ChunkBytes: 64, ShardCount: 4, TTLResolution: time.Second, TouchBuffer: 16,
    }
}

func newTestStore(t testing.TB) *CoreStore {
    t.Helper()
    return newTestStoreWithClock(t, newManualClock(time.Unix(100, 0)), false)
}

func newTestStoreWithClock(t testing.TB, c clock, maintenance bool) *CoreStore {
    t.Helper()
    cfg := compactConfig()
    return newConfiguredTestStore(t, cfg, c, maintenance)
}

func newConfiguredTestStore(t testing.TB, cfg Config, c clock, maintenance bool) *CoreStore {
    t.Helper()
    s, err := newCoreStore(cfg, coreDependencies{
        arena: NewHeapArena(cfg.ChunkBytes), clock: c, hash: protocolHash,
        startMaintenance: maintenance,
    })
    if err != nil { t.Fatal(err) }
    t.Cleanup(func() { _ = s.Close() })
    return s
}

func putString(t testing.TB, s *CoreStore, key, value string) {
    t.Helper()
    if _, err := s.Put(context.Background(), []byte(key), strings.NewReader(value), PutOptions{Size: int64(len(value))}); err != nil {
        t.Fatal(err)
    }
}

func assertValue(t testing.TB, s *CoreStore, key, want string) {
    t.Helper()
    object, err := s.Get(context.Background(), []byte(key))
    if err != nil { t.Fatal(err) }
    got, err := io.ReadAll(object)
    object.Close()
    if err != nil || string(got) != want { t.Fatalf("got=%q want=%q err=%v", got, want, err) }
}

func costFor(key string, payloadBytes int) int64 {
    return int64(payloadBytes+len(key)) + entryOverheadBytes
}

func waitForActiveOperations(t testing.TB, gate *operationGate, want int64) {
    t.Helper()
    deadline := time.Now().Add(5 * time.Second)
    for gate.activeCount.Load() < want {
        if time.Now().After(deadline) {
            t.Fatalf("active operations = %d, want %d", gate.activeCount.Load(), want)
        }
        runtime.Gosched()
    }
}

func newCapacityTestStore(t testing.TB, capacity int64) *CoreStore {
    t.Helper()
    return newCapacityStoreWithClock(t, capacity, newManualClock(time.Unix(100, 0)))
}

func newCapacityStoreWithClock(t testing.TB, capacity int64, c clock) *CoreStore {
    t.Helper()
    cfg := compactConfig()
    cfg.CapacityBytes = capacity
    return newConfiguredTestStore(t, cfg, c, false)
}

func newTestStoreWithTouchBuffer(t testing.TB, size int, maintenance bool) *CoreStore {
    t.Helper()
    cfg := compactConfig()
    cfg.TouchBuffer = size
    return newConfiguredTestStore(t, cfg, newManualClock(time.Unix(100, 0)), maintenance)
}

func newFuzzStore(t testing.TB) *CoreStore {
    t.Helper()
    cfg := compactConfig()
    cfg.CapacityBytes = 16 << 20
    cfg.MaxObjectBytes = 4096
    cfg.MaxStagingBytes = 8192
    return newConfiguredTestStore(t, cfg, newManualClock(time.Unix(100, 0)), false)
}

func newBenchmarkStore(b *testing.B, capacity int64) *CoreStore {
    b.Helper()
    cfg := DefaultConfig()
    cfg.CapacityBytes = capacity
    cfg.MaxObjectBytes = capacity / 2
    cfg.MaxStagingBytes = cfg.MaxObjectBytes
    if int64(cfg.ChunkBytes) > cfg.MaxObjectBytes { cfg.ChunkBytes = int(cfg.MaxObjectBytes) }
    cfg.ShardCount = 1024
    return newConfiguredTestStore(b, cfg, realClock{}, true)
}
~~~

Define time conversion without turning no-TTL into the Unix epoch:

~~~go
func timeFromUnixNano(value int64) time.Time {
    if value == 0 { return time.Time{} }
    return time.Unix(0, value)
}
~~~

Create operationGate in lifecycle.go now so every Store method compiles:

~~~go
type operationGate struct {
    mu sync.Mutex
    closed bool
    active sync.WaitGroup
    activeCount atomic.Int64
}

func (g *operationGate) enter() error {
    g.mu.Lock()
    defer g.mu.Unlock()
    if g.closed { return ErrClosed }
    g.active.Add(1)
    g.activeCount.Add(1)
    return nil
}

func (g *operationGate) leave() {
    g.activeCount.Add(-1)
    g.active.Done()
}

func (g *operationGate) closeAdmission() {
    g.mu.Lock()
    g.closed = true
    g.mu.Unlock()
}

func (g *operationGate) wait() { g.active.Wait() }
~~~

The mutex around closed and WaitGroup.Add makes it safe to call wait after closeAdmission. Task 9 extends Store Close sequencing around this gate.

Task 6 uses this deliberately minimal Close, which is sufficient for CRUD tests and leaves the staging-wakeup and index-drain behavior failing for Task 9:

~~~go
func (s *CoreStore) Close() error {
    s.closeOnce.Do(func() {
        s.gate.closeAdmission()
        s.gate.wait()
        s.closeErr = s.arena.Close()
        close(s.closeDone)
    })
    <-s.closeDone
    return s.closeErr
}
~~~

Add closeErr to CoreStore. Until Task 7 installs the maintenance loop, newCoreStore creates maintenanceDone as an already-closed channel even when the dependency requests maintenance; Task 7 changes construction to start the ticker loop.

- [ ] **Step 4: Implement atomic Put**

Put follows this exact ownership sequence:

~~~go
func (s *CoreStore) Put(ctx context.Context, key []byte, src io.Reader, opts PutOptions) (ObjectInfo, error) {
    if err := s.gate.enter(); err != nil { return ObjectInfo{}, err }
    defer s.gate.leave()
    if err := ctx.Err(); err != nil { return ObjectInfo{}, err }
    if err := validateKey(key); err != nil { s.rejectPut(ctx); return ObjectInfo{}, err }
    if opts.Size < 0 { s.rejectPut(ctx); return ObjectInfo{}, ErrSizeMismatch }
    if opts.Size > s.cfg.MaxObjectBytes { s.rejectPut(ctx); return ObjectInfo{}, ErrObjectTooLarge }
    if opts.TTL < 0 { s.rejectPut(ctx); return ObjectInfo{}, ErrInvalidTTL }
    if err := s.staging.reserve(ctx, opts.Size); err != nil { s.rejectPut(ctx); return ObjectInfo{}, err }
    defer s.staging.release(opts.Size)
    ref, err := s.arena.Write(ctx, src, opts.Size)
    if err != nil { s.rejectPut(ctx); return ObjectInfo{}, err }
    committed := false
    defer func() { if !committed { s.arena.Release(ref) } }()
    if err := ctx.Err(); err != nil { return ObjectInfo{}, err }

    immutableKey := string(key)
    id := shardIDWithHash(key, s.cfg.ShardCount, s.hash)
    sh := &s.shards[id]
    sh.mu.Lock()
    old := sh.entries[immutableKey]
    cost := opts.Size + int64(len(immutableKey)) + entryOverheadBytes
    delta := cost
    if old != nil { delta -= old.cost }
    if delta > 0 && !s.reserveLive(delta) {
        sh.mu.Unlock()
        s.rejectPut(ctx)
        return ObjectInfo{}, ErrNoCapacity
    }
    now := s.clock.Now()
    expiresAt := int64(0)
    if opts.TTL > 0 { expiresAt = now.Add(opts.TTL).UnixNano() }
    generation := s.generation.Add(1)
    current := &entry{key: immutableKey, value: ref, size: opts.Size, cost: cost, expiresAt: expiresAt, generation: generation}
    sh.entries[immutableKey] = current
    sh.policy.insert(immutableKey, generation, cost)
    sh.bytes += delta
    if delta < 0 { s.liveBytes.Add(delta) }
    payloadDelta := opts.Size
    if old != nil { payloadDelta -= old.size } else { s.entryCount.Add(1) }
    s.payloadBytes.Add(payloadDelta)
    sh.mu.Unlock()

    committed = true
    if old != nil { s.arena.Release(old.value) }
    if expiresAt != 0 {
        s.wheel.schedule(expirationEvent{shardID: id, key: immutableKey, generation: generation, expiresAt: expiresAt})
    }
    s.counters.puts.Add(1)
    return ObjectInfo{Size: opts.Size, ExpiresAt: timeFromUnixNano(expiresAt)}, nil
}
~~~

reserveLive uses a compare-and-swap loop and never permits liveBytes+delta to exceed CapacityBytes. rejectPut does not increment RejectedPuts for context cancellation or deadline expiration.

- [ ] **Step 5: Implement snapshot Get and Delete**

Get increments Gets once. Under shard RLock it rejects missing or expired entries, calls Arena.Open before unlocking, then sends a touchEvent with a non-blocking select. A successful Get increments Hits and returns a storeObject containing the Arena reader and immutable ObjectInfo. A miss increments Misses and returns ErrNotFound.

Delete acquires one shard write lock, removes the matching entry and SLRU node, decrements shard bytes, live bytes, payload bytes, and entry count, then unlocks before Arena.Release. It returns false for missing or expired data; only a successful live deletion increments Deletes.

storeObject delegates Read, closes its Arena reader once, and returns its captured ObjectInfo:

~~~go
type storeObject struct {
    reader io.ReadCloser
    info ObjectInfo
    once sync.Once
    closeErr error
}

func (o *storeObject) Read(p []byte) (int, error) { return o.reader.Read(p) }
func (o *storeObject) Info() ObjectInfo { return o.info }
func (o *storeObject) Close() error {
    o.once.Do(func() { o.closeErr = o.reader.Close() })
    return o.closeErr
}
~~~

- [ ] **Step 6: Format and verify CRUD**

~~~bash
gofmt -w internal/store/stats.go internal/store/entry.go internal/store/core.go internal/store/operations.go internal/store/store_test.go
go test ./internal/store -run TestCoreStore -count=1
~~~

Expected: PASS.

- [ ] **Step 7: Commit**

~~~bash
git add internal/store/stats.go internal/store/entry.go internal/store/core.go internal/store/operations.go internal/store/store_test.go
git commit -m "feat: implement atomic in-memory store"
~~~

## Task 7: Exact TTL, Lazy Expiration, and Maintenance

**Files:**
- Create: **internal/store/maintenance.go**
- Create: **internal/store/ttl_test.go**
- Modify: **internal/store/core.go**
- Modify: **internal/store/operations.go**
- Modify: **internal/store/entry.go**

- [ ] **Step 1: Write failing fake-clock TTL tests**

~~~go
func TestTTLStartsAtCommitAndLazyGetNeverReturnsExpiredValue(t *testing.T) {
    clock := newManualClock(time.Unix(100, 0))
    s := newTestStoreWithClock(t, clock, false)
    info, err := s.Put(context.Background(), []byte("k"), strings.NewReader("v"), PutOptions{Size: 1, TTL: 2 * time.Second})
    if err != nil { t.Fatal(err) }
    if !info.ExpiresAt.Equal(clock.Now().Add(2 * time.Second)) { t.Fatalf("expires = %s", info.ExpiresAt) }
    clock.Advance(2 * time.Second)
    if _, err := s.Get(context.Background(), []byte("k")); !errors.Is(err, ErrNotFound) {
        t.Fatalf("error = %v", err)
    }
    stats := s.Stats()
    if stats.Expirations != 1 || stats.Entries != 0 { t.Fatalf("stats = %+v", stats) }
}

func TestStaleExpirationCannotDeleteReplacement(t *testing.T) {
    clock := newManualClock(time.Unix(100, 0))
    s := newTestStoreWithClock(t, clock, false)
    s.Put(context.Background(), []byte("k"), strings.NewReader("a"), PutOptions{Size: 1, TTL: time.Second})
    s.Put(context.Background(), []byte("k"), strings.NewReader("b"), PutOptions{Size: 1})
    clock.Advance(time.Second)
    s.maintenanceStep(clock.Now())
    object, err := s.Get(context.Background(), []byte("k"))
    if err != nil { t.Fatal(err) }
    got, _ := io.ReadAll(object)
    object.Close()
    if string(got) != "b" { t.Fatalf("value = %q", got) }
}
~~~

Use these exact expiry ownership tests:

~~~go
func TestDeleteExpiredEntryReturnsFalse(t *testing.T) {
    clock := newManualClock(time.Unix(100, 0))
    s := newTestStoreWithClock(t, clock, false)
    s.Put(context.Background(), []byte("k"), strings.NewReader("v"), PutOptions{Size: 1, TTL: time.Second})
    clock.Advance(time.Second)
    deleted, err := s.Delete(context.Background(), []byte("k"))
    if err != nil || deleted { t.Fatalf("deleted=%v err=%v", deleted, err) }
    if stats := s.Stats(); stats.Entries != 0 || stats.Expirations != 1 {
        t.Fatalf("stats = %+v", stats)
    }
}

func TestExpirationKeepsOpenReaderValid(t *testing.T) {
    clock := newManualClock(time.Unix(100, 0))
    s := newTestStoreWithClock(t, clock, false)
    s.Put(context.Background(), []byte("k"), strings.NewReader("value"), PutOptions{Size: 5, TTL: time.Second})
    object, err := s.Get(context.Background(), []byte("k"))
    if err != nil { t.Fatal(err) }
    clock.Advance(time.Second)
    s.maintenanceStep(clock.Now())
    got, err := io.ReadAll(object)
    if err != nil || string(got) != "value" { t.Fatalf("got=%q err=%v", got, err) }
    object.Close()
}
~~~

- [ ] **Step 2: Run and verify failure**

~~~bash
go test ./internal/store -run "Test(TTL|StaleExpiration)" -count=1
~~~

Expected: FAIL because maintenanceStep and manual clock helpers are absent or lazy cleanup is incomplete.

- [ ] **Step 3: Implement generation-checked cleanup**

Add expireIfMatch:

~~~go
func (s *CoreStore) expireIfMatch(id uint32, key string, generation uint64, now int64) bool {
    sh := &s.shards[id]
    sh.mu.Lock()
    current := sh.entries[key]
    if current == nil || current.generation != generation || current.expiresAt == 0 || current.expiresAt > now {
        sh.mu.Unlock()
        return false
    }
    ref := s.removeEntryLocked(sh, current)
    sh.mu.Unlock()
    s.arena.Release(ref)
    s.counters.expirations.Add(1)
    return true
}
~~~

Get captures the expired generation under RLock, unlocks, calls expireIfMatch, counts a miss, and returns ErrNotFound. Delete checks expiry under its write lock, removes it as an expiration, releases after unlock, and returns false.

- [ ] **Step 4: Implement maintenance**

maintenanceStep performs these actions in order:

1. Take due wheel events for now and call expireIfMatch for each.
2. Drain at most TouchBuffer queued hit events, validating key and generation under one shard lock per event.
3. If live usage is at or above the high watermark, invoke background eviction toward the low watermark.

The production loop uses time.NewTicker(TTLResolution), also wakes on pressure signals, and exits on the Store stop channel. Tests call maintenanceStep directly and never sleep.

- [ ] **Step 5: Format and verify TTL**

~~~bash
gofmt -w internal/store/core.go internal/store/operations.go internal/store/entry.go internal/store/maintenance.go internal/store/ttl_test.go
go test ./internal/store -run "Test(TTL|StaleExpiration|Expired)" -count=1
~~~

Expected: PASS.

- [ ] **Step 6: Commit**

~~~bash
git add internal/store/core.go internal/store/operations.go internal/store/entry.go internal/store/maintenance.go internal/store/ttl_test.go
git commit -m "feat: expire entries with generation fencing"
~~~

## Task 8: Hard Capacity and Pressure Eviction

**Files:**
- Create: **internal/store/eviction.go**
- Create: **internal/store/eviction_test.go**
- Modify: **internal/store/operations.go**
- Modify: **internal/store/maintenance.go**

- [ ] **Step 1: Write failing capacity and victim tests**

~~~go
func TestPutEvictsProbationLRUToFit(t *testing.T) {
    const valueBytes = 960
    s := newCapacityTestStore(t, costFor("a", valueBytes)+costFor("b", valueBytes))
    putString(t, s, "a", strings.Repeat("a", valueBytes))
    putString(t, s, "b", strings.Repeat("b", valueBytes))
    putString(t, s, "c", strings.Repeat("c", valueBytes))
    if _, err := s.Get(context.Background(), []byte("a")); !errors.Is(err, ErrNotFound) {
        t.Fatalf("a error = %v", err)
    }
    assertValue(t, s, "b", strings.Repeat("b", valueBytes))
    assertValue(t, s, "c", strings.Repeat("c", valueBytes))
    if stats := s.Stats(); stats.LiveBytes > stats.CapacityBytes || stats.Evictions != 1 {
        t.Fatalf("stats = %+v", stats)
    }
}

func TestFailedReplacementPreservesOldValue(t *testing.T) {
    s := newCapacityTestStore(t, 4096)
    old := strings.Repeat("o", 960)
    putString(t, s, "k", old)
    // Simulate an operator capacity reduction after startup to force an
    // otherwise valid replacement through the failed-admission path.
    s.cfg.CapacityBytes = s.liveBytes.Load()
    larger := strings.Repeat("n", 1024)
    _, err := s.Put(context.Background(), []byte("k"), strings.NewReader(larger), PutOptions{Size: int64(len(larger))})
    if !errors.Is(err, ErrNoCapacity) { t.Fatalf("error = %v", err) }
    assertValue(t, s, "k", old)
}

func TestFailedStageDoesNotEvictLiveData(t *testing.T) {
    const valueBytes = 960
    s := newCapacityTestStore(t, 2*costFor("a", valueBytes))
    value := strings.Repeat("a", valueBytes)
    putString(t, s, "a", value)
    _, err := s.Put(context.Background(), []byte("b"), strings.NewReader("x"), PutOptions{Size: valueBytes})
    if !errors.Is(err, ErrSizeMismatch) { t.Fatalf("error = %v", err) }
    assertValue(t, s, "a", value)
    if got := s.Stats().Evictions; got != 0 { t.Fatalf("evictions = %d", got) }
}
~~~

Use these focused pressure checks:

~~~go
func TestConcurrentCapacityNeverOverAdmits(t *testing.T) {
    const valueBytes = 256
    s := newCapacityTestStore(t, 8*costFor("k00", valueBytes))
    var wg sync.WaitGroup
    errCh := make(chan error, 32)
    for i := 0; i < 32; i++ {
        wg.Add(1)
        go func(i int) {
            defer wg.Done()
            key := fmt.Sprintf("k%02d", i)
            _, err := s.Put(context.Background(), []byte(key), bytes.NewReader(make([]byte, valueBytes)), PutOptions{Size: valueBytes})
            if err != nil && !errors.Is(err, ErrNoCapacity) { errCh <- err; return }
            stats := s.Stats()
            if stats.LiveBytes > stats.CapacityBytes {
                errCh <- fmt.Errorf("live=%d capacity=%d", stats.LiveBytes, stats.CapacityBytes)
            }
        }(i)
    }
    wg.Wait()
    close(errCh)
    for err := range errCh { t.Error(err) }
}

func TestPressureRemovesExpiredBeforeLiveVictim(t *testing.T) {
    const valueBytes = 960
    clock := newManualClock(time.Unix(100, 0))
    s := newCapacityStoreWithClock(t, 2*costFor("a", valueBytes), clock)
    a := strings.Repeat("a", valueBytes)
    b := strings.Repeat("b", valueBytes)
    c := strings.Repeat("c", valueBytes)
    s.Put(context.Background(), []byte("a"), strings.NewReader(a), PutOptions{Size: valueBytes, TTL: time.Second})
    putString(t, s, "b", b)
    clock.Advance(time.Second)
    putString(t, s, "c", c)
    assertValue(t, s, "b", b)
    assertValue(t, s, "c", c)
    if stats := s.Stats(); stats.Expirations != 1 || stats.Evictions != 0 {
        t.Fatalf("stats = %+v", stats)
    }
}

func TestHitQueueDropDoesNotFailGet(t *testing.T) {
    s := newTestStoreWithTouchBuffer(t, 1, false)
    putString(t, s, "k", "value")
    first, err := s.Get(context.Background(), []byte("k"))
    if err != nil { t.Fatal(err) }
    first.Close()
    second, err := s.Get(context.Background(), []byte("k"))
    if err != nil { t.Fatal(err) }
    second.Close()
    if got := s.Stats().TouchDrops; got != 1 { t.Fatalf("touch drops = %d", got) }
}
~~~

The existing TestFailedReplacementPreservesOldValue exercises replacement exclusion. TestSLRUPromotionDemotionAndVictimOrder provides protected-vs-probation ordering at policy level.

- [ ] **Step 2: Run and verify failure**

~~~bash
go test ./internal/store -run "Test(PutEvicts|FailedReplacement|FailedStage|ConcurrentCapacity)" -count=1
~~~

Expected: FAIL because Put returns ErrNoCapacity without eviction.

- [ ] **Step 3: Implement bounded rotating eviction**

Use an exclusion carrying shard, key, and generation. makeRoom serializes with evictionMu, snapshots entryCount as the victim-attempt limit, and loops from evictionCursor:

~~~go
func (s *CoreStore) makeRoom(ctx context.Context, needed int64, exclude evictionExclusion) int64 {
    if needed <= 0 { return 0 }
    s.evictionMu.Lock()
    defer s.evictionMu.Unlock()
    victimLimit := s.entryCount.Load()
    var freed, victims int64
    emptyVisits := uint32(0)
    for freed < needed && victims < victimLimit && emptyVisits < s.cfg.ShardCount {
        if ctx.Err() != nil { break }
        id := s.evictionCursor & (s.cfg.ShardCount - 1)
        s.evictionCursor++
        removed, bytes := s.evictOneFromShard(id, exclude, s.clock.Now().UnixNano())
        if removed {
            victims++
            freed += bytes
            emptyVisits = 0
        } else {
            emptyVisits++
        }
    }
    return freed
}
~~~

evictOneFromShard locks one shard, removes all due expired entries first, then asks SLRU for one candidate excluding the replacement generation. It validates the candidate against the entry map, removes it through removeEntryLocked, unlocks, releases all ValueRefs, and increments Expirations or Evictions correctly. No Arena.Release runs under a shard write lock.

- [ ] **Step 4: Add three-attempt admission to Put**

After staging succeeds, Put loops at most maxAdmissionAttempts. Each iteration re-locks the shard and re-reads the old entry. If the positive delta cannot be reserved, it unlocks, calculates the current shortage, calls makeRoom with the old entry excluded, checks context, and retries. If all attempts fail, it releases the staged ValueRef, increments RejectedPuts for a non-context failure, and returns ErrNoCapacity without removing the old value.

After a successful Put, signal the buffered pressure channel when liveBytes*100 is at least CapacityBytes*95. The send is non-blocking.

- [ ] **Step 5: Implement low-watermark background eviction**

~~~go
func (s *CoreStore) evictToLowWatermark(ctx context.Context) {
    low := s.cfg.CapacityBytes * lowWatermarkNumerator / watermarkDenominator
    used := s.liveBytes.Load()
    if used <= low { return }
    s.makeRoom(ctx, used-low, evictionExclusion{})
}
~~~

maintenanceStep invokes this only when usage is at least the 95 percent high watermark.

- [ ] **Step 6: Format and verify capacity**

~~~bash
gofmt -w internal/store/eviction.go internal/store/eviction_test.go internal/store/operations.go internal/store/maintenance.go
go test ./internal/store -run "Test(PutEvicts|FailedReplacement|FailedStage|ConcurrentCapacity|SLRU|Touch)" -count=1
~~~

Expected: PASS.

- [ ] **Step 7: Commit**

~~~bash
git add internal/store/eviction.go internal/store/eviction_test.go internal/store/operations.go internal/store/maintenance.go
git commit -m "feat: enforce byte capacity with slru eviction"
~~~

## Task 9: Close Semantics and Concurrency Races

**Files:**
- Create: **internal/store/lifecycle.go**
- Create: **internal/store/lifecycle_test.go**
- Modify: **internal/store/lifecycle.go**
- Modify: **internal/store/core.go**
- Modify: **internal/store/operations.go**

- [ ] **Step 1: Write failing lifecycle tests**

~~~go
func TestCloseRejectsOperationsAndKeepsOpenReaderValid(t *testing.T) {
    s := newTestStore(t)
    putString(t, s, "k", "value")
    object, err := s.Get(context.Background(), []byte("k"))
    if err != nil { t.Fatal(err) }
    if err := s.Close(); err != nil { t.Fatal(err) }
    if err := s.Close(); err != nil { t.Fatal(err) }
    if _, err := s.Get(context.Background(), []byte("k")); !errors.Is(err, ErrClosed) { t.Fatalf("get = %v", err) }
    if _, err := s.Put(context.Background(), []byte("k"), strings.NewReader("x"), PutOptions{Size: 1}); !errors.Is(err, ErrClosed) { t.Fatalf("put = %v", err) }
    if _, err := s.Delete(context.Background(), []byte("k")); !errors.Is(err, ErrClosed) { t.Fatalf("delete = %v", err) }
    got, err := io.ReadAll(object)
    if err != nil || string(got) != "value" { t.Fatalf("got=%q err=%v", got, err) }
    object.Close()
}

func TestCloseWakesStagingWaiter(t *testing.T) {
    s := newTestStore(t)
    if err := s.staging.reserve(context.Background(), s.cfg.MaxStagingBytes); err != nil { t.Fatal(err) }
    result := make(chan error, 1)
    go func() {
        _, err := s.Put(context.Background(), []byte("k"), strings.NewReader("x"), PutOptions{Size: 1})
        result <- err
    }()
    waitForActiveOperations(t, &s.gate, 1)
    closeDone := make(chan error, 1)
    go func() { closeDone <- s.Close() }()
    if err := <-result; !errors.Is(err, ErrClosed) { t.Fatalf("put error = %v", err) }
    s.staging.release(s.cfg.MaxStagingBytes)
    if err := <-closeDone; err != nil { t.Fatal(err) }
}
~~~

Use a bounded completion test for the Close race:

~~~go
func TestConcurrentOperationsFinishDuringClose(t *testing.T) {
    s := newTestStore(t)
    ctx, cancel := context.WithCancel(context.Background())
    var wg sync.WaitGroup
    for worker := 0; worker < 16; worker++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            key := []byte(fmt.Sprintf("k-%d", id))
            for ctx.Err() == nil {
                _, err := s.Put(ctx, key, strings.NewReader("v"), PutOptions{Size: 1})
                if errors.Is(err, ErrClosed) || errors.Is(err, context.Canceled) { return }
                object, err := s.Get(ctx, key)
                if err == nil { object.Close() }
                _, _ = s.Delete(ctx, key)
            }
        }(worker)
    }
    if err := s.Close(); err != nil { t.Fatal(err) }
    cancel()
    done := make(chan struct{})
    go func() { wg.Wait(); close(done) }()
    select {
    case <-done:
    case <-time.After(5 * time.Second):
        t.Fatal("operations did not finish after Close")
    }
}
~~~

- [ ] **Step 2: Run and verify failure**

~~~bash
go test ./internal/store -run TestClose -count=1
~~~

Expected: FAIL because the lifecycle gate and Close sequencing are incomplete.

- [ ] **Step 3: Replace provisional Close with full shutdown**

~~~go
func (s *CoreStore) Close() error {
    s.closeOnce.Do(func() {
        s.gate.closeAdmission()
        s.staging.close()
        close(s.stop)
        s.gate.wait()
        <-s.maintenanceDone

        refs := make([]ValueRef, 0, s.entryCount.Load())
        for i := range s.shards {
            sh := &s.shards[i]
            sh.mu.Lock()
            for _, current := range sh.entries { refs = append(refs, current.value) }
            sh.entries = make(map[string]*entry)
            sh.policy = newSLRU(s.protectedLimit())
            sh.bytes = 0
            sh.mu.Unlock()
        }
        s.liveBytes.Store(0)
        s.payloadBytes.Store(0)
        s.entryCount.Store(0)
        for _, ref := range refs { s.arena.Release(ref) }
        s.closeErr = s.arena.Close()
        close(s.closeDone)
    })
    <-s.closeDone
    return s.closeErr
}
~~~

When maintenance is disabled, newCoreStore supplies an already-closed maintenanceDone channel. Stats therefore remains callable and reports zero Entries, LiveBytes, and PayloadBytes after Close while cumulative counters remain unchanged.

- [ ] **Step 4: Run lifecycle and race tests**

~~~bash
gofmt -w internal/store/lifecycle.go internal/store/lifecycle_test.go internal/store/core.go internal/store/operations.go
go test ./internal/store -run TestClose -count=1
go test -race ./internal/store/... -count=1
~~~

Expected: both commands PASS with no race report.

- [ ] **Step 5: Commit**

~~~bash
git add internal/store/lifecycle.go internal/store/lifecycle_test.go internal/store/core.go internal/store/operations.go
git commit -m "feat: close store without invalidating readers"
~~~

## Task 10: Model Tests, Fuzzing, Benchmarks, and Full Verification

**Files:**
- Create: **internal/store/model_test.go**
- Create: **internal/store/fuzz_test.go**
- Create: **internal/store/benchmark_test.go**
- Create: **internal/store/large_object_test.go**

- [ ] **Step 1: Add deterministic randomized model coverage**

Use rand.New(rand.NewSource(1)), a fake Clock, a capacity large enough to avoid eviction, and 10,000 operations over 64 binary keys. Mirror Put/Delete/Get in a reference map holding copied values and expiration times. After every Get, compare bytes and not-found state. Advance fake time every 100 operations and call maintenanceStep.

Run:

~~~bash
go test ./internal/store -run TestStoreMatchesReferenceModel -count=1
~~~

Expected: PASS.

- [ ] **Step 2: Add fuzz targets**

~~~go
func FuzzStoreRoundTrip(f *testing.F) {
    f.Add([]byte("key"), []byte("value"))
    f.Fuzz(func(t *testing.T, key, value []byte) {
        if len(key) == 0 || len(key) > maxKeyBytes || len(value) > 4096 { t.Skip() }
        s := newFuzzStore(t)
        if _, err := s.Put(context.Background(), key, bytes.NewReader(value), PutOptions{Size: int64(len(value))}); err != nil { t.Fatal(err) }
        object, err := s.Get(context.Background(), key)
        if err != nil { t.Fatal(err) }
        got, err := io.ReadAll(object)
        object.Close()
        if err != nil || !bytes.Equal(got, value) { t.Fatalf("round trip mismatch") }
    })
}
~~~

Use a bounded declared-size fuzz target:

~~~go
func FuzzHeapArenaDeclaredSize(f *testing.F) {
    f.Add([]byte("value"), int8(0))
    f.Fuzz(func(t *testing.T, value []byte, offset int8) {
        if len(value) > 4096 { t.Skip() }
        normalized := int64(0)
        switch int(offset) % 3 {
        case 1, 2:
            normalized = 1
        case -1, -2:
            normalized = -1
        }
        declared := int64(len(value)) + normalized
        arena := NewHeapArena(64)
        ref, err := arena.Write(context.Background(), bytes.NewReader(value), declared)
        if normalized != 0 {
            if !errors.Is(err, ErrSizeMismatch) { t.Fatalf("error=%v declared=%d", err, declared) }
            return
        }
        if err != nil { t.Fatal(err) }
        reader, err := arena.Open(ref)
        if err != nil { t.Fatal(err) }
        got, err := io.ReadAll(reader)
        reader.Close()
        arena.Release(ref)
        if err != nil || !bytes.Equal(got, value) { t.Fatalf("round trip mismatch") }
    })
}
~~~

Run:

~~~bash
go test ./internal/store -run=^$ -fuzz=FuzzStoreRoundTrip -fuzztime=5s
go test ./internal/store -run=^$ -fuzz=FuzzHeapArenaDeclaredSize -fuzztime=5s
~~~

Expected: both fuzz runs finish without a failure.

- [ ] **Step 3: Add streaming benchmarks**

Define the benchmark with reusable payloads and a bounded Store per subtest:

~~~go
func BenchmarkStoreStreaming(b *testing.B) {
    for _, size := range []int{1 << 10, 1 << 20, 32 << 20} {
        b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
            s := newBenchmarkStore(b, int64(size)*2+(1<<20))
            value := make([]byte, size)
            key := []byte("benchmark")
            b.SetBytes(int64(size))
            b.ReportAllocs()
            b.ResetTimer()
            for i := 0; i < b.N; i++ {
                if _, err := s.Put(context.Background(), key, bytes.NewReader(value), PutOptions{Size: int64(size)}); err != nil {
                    b.Fatal(err)
                }
                object, err := s.Get(context.Background(), key)
                if err != nil { b.Fatal(err) }
                if _, err := io.Copy(io.Discard, object); err != nil { b.Fatal(err) }
                object.Close()
            }
        })
    }
    b.Run("parallel_1KiB", func(b *testing.B) {
        s := newBenchmarkStore(b, 64<<20)
        value := make([]byte, 1<<10)
        var sequence atomic.Uint64
        b.SetBytes(1 << 10)
        b.ReportAllocs()
        b.RunParallel(func(pb *testing.PB) {
            for pb.Next() {
                id := sequence.Add(1)
                key := []byte(strconv.FormatUint(id%1024, 10))
                if _, err := s.Put(context.Background(), key, bytes.NewReader(value), PutOptions{Size: 1 << 10}); err != nil {
                    b.Error(err)
                    return
                }
                object, err := s.Get(context.Background(), key)
                if err == nil { io.Copy(io.Discard, object); object.Close() }
            }
        })
    })
}
~~~

Run a short baseline:

~~~bash
go test ./internal/store -run=^$ -bench=BenchmarkStoreStreaming -benchtime=100ms -benchmem
~~~

Expected: benchmark rows for 1KiB, 1MiB, 32MiB, and parallel operations; no pass/fail throughput threshold.

- [ ] **Step 4: Add opt-in production-boundary test**

Use an infinite zero reader and verify the production boundary:

~~~go
type zeroReader struct{}
func (zeroReader) Read(p []byte) (int, error) {
    clear(p)
    return len(p), nil
}

func TestProductionMaxObject(t *testing.T) {
    if os.Getenv("MINIKV_LARGE_TEST") != "1" { t.Skip("set MINIKV_LARGE_TEST=1") }
    cfg := DefaultConfig()
    s, err := New(cfg)
    if err != nil { t.Fatal(err) }
    defer s.Close()
    src := io.LimitReader(zeroReader{}, cfg.MaxObjectBytes)
    if _, err := s.Put(context.Background(), []byte("max"), src, PutOptions{Size: cfg.MaxObjectBytes}); err != nil {
        t.Fatal(err)
    }
    object, err := s.Get(context.Background(), []byte("max"))
    if err != nil { t.Fatal(err) }
    n, err := io.Copy(io.Discard, object)
    object.Close()
    if err != nil || n != cfg.MaxObjectBytes { t.Fatalf("bytes=%d err=%v", n, err) }
    panicReader := readerFunc(func([]byte) (int, error) { panic("oversized source was read") })
    if _, err := s.Put(context.Background(), []byte("too-large"), panicReader, PutOptions{Size: cfg.MaxObjectBytes + 1}); !errors.Is(err, ErrObjectTooLarge) {
        t.Fatalf("error = %v", err)
    }
}
~~~

Run outside race mode:

~~~bash
$env:MINIKV_LARGE_TEST='1'; go test ./internal/store -run TestProductionMaxObject -count=1
~~~

Expected: PASS.

- [ ] **Step 5: Run formatting and full verification**

Run:

~~~bash
gofmt -w internal/store
go test ./... -count=1
go test -race ./internal/store/... -count=1
go vet ./...
git diff --check
~~~

Expected: all tests and vet pass, race reports no issue, and git diff --check prints no output.

- [ ] **Step 6: Inspect implementation scope**

Run:

~~~bash
git status --short
git diff --stat
~~~

Expected: README.md remains unchanged and empty; changes are limited to go.mod, go.sum, internal/store, the design seed clarification, and this plan.

- [ ] **Step 7: Commit verification assets**

~~~bash
git add internal/store docs/superpowers/specs/2026-07-21-single-node-kernel-design.md docs/superpowers/plans/2026-07-21-single-node-kernel.md
git commit -m "test: verify single-node cache kernel"
~~~

## Completion Gate

Before claiming completion, invoke superpowers:requesting-code-review and address findings, then invoke superpowers:verification-before-completion and rerun:

~~~bash
go test ./... -count=1
go test -race ./internal/store/... -count=1
go vet ./...
git diff --check
git status --short --branch
~~~

Report the exact test results, benchmark availability, branch state, and whether the opt-in 128 MiB test was run. Do not push unless the user explicitly requests it.
