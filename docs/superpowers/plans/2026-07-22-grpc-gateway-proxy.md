# gRPC Gateway Proxy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the single-node Store through a bounded-memory gRPC NodeService and a public CacheService gateway that proxies Put, Get, and Delete to one statically configured backend.

**Architecture:** Versioned Protobuf contracts define client-streaming Put, server-streaming Get, and unary Delete. A gateway validates public frames, resolves one static route, reuses a backend connection, and forwards one frame at a time. A node validates the route and streams directly into or out of the existing Store. Shared XXH3 shard mapping, stable gRPC status translation, health reporting, and graceful shutdown create the extension points for later etcd placement and replication.

**Tech Stack:** Go 1.23, gRPC-Go v1.75.1, Protobuf-Go v1.36.11, protoc-gen-go-grpc v1.5.1, Buf CLI v1.50.0, github.com/zeebo/xxh3 v1.1.0, standard gRPC health service, Go tests/benchmarks/race detector.

## Global Constraints

- Keep `README.md` empty.
- Keep the public package at `minikv.v1`; commit generated Go files under `gen/go/minikv/v1`.
- Keep the repository buildable with Go 1.23. Do not upgrade to gRPC-Go v1.82.0 because it requires Go 1.25.
- Use 1024 logical shards, XXH3-64 seed zero, 256 KiB payload chunks, 1 MiB gRPC send/receive limits, 128 MiB maximum objects, 64 GiB default node capacity, and 512 MiB maximum staging.
- Gateway and node may hold one application chunk plus bounded gRPC transport buffers, never a complete object for transport.
- Put has no automatic retry and `request_id` has no deduplication semantics in this milestone.
- Use plaintext gRPC and loopback defaults only. TLS, etcd, replication, and Python clients remain out of scope.
- Add tests before each behavior, run the focused failing test, implement the minimum behavior, rerun the focused test, then commit.
- Do not start the next task while the current task's focused tests fail.

---

## File Map

- Create `api/minikv/v1/cache.proto`: public and node-internal wire contracts.
- Create `buf.yaml` and `buf.gen.yaml`: lint and version-pinned remote generation configuration.
- Create `gen/go/minikv/v1/cache.pb.go` and `cache_grpc.pb.go`: committed generated bindings.
- Modify `go.mod` and `go.sum`: add compatible gRPC and Protobuf runtimes.
- Create `internal/sharding/sharding.go` and tests: shared protocol hash and shard calculation.
- Delete `internal/store/hash.go`; modify `internal/store/core.go`, `operations.go`, and `hash_test.go`: consume shared sharding code.
- Replace `internal/transport/doc.go` with transport package documentation.
- Create `internal/transport/grpccommon`: limits, frame validation, stream reader, timestamps, and status mapping.
- Create `internal/routing`: Router, StaticRouter, and reusable gRPC BackendPool.
- Create `internal/transport/grpcnode`: NodeService implementation.
- Create `internal/transport/grpcgateway`: CacheService proxy implementation.
- Replace `internal/config/config.go`: explicit role parsing and transport settings.
- Modify `internal/store/config.go`: expose read-only configuration validation to process assembly.
- Replace `internal/server/server.go`; create `internal/server/node.go`, `gateway.go`, and `lifecycle.go`: role assembly, health, and shutdown.
- Modify `cmd/minikv/main.go`: CLI parsing and signal context.
- Create `internal/transport/integration_test.go`, `benchmark_test.go`, and `large_object_test.go`: two-hop verification and baselines.

## Dependency Direction

```text
cmd/minikv
  -> internal/config
  -> internal/server
       -> internal/transport/grpcgateway -> internal/routing
       -> internal/transport/grpcnode    -> internal/store
       -> internal/transport/grpccommon
       -> gen/go/minikv/v1

internal/store   -> internal/sharding
internal/routing -> internal/sharding
```

`internal/store` must not import transport, routing, server, or generated Protobuf packages. `grpccommon` may import Store only for stable error classification; it must not construct or own a Store.

---

## Task 1: Versioned Protobuf Contract and Reproducible Generation

**Files:**
- Create: `api/minikv/v1/cache.proto`
- Create: `buf.yaml`
- Create: `buf.gen.yaml`
- Create: `internal/transport/protocol_contract_test.go`
- Create: `gen/go/minikv/v1/cache.pb.go`
- Create: `gen/go/minikv/v1/cache_grpc.pb.go`
- Modify: `go.mod`
- Modify: `go.sum`

**Interfaces introduced:**

```protobuf
service CacheService {
  rpc Put(stream PutRequest) returns (PutResponse);
  rpc Get(GetRequest) returns (stream GetResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
}

service NodeService {
  rpc Put(stream NodePutRequest) returns (PutResponse);
  rpc Get(NodeGetRequest) returns (stream GetResponse);
  rpc Delete(NodeDeleteRequest) returns (DeleteResponse);
}
```

- [ ] **Step 1: Write the generated-contract test first**

Create `internal/transport/protocol_contract_test.go` in package `transport_test`. Assert that:

```go
func TestProtocolServiceNames(t *testing.T) {
    if got, want := minikvv1.CacheService_ServiceDesc.ServiceName, "minikv.v1.CacheService"; got != want {
        t.Fatalf("CacheService name = %q, want %q", got, want)
    }
    if got, want := minikvv1.NodeService_ServiceDesc.ServiceName, "minikv.v1.NodeService"; got != want {
        t.Fatalf("NodeService name = %q, want %q", got, want)
    }
}

func TestPutRequestOneofSeparatesHeaderAndChunk(t *testing.T) {
    request := &minikvv1.PutRequest{
        Frame: &minikvv1.PutRequest_Header{Header: &minikvv1.PutHeader{Key: []byte("key")}},
    }
    if request.GetHeader() == nil || request.GetChunk() != nil {
        t.Fatalf("unexpected frame: %#v", request.GetFrame())
    }
}
```

- [ ] **Step 2: Run the test and verify RED**

```bash
go test ./internal/transport -run "TestProtocol(ServiceNames|PutRequestOneof)" -count=1
```

Expected: FAIL because `gen/go/minikv/v1` does not exist.

- [ ] **Step 3: Add the complete protocol source**

Create `api/minikv/v1/cache.proto`:

```protobuf
syntax = "proto3";

package minikv.v1;

option go_package = "github.com/qingketsing/Mini-KV-Cache-System/gen/go/minikv/v1;minikvv1";

service CacheService {
  rpc Put(stream PutRequest) returns (PutResponse);
  rpc Get(GetRequest) returns (stream GetResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
}

service NodeService {
  rpc Put(stream NodePutRequest) returns (PutResponse);
  rpc Get(NodeGetRequest) returns (stream GetResponse);
  rpc Delete(NodeDeleteRequest) returns (DeleteResponse);
}

message PutHeader {
  bytes key = 1;
  uint64 value_size = 2;
  uint64 ttl_milliseconds = 3;
  bytes request_id = 4;
}

message DataChunk {
  uint32 sequence = 1;
  bytes data = 2;
}

message PutRequest {
  oneof frame {
    PutHeader header = 1;
    DataChunk chunk = 2;
  }
}

message PutResponse {
  uint64 value_size = 1;
  int64 expires_at_unix_milliseconds = 2;
}

message GetRequest { bytes key = 1; }

message GetHeader {
  uint64 value_size = 1;
  int64 expires_at_unix_milliseconds = 2;
}

message GetResponse {
  oneof frame {
    GetHeader header = 1;
    DataChunk chunk = 2;
  }
}

message DeleteRequest { bytes key = 1; }
message DeleteResponse { bool deleted = 1; }

message RouteContext {
  uint32 shard_id = 1;
  uint64 shard_epoch = 2;
}

message NodePutHeader {
  RouteContext route = 1;
  PutHeader request = 2;
}

message NodePutRequest {
  oneof frame {
    NodePutHeader header = 1;
    DataChunk chunk = 2;
  }
}

message NodeGetRequest {
  RouteContext route = 1;
  GetRequest request = 2;
}

message NodeDeleteRequest {
  RouteContext route = 1;
  DeleteRequest request = 2;
}
```

- [ ] **Step 4: Add Buf configuration with pinned remote plugins**

Use a Buf v2 module rooted at `api`. Create `buf.yaml`:

```yaml
version: v2
modules:
  - path: api
lint:
  use:
    - STANDARD
breaking:
  use:
    - FILE
```

Create `buf.gen.yaml`:

```yaml
version: v2
clean: true
plugins:
  - remote: buf.build/protocolbuffers/go:v1.36.11
    out: gen/go
    opt:
      - paths=source_relative
  - remote: buf.build/grpc/go:v1.5.1
    out: gen/go
    opt:
      - paths=source_relative
      - use_generic_streams_experimental=false
inputs:
  - directory: api
```

The canonical commands, run from the repository root, are:

```bash
go run github.com/bufbuild/buf/cmd/buf@v1.50.0 lint api
go run github.com/bufbuild/buf/cmd/buf@v1.50.0 generate
```

- [ ] **Step 5: Generate bindings and add compatible runtimes**

```bash
go get google.golang.org/grpc@v1.75.1 google.golang.org/protobuf@v1.36.11
go mod edit -go=1.23
go run github.com/bufbuild/buf/cmd/buf@v1.50.0 generate
gofmt -w internal/transport/protocol_contract_test.go
```

Expected generated paths: `gen/go/minikv/v1/cache.pb.go` and `gen/go/minikv/v1/cache_grpc.pb.go`.

- [ ] **Step 6: Verify lint, generated shape, and Go 1.23 directive**

```bash
go run github.com/bufbuild/buf/cmd/buf@v1.50.0 lint api
go test ./internal/transport -run "TestProtocol(ServiceNames|PutRequestOneof)" -count=1
go mod edit -json
git diff --check
```

Expected: lint and tests PASS; `go.mod` still reports `Go: "1.23"`.

- [ ] **Step 7: Commit**

```bash
git add api buf.yaml buf.gen.yaml gen/go internal/transport/protocol_contract_test.go go.mod go.sum
git commit -m "feat: define grpc cache protocol"
```

---

## Task 2: Shared Protocol Sharding

**Files:**
- Create: `internal/sharding/sharding.go`
- Create: `internal/sharding/sharding_test.go`
- Delete: `internal/store/hash.go`
- Modify: `internal/store/hash_test.go`
- Modify: `internal/store/core.go`
- Modify: `internal/store/operations.go`

**Interfaces introduced:**

```go
package sharding

type HashFunc func([]byte) uint64

func Hash(key []byte) uint64
func ID(key []byte, shardCount uint32) (uint32, error)
func IDWithHash(key []byte, shardCount uint32, hash HashFunc) uint32
```

- [ ] **Step 1: Write shared vector and validation tests**

Test the known `"hello"` XXH3 seed-zero hash `0x9555e8555c62dcfd`, shard 253 for 1024 shards, binary keys, zero shards, and non-power-of-two shard counts. Keep one Store-level test proving Store uses the injected shared hash function.

```go
func TestIDUsesProtocolXXH3(t *testing.T) {
    const known = uint64(0x9555e8555c62dcfd)
    if got := Hash([]byte("hello")); got != known {
        t.Fatalf("hash = %#x, want %#x", got, known)
    }
    if got, err := ID([]byte("hello"), 1024); err != nil || got != uint32(known&1023) {
        t.Fatalf("shard = %d, error = %v", got, err)
    }
}
```

- [ ] **Step 2: Run and verify RED**

```bash
go test ./internal/sharding ./internal/store -run "Test(IDUsesProtocolXXH3|StoreUsesSharedHash)" -count=1
```

Expected: FAIL because `internal/sharding` does not exist.

- [ ] **Step 3: Implement the shared mapping**

```go
const Seed uint64 = 0

func Hash(key []byte) uint64 {
    return xxh3.HashSeed(key, Seed)
}

func ID(key []byte, shardCount uint32) (uint32, error) {
    if shardCount == 0 || shardCount&(shardCount-1) != 0 {
        return 0, fmt.Errorf("sharding: shard count must be a power of two")
    }
    return IDWithHash(key, shardCount, Hash), nil
}

func IDWithHash(key []byte, shardCount uint32, hash HashFunc) uint32 {
    return uint32(hash(key) & uint64(shardCount-1))
}
```

Change `coreDependencies.hash` and `CoreStore.hash` to `sharding.HashFunc`, default it to `sharding.Hash`, and replace Store's local shard calculation with `sharding.IDWithHash`. Delete the copied Store hash implementation.

- [ ] **Step 4: Verify Store behavior did not change**

```bash
gofmt -w internal/sharding internal/store/core.go internal/store/operations.go internal/store/hash_test.go
go test ./internal/sharding ./internal/store -count=1
```

Expected: PASS, including the existing Store hash vector.

- [ ] **Step 5: Commit**

```bash
git add internal/sharding internal/store/core.go internal/store/operations.go internal/store/hash_test.go internal/store/hash.go
git commit -m "refactor: share protocol shard mapping"
```

---

## Task 3: Transport Limits, Validation, Stream Reader, and Status Mapping

**Files:**
- Replace: `internal/transport/doc.go`
- Create: `internal/transport/grpccommon/limits.go`
- Create: `internal/transport/grpccommon/validate.go`
- Create: `internal/transport/grpccommon/put_reader.go`
- Create: `internal/transport/grpccommon/status.go`
- Create: `internal/transport/grpccommon/time.go`
- Create: `internal/transport/grpccommon/validate_test.go`
- Create: `internal/transport/grpccommon/put_reader_test.go`
- Create: `internal/transport/grpccommon/status_test.go`

**Interfaces introduced:**

```go
type Limits struct {
    ChunkBytes      int
    MaxMessageBytes int
    MaxObjectBytes  int64
}

func DefaultLimits() Limits
func (l Limits) Validate() error
func ParsePutHeader(*minikvv1.PutHeader, Limits) ([]byte, int64, time.Duration, error)
func ValidateKey([]byte) error
func ValidateChunk(*minikvv1.DataChunk, uint32, int) error
func NewNodePutReader(context.Context, int64, int, func() (*minikvv1.NodePutRequest, error)) io.Reader
func StatusError(error) error
func BackendStatus(context.Context, error) error
func ObjectInfoResponse(store.ObjectInfo) *minikvv1.PutResponse
func ObjectInfoHeader(store.ObjectInfo) *minikvv1.GetHeader
```

- [ ] **Step 1: Write table-driven validation and conversion tests**

Cover valid headers, nil headers, 0-byte values, size above 128 MiB, size above `math.MaxInt64`, TTL multiplication overflow, negative duration impossibility after conversion, request IDs of 0/15/16/17 bytes, empty keys, 1025-byte keys, nil chunks, empty chunks, oversized chunks, and wrong sequences.

Use explicit boundary conversion:

```go
func checkedMilliseconds(value uint64) (time.Duration, error) {
    if value > uint64(math.MaxInt64/int64(time.Millisecond)) {
        return 0, ErrInvalidTTL
    }
    return time.Duration(value) * time.Millisecond, nil
}
```

- [ ] **Step 2: Write stream-reader tests before implementation**

Use a receive closure backed by a slice of `NodePutRequest` frames. Cover:

- a consumer buffer smaller than a received chunk, proving the unread suffix is retained;
- contiguous multi-chunk reads;
- EOF only after the declared byte count and stream EOF;
- stream EOF before the declared byte count;
- an extra chunk after the declared byte count;
- a repeated header;
- empty, oversized, skipped, and duplicate chunks;
- context cancellation before the next receive.

```go
reader := NewNodePutReader(ctx, 6, 4, receive(
    chunk(0, []byte("abcd")),
    chunk(1, []byte("ef")),
))
first := make([]byte, 2)
second := make([]byte, 4)
requireRead(t, reader, first, "ab")
requireRead(t, reader, second, "cdef")
requireEOF(t, reader)
```

- [ ] **Step 3: Run and verify RED**

```bash
go test ./internal/transport/grpccommon -count=1
```

Expected: FAIL because the package is not implemented.

- [ ] **Step 4: Implement limits and frame validation**

Use stable sentinel errors local to `grpccommon` for malformed header, invalid request ID, invalid sequence, empty chunk, oversized chunk, short stream, and long stream. Return wrapped sentinels so `errors.Is` survives Store's reader wrapping. `Limits.Validate` must reserve 64 KiB for Protobuf framing and reject `ChunkBytes > MaxMessageBytes-(64<<10)`.

Default limits:

```go
func DefaultLimits() Limits {
    return Limits{
        ChunkBytes:      256 << 10,
        MaxMessageBytes: 1 << 20,
        MaxObjectBytes:  128 << 20,
    }
}
```

- [ ] **Step 5: Implement the suffix-preserving reader**

The reader owns `pending []byte`, `expectedSequence uint32`, `remaining int64`, and `verifiedEOF bool`. Every refill performs exactly one receive. Once `remaining == 0`, the next `Read` performs one final receive: stream EOF becomes `io.EOF`; any frame becomes `ErrLongStream`. It never starts a goroutine and never buffers more than one received chunk.

- [ ] **Step 6: Implement stable error translation and timestamps**

Map errors using `errors.Is` in this order: caller context; malformed protocol plus `store.ErrInvalidKey`, `store.ErrInvalidTTL`, `store.ErrObjectTooLarge`, and `store.ErrSizeMismatch`; Store capacity; Store not found; Store closed; then internal. Preserve recognized backend gRPC codes but replace backend messages with stable text; when the inbound context is canceled or expired, return that context status instead. Convert an unexpected backend `Unknown` code to `Internal`.

```go
switch {
case errors.Is(err, context.Canceled):
    return status.Error(codes.Canceled, "request canceled")
case errors.Is(err, context.DeadlineExceeded):
    return status.Error(codes.DeadlineExceeded, "request deadline exceeded")
case errors.Is(err, store.ErrNoCapacity):
    return status.Error(codes.ResourceExhausted, "cache capacity unavailable")
case errors.Is(err, store.ErrNotFound):
    return status.Error(codes.NotFound, "key not found")
case errors.Is(err, store.ErrClosed):
    return status.Error(codes.Unavailable, "cache node unavailable")
}
```

Convert a zero `ExpiresAt` to wire value zero. For non-zero times, use `UnixMilli()`.

- [ ] **Step 7: Verify focused behavior**

```bash
gofmt -w internal/transport
go test ./internal/transport/grpccommon -count=1
go test ./internal/store -count=1
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add internal/transport/doc.go internal/transport/grpccommon
git commit -m "feat: validate grpc streaming frames"
```

---

## Task 4: Static Router and Reusable Backend Connections

**Files:**
- Replace: `internal/cluster/doc.go`
- Create: `internal/routing/router.go`
- Create: `internal/routing/static.go`
- Create: `internal/routing/static_test.go`
- Create: `internal/routing/backend_pool.go`
- Create: `internal/routing/backend_pool_test.go`

**Interfaces introduced:**

```go
type Route struct {
    NodeID  string
    Address string
    ShardID uint32
    Epoch   uint64
}

type Router interface {
    Resolve(context.Context, []byte) (Route, error)
    Close() error
}

type DialFunc func(string, ...grpc.DialOption) (*grpc.ClientConn, error)

type BackendPool struct {
    // guarded map[string]*poolEntry; each entry has one ready channel
}

func NewStaticRouter(nodeID, address string, shardCount uint32) (*StaticRouter, error)
func NewBackendPool(DialFunc, ...grpc.DialOption) *BackendPool
func (p *BackendPool) Client(context.Context, string) (minikvv1.NodeServiceClient, error)
func (p *BackendPool) Close() error
```

- [ ] **Step 1: Write StaticRouter tests**

Verify empty node ID/address rejection, invalid shard counts, cancellation, epoch zero, the known `hello` shard vector, binary keys, repeated Resolve calls, and idempotent Close. Resolve after Close returns a routing closed error.

- [ ] **Step 2: Write BackendPool concurrency tests**

Inject a counting dial function. Start 32 goroutines requesting the same address and assert one connection is created and every returned NodeService client shares it. Request a second address and assert a second dial. Cover waiter cancellation while the first dial is blocked. Close twice, assert every connection closes, and assert Get after Close returns a stable pool-closed error.

- [ ] **Step 3: Run and verify RED**

```bash
go test ./internal/routing -count=1
```

Expected: FAIL because routing is not implemented.

- [ ] **Step 4: Implement StaticRouter**

Copy key bytes nowhere. Resolve checks `ctx.Err()`, computes `sharding.ID(key, shardCount)`, and returns the configured node/address with epoch zero. Router validation does not replace public key validation; it only validates router configuration and shard count.

- [ ] **Step 5: Implement single-flight connection reuse**

The production dialer is `grpc.NewClient`. Configure plaintext credentials and default call limits once:

```go
options := []grpc.DialOption{
    grpc.WithTransportCredentials(insecure.NewCredentials()),
    grpc.WithDefaultCallOptions(
        grpc.MaxCallRecvMsgSize(limits.MaxMessageBytes),
        grpc.MaxCallSendMsgSize(limits.MaxMessageBytes),
    ),
}
```

Insert a per-address `poolEntry{ready: make(chan struct{})}` under the mutex before dialing, then release the mutex. Concurrent callers find the entry and wait on either `entry.ready` or their context. The creator stores the connection or error and closes `ready` exactly once. If Close wins during a dial, close the new connection and publish the pool-closed error. `Close` marks the pool closed under lock, snapshots completed connections, then closes them outside the lock. This guarantees one dial attempt per address without holding the global mutex across dial work.

- [ ] **Step 6: Verify focused behavior and race safety**

```bash
gofmt -w internal/routing internal/cluster/doc.go
go test ./internal/routing -count=1
go test -race ./internal/routing -count=1
```

Expected: both PASS and the dial count remains one for a shared address.

- [ ] **Step 7: Commit**

```bash
git add internal/cluster/doc.go internal/routing
git commit -m "feat: add static routing and backend pooling"
```

---

## Task 5: NodeService Streaming Put

**Files:**
- Create: `internal/transport/grpcnode/server.go`
- Create: `internal/transport/grpcnode/route.go`
- Create: `internal/transport/grpcnode/put.go`
- Create: `internal/transport/grpcnode/put_test.go`

**Interfaces introduced:**

```go
type Server struct {
    minikvv1.UnimplementedNodeServiceServer
    store      store.Store
    limits     grpccommon.Limits
    shardCount uint32
}

func New(store.Store, grpccommon.Limits, uint32) (*Server, error)
func (s *Server) Put(minikvv1.NodeService_PutServer) error
```

- [ ] **Step 1: Write direct-node Put tests over bufconn**

Start a real gRPC server with a small real CoreStore. Cover 0 B, 1 B, cross-chunk values, binary keys, TTL metadata, missing header, chunk first, duplicate header, wrong shard, non-zero epoch acceptance, short stream, long stream, wrong sequence, oversized chunk, canceled stream, and capacity exhaustion. After every malformed or canceled Put, call Store.Get and assert `store.ErrNotFound`.

- [ ] **Step 2: Run and verify RED**

```bash
go test ./internal/transport/grpcnode -run TestNodePut -count=1
```

Expected: FAIL because NodeService is not implemented.

- [ ] **Step 3: Implement server construction and route validation**

Reject nil Store, invalid limits, and invalid shard count. For every operation, require non-nil route/request, calculate the expected shard from the request key, and reject a mismatch as `InvalidArgument`. Accept any epoch in this milestone without enforcing ownership.

- [ ] **Step 4: Implement Put without transport buffering**

Handler order:

```go
first, err := stream.Recv()
// Require exactly one NodePutHeader.
key, size, ttl, err := grpccommon.ParsePutHeader(first.GetHeader().GetRequest(), s.limits)
// Validate RouteContext against key and shardCount.
reader := grpccommon.NewNodePutReader(stream.Context(), size, s.limits.ChunkBytes, stream.Recv)
info, err := s.store.Put(stream.Context(), key, reader, store.PutOptions{Size: size, TTL: ttl})
// Translate errors and send ObjectInfoResponse.
return stream.SendAndClose(grpccommon.ObjectInfoResponse(info))
```

Do not read ahead, concatenate chunks, or launch a receiver goroutine. Copy the key before passing it to Store only if generated-message ownership could outlive the call; the current Store converts it to an immutable string during Put.

- [ ] **Step 5: Verify publication and cancellation guarantees**

```bash
gofmt -w internal/transport/grpcnode
go test ./internal/transport/grpcnode -run TestNodePut -count=1
go test -race ./internal/transport/grpcnode -run TestNodePut -count=1
```

Expected: PASS; partial streams never become visible and the race detector is quiet.

- [ ] **Step 6: Commit**

```bash
git add internal/transport/grpcnode
git commit -m "feat: stream puts into cache nodes"
```

---

## Task 6: NodeService Streaming Get and Unary Delete

**Files:**
- Create: `internal/transport/grpcnode/get.go`
- Create: `internal/transport/grpcnode/delete.go`
- Create: `internal/transport/grpcnode/get_test.go`
- Create: `internal/transport/grpcnode/delete_test.go`
- Create: `internal/transport/grpcnode/status_test.go`

**Interfaces implemented:**

```go
func (s *Server) Get(*minikvv1.NodeGetRequest, minikvv1.NodeService_GetServer) error
func (s *Server) Delete(context.Context, *minikvv1.NodeDeleteRequest) (*minikvv1.DeleteResponse, error)
```

- [ ] **Step 1: Write direct-node Get/Delete tests**

Cover header-first responses, contiguous sequence numbers, zero-byte values, exact cross-chunk reconstruction, TTL milliseconds, binary keys, not found, expired values, Delete true then false, invalid routes, caller cancellation, Store closed, and all stable Store error mappings.

Add a tracking Store Object that records `Close` calls. Assert exactly one close on success, client cancellation, read error, and stream send error.

- [ ] **Step 2: Run and verify RED**

```bash
go test ./internal/transport/grpcnode -run "TestNode(Get|Delete|Status)" -count=1
```

Expected: FAIL because Get and Delete are unimplemented.

- [ ] **Step 3: Implement bounded Get streaming**

Call Store.Get, immediately `defer object.Close()`, send exactly one GetHeader, and then read at most `limits.ChunkBytes` per loop. Use `io.ReadFull` against the remaining declared size so an unexpected Store EOF maps to `Internal`. Send one DataChunk and wait for Send to return before reading again.

```go
remaining := info.Size
buffer := make([]byte, s.limits.ChunkBytes)
for sequence := uint32(0); remaining > 0; sequence++ {
    width := int64(len(buffer))
    if remaining < width { width = remaining }
    if _, err := io.ReadFull(object, buffer[:width]); err != nil {
        return status.Error(codes.Internal, "cache object read failed")
    }
    if err := stream.Send(grpccommon.GetChunk(sequence, buffer[:width])); err != nil {
        return grpccommon.StatusError(err)
    }
    remaining -= width
}
```

- [ ] **Step 4: Implement Delete and stable status mapping**

Validate request and route before Store.Delete. Return `{deleted:false}` with OK for absent/expired values. Convert Store errors only through `grpccommon.StatusError`.

- [ ] **Step 5: Verify node service and resource release**

```bash
gofmt -w internal/transport/grpcnode
go test ./internal/transport/grpcnode -count=1
go test -race ./internal/transport/grpcnode -count=1
```

Expected: PASS; every Object is closed on every handler exit.

- [ ] **Step 6: Commit**

```bash
git add internal/transport/grpcnode
git commit -m "feat: stream gets and deletes from cache nodes"
```

---

## Task 7: Gateway Streaming Put Proxy

**Files:**
- Create: `internal/transport/grpcgateway/server.go`
- Create: `internal/transport/grpcgateway/put.go`
- Create: `internal/transport/grpcgateway/put_test.go`

**Interfaces introduced:**

```go
type Backends interface {
    Client(context.Context, string) (minikvv1.NodeServiceClient, error)
    Close() error
}

type Server struct {
    minikvv1.UnimplementedCacheServiceServer
    router   routing.Router
    backends Backends
    limits   grpccommon.Limits
}

func New(routing.Router, Backends, grpccommon.Limits) (*Server, error)
func (s *Server) Put(minikvv1.CacheService_PutServer) error
```

- [ ] **Step 1: Write gateway Put proxy tests**

Use a recording Router and scripted NodeService backend. Verify:

- the first public header is validated before routing;
- Router.Resolve is called exactly once with the header key;
- NodePutHeader contains the selected shard and epoch and the unchanged public header;
- chunks remain ordered and unchanged;
- each inbound Recv is followed by its backend Send;
- client EOF invokes backend CloseAndRecv and gateway SendAndClose;
- backend status codes pass through with sanitized messages;
- routing/pool failures map to Unavailable;
- cancellation reaches the backend context;
- no automatic retry occurs.

For backpressure, block the backend's first chunk Send, expose an inbound receive counter, and assert the gateway has consumed only the header and first chunk until Send is released.

- [ ] **Step 2: Run and verify RED**

```bash
go test ./internal/transport/grpcgateway -run TestGatewayPut -count=1
```

Expected: FAIL because the gateway is not implemented.

- [ ] **Step 3: Implement header handling and route injection**

Receive one frame, require a public PutHeader, call `ParsePutHeader`, resolve the key, acquire the cached backend client, create NodeService.Put with `stream.Context()`, and send one NodePutHeader. Do not forward the public PutRequest header as a second frame.

- [ ] **Step 4: Implement lock-step chunk proxying**

```go
expected := uint32(0)
for {
    request, err := stream.Recv()
    if errors.Is(err, io.EOF) {
        response, backendErr := backend.CloseAndRecv()
        if backendErr != nil { return grpccommon.BackendStatus(stream.Context(), backendErr) }
        return stream.SendAndClose(response)
    }
    if err != nil { return grpccommon.StatusError(err) }
    chunk := request.GetChunk()
    if err := grpccommon.ValidateChunk(chunk, expected, s.limits.ChunkBytes); err != nil {
        return grpccommon.StatusError(err)
    }
    if err := backend.Send(grpccommon.NodeChunk(chunk)); err != nil {
        return grpccommon.BackendStatus(stream.Context(), err)
    }
    expected++
}
```

Track declared bytes at the gateway too. Reject a chunk that would exceed the declared size before forwarding it, and reject client EOF when fewer bytes arrived. The node remains the final publication guard.

- [ ] **Step 5: Verify ordering, flow control, and races**

```bash
gofmt -w internal/transport/grpcgateway
go test ./internal/transport/grpcgateway -run TestGatewayPut -count=1
go test -race ./internal/transport/grpcgateway -run TestGatewayPut -count=1
```

Expected: PASS; the backpressure test proves no read-ahead loop.

- [ ] **Step 6: Commit**

```bash
git add internal/transport/grpcgateway
git commit -m "feat: proxy streaming puts through gateway"
```

---

## Task 8: Gateway Streaming Get and Unary Delete Proxy

**Files:**
- Create: `internal/transport/grpcgateway/get.go`
- Create: `internal/transport/grpcgateway/delete.go`
- Create: `internal/transport/grpcgateway/get_test.go`
- Create: `internal/transport/grpcgateway/delete_test.go`

**Interfaces implemented:**

```go
func (s *Server) Get(*minikvv1.GetRequest, minikvv1.CacheService_GetServer) error
func (s *Server) Delete(context.Context, *minikvv1.DeleteRequest) (*minikvv1.DeleteResponse, error)
```

- [ ] **Step 1: Write gateway Get/Delete tests**

Verify one route resolution, exact RouteContext injection, unchanged public messages, header-first Get forwarding, contiguous chunk forwarding, zero-byte Get, binary keys, Delete booleans, backend status preservation, route/pool failures, cancellation propagation, and backend connection reuse across Put/Get/Delete.

Add a slow public Get stream whose Send blocks. Assert the gateway does not call backend Recv again until the public Send returns.

- [ ] **Step 2: Run and verify RED**

```bash
go test ./internal/transport/grpcgateway -run "TestGateway(Get|Delete)" -count=1
```

Expected: FAIL because Get and Delete are unimplemented.

- [ ] **Step 3: Implement Get as a lock-step response proxy**

Validate the public key, resolve once, acquire a backend, call NodeService.Get with the inbound context and route, then loop one backend Recv followed by one public Send. Validate that the first frame is one header, later frames are contiguous chunks, and total bytes equal the declared size. Treat backend EOF as success only after the exact byte count. A malformed backend stream is an `Internal` error, never an `InvalidArgument` attributed to the public caller.

- [ ] **Step 4: Implement unary Delete**

Validate key, resolve once, call NodeService.Delete with RouteContext, and return the response unchanged. Do not retry any backend call.

- [ ] **Step 5: Verify proxy behavior and connection reuse**

```bash
gofmt -w internal/transport/grpcgateway
go test ./internal/transport/grpcgateway -count=1
go test -race ./internal/transport/grpcgateway -count=1
```

Expected: PASS; slow-client tests prove Get backpressure reaches the backend.

- [ ] **Step 6: Commit**

```bash
git add internal/transport/grpcgateway
git commit -m "feat: proxy gets and deletes through gateway"
```

---

## Task 9: Role Configuration, Health, and Graceful Runtime

**Files:**
- Replace: `internal/config/config.go`
- Create: `internal/config/config_test.go`
- Modify: `internal/store/config.go`
- Modify: `internal/store/config_test.go`
- Replace: `internal/server/server.go`
- Create: `internal/server/node.go`
- Create: `internal/server/gateway.go`
- Create: `internal/server/lifecycle.go`
- Create: `internal/server/server_test.go`
- Modify: `cmd/minikv/main.go`
- Create: `cmd/minikv/main_test.go`

**Interfaces introduced:**

```go
type Role string

const (
    RoleNode    Role = "node"
    RoleGateway Role = "gateway"
)

type Config struct {
    Role            Role
    NodeID          string
    ListenAddr      string
    BackendAddr     string
    ChunkBytes      int
    MaxMessageBytes int
    ShutdownTimeout time.Duration
    Store           store.Config
}

func Parse(args []string) (Config, error)
// In package store:
func (c Config) Validate() error
func New(config.Config) (*Server, error)
func (s *Server) Run(context.Context) error
```

- [ ] **Step 1: Write CLI parsing tests**

Cover no role, unknown role, role-specific defaults, explicit flags, invalid addresses, missing gateway backend, invalid chunk/message relationships, invalid Store configuration, and these accepted commands:

```text
minikv node --listen 127.0.0.1:9091
minikv gateway --listen 127.0.0.1:9090 --backend 127.0.0.1:9091
```

Node defaults to `127.0.0.1:9091`; gateway defaults to `127.0.0.1:9090` with backend `127.0.0.1:9091`.

- [ ] **Step 2: Write runtime lifecycle tests**

Inject listeners and dependencies so tests do not use fixed ports. Assert:

- node health is SERVING after startup and NOT_SERVING during shutdown;
- gateway health is SERVING after its backend connection object is created;
- cancellation stops accepting new RPCs;
- graceful stop waits for an active handler;
- timeout forces Stop;
- node closes Store after handlers stop;
- gateway closes backend connections after handlers stop;
- Run returns listener/server errors and remains idempotent under cancellation.

- [ ] **Step 3: Run and verify RED**

```bash
go test ./internal/config ./internal/server ./cmd/minikv -count=1
```

Expected: FAIL because explicit roles and networking are not implemented.

- [ ] **Step 4: Implement role parsing and validation**

Rename Store's private `validate` method to exported `Validate`, keep all existing checks unchanged, and have `store.New` call it. Use a dedicated `flag.FlagSet` per role with `ContinueOnError`; do not mutate global flags. Start from `store.DefaultConfig()` and `grpccommon.DefaultLimits()`, then call `cfg.Store.Validate()` as part of process configuration validation. Reject extra positional arguments and return errors instead of calling `os.Exit` in config code.

- [ ] **Step 5: Assemble node and gateway gRPC servers**

Server options for both roles:

```go
grpc.NewServer(
    grpc.MaxRecvMsgSize(cfg.MaxMessageBytes),
    grpc.MaxSendMsgSize(cfg.MaxMessageBytes),
)
```

Node construction order: validate config, create listener, create Store, create NodeService, register NodeService and health, set SERVING, serve. Gateway order: validate config, create listener, create StaticRouter, create BackendPool, pre-create the backend client, create CacheService, register CacheService and health, set SERVING, serve.

- [ ] **Step 6: Implement bounded graceful shutdown**

On context cancellation, set health to NOT_SERVING, call `GracefulStop` in one tracked goroutine, and wait for either completion or `ShutdownTimeout`. On timeout call `Stop` and wait for completion. Only then close role resources in reverse ownership order.

- [ ] **Step 7: Wire process signals in main**

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

cfg, err := config.Parse(os.Args[1:])
if err != nil { return err }
srv, err := server.New(cfg)
if err != nil { return err }
return srv.Run(ctx)
```

Keep `main` as the only place that prints an error and exits non-zero; place the testable flow in `run(args []string) error`.

- [ ] **Step 8: Verify runtime behavior**

```bash
gofmt -w internal/config internal/server internal/store/config.go internal/store/config_test.go cmd/minikv
go test ./internal/config ./internal/server ./internal/store ./cmd/minikv -count=1
go test -race ./internal/server ./cmd/minikv -count=1
go build ./...
```

Expected: PASS and the binary builds.

- [ ] **Step 9: Commit**

```bash
git add internal/config internal/server internal/store/config.go internal/store/config_test.go cmd/minikv
git commit -m "feat: run grpc gateway and node roles"
```

---

## Task 10: Two-Hop Integration Tests and Benchmarks

**Files:**
- Create: `internal/transport/integration_test.go`
- Create: `internal/transport/benchmark_test.go`
- Create: `internal/transport/large_object_test.go`

**Test topology:**

```text
generated CacheService client
  -> bufconn gateway gRPC server
  -> shared BackendPool connection
  -> bufconn node gRPC server
  -> real CoreStore and HeapArena
```

- [ ] **Step 1: Build a deterministic two-hop harness**

The harness creates small Store limits, two independent bufconn listeners, real NodeService and CacheService implementations, one StaticRouter, one BackendPool, and a generated public client. Cleanup order is client connection, gateway, pool/router, node, Store. Register cleanup immediately after each resource is created.

- [ ] **Step 2: Add end-to-end behavior tests**

Cover 0 B, 1 B, `ChunkBytes-1`, `ChunkBytes`, `ChunkBytes+1`, 1 MiB, binary keys, TTL metadata, expiration, Delete, not found, capacity exhaustion, short Put, long Put, malformed sequence, and canceled Put/Get. Reconstruct Get responses while asserting header-first framing, exact sequence, and exact total bytes.

- [ ] **Step 3: Prove partial Put invisibility across both hops**

Open a public Put, send header and one partial chunk, assert a concurrent Get returns NotFound, cancel the Put, wait for Canceled, and assert Store Stats reports zero staging bytes within a bounded deadline.

- [ ] **Step 4: Add transport benchmarks**

```go
func BenchmarkGatewayRoundTrip(b *testing.B) {
    for _, size := range []int{1 << 10, 1 << 20, 32 << 20} {
        b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
            b.SetBytes(int64(size))
            b.ReportAllocs()
            // Each iteration streams Put then Get through gateway and node.
        })
    }
}
```

Use a reusable zero-filled payload and unique bounded keys. Do not set a pass/fail throughput threshold in the first baseline.

- [ ] **Step 5: Add the opt-in 128 MiB test**

Gate with `MINIKV_GRPC_LARGE_TEST=1`. Generate chunks from a bounded zero source, stream one 128 MiB value through both hops, stream it back to `io.Discard`, and assert exact bytes. Sample Store Stats before and after and assert staging returns to zero. Do not run this under the race detector.

- [ ] **Step 6: Run focused integration verification**

```bash
gofmt -w internal/transport
go test ./internal/transport -run TestGatewayNodeIntegration -count=1
go test -race ./internal/transport -run "TestGatewayNodeIntegration|TestPartialPut" -count=1
go test ./internal/transport -run=^$ -bench=BenchmarkGatewayRoundTrip -benchtime=100ms -benchmem
$env:MINIKV_GRPC_LARGE_TEST='1'; go test ./internal/transport -run TestGatewayNodeProductionMaxObject -count=1
```

Expected: tests PASS; benchmark prints 1 KiB, 1 MiB, and 32 MiB rows; opt-in test transfers exactly 128 MiB.

- [ ] **Step 7: Commit**

```bash
git add internal/transport/integration_test.go internal/transport/benchmark_test.go internal/transport/large_object_test.go
git commit -m "test: verify two-hop grpc data path"
```

---

## Task 11: Generation Drift, Full Verification, and Design Status

**Files:**
- Modify: `docs/superpowers/specs/2026-07-22-grpc-gateway-proxy-design.md`
- Modify as generated: `gen/go/minikv/v1/cache.pb.go`
- Modify as generated: `gen/go/minikv/v1/cache_grpc.pb.go`

- [ ] **Step 1: Regenerate and prove bindings are clean**

```bash
go run github.com/bufbuild/buf/cmd/buf@v1.50.0 lint api
go run github.com/bufbuild/buf/cmd/buf@v1.50.0 generate
git diff --exit-code -- gen/go
```

Expected: no generated diff.

- [ ] **Step 2: Run complete tests and static analysis**

```bash
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
go build ./...
git diff --check
```

Expected: every command PASS; race reports no issue; diff check prints no output.

- [ ] **Step 3: Re-run the large-object boundary outside race mode**

```bash
$env:MINIKV_GRPC_LARGE_TEST='1'; go test ./internal/transport -run TestGatewayNodeProductionMaxObject -count=1
```

Expected: PASS with exact 128 MiB Put/Get through both gRPC hops.

- [ ] **Step 4: Run the real-listener runtime smoke test**

Add `TestNodeAndGatewayRealListenerSmoke` to `internal/server/server_test.go`. Bind node and gateway listeners to `127.0.0.1:0`, run both roles with cancellable contexts, dial the actual TCP gateway, verify both health services report SERVING, and Put/Get/Delete a cross-chunk value through the generated CacheService client. Cancel both contexts and assert both Run calls return within the configured shutdown timeout.

```bash
go test ./internal/server -run TestNodeAndGatewayRealListenerSmoke -count=1
```

Expected: PASS without fixed ports or external client tools.

- [ ] **Step 5: Mark the approved design implemented**

Change only the spec status line:

```text
Status: Implemented
```

- [ ] **Step 6: Inspect final scope**

```bash
git status --short
git diff --stat
git log --oneline --decorate -12
```

Expected: `README.md` remains empty; changes are limited to the protocol, generated bindings, shared sharding, routing, gRPC transport, runtime configuration/server code, tests, and the approved design status.

- [ ] **Step 7: Commit verification state**

```bash
git add docs/superpowers/specs/2026-07-22-grpc-gateway-proxy-design.md gen/go
git commit -m "chore: finalize grpc proxy milestone"
```

If regeneration produced no diff, include only the spec status file in this commit.

## Completion Gate

Before claiming implementation complete, invoke `superpowers:requesting-code-review` and address every accepted finding. Then invoke `superpowers:verification-before-completion` and rerun:

```bash
go run github.com/bufbuild/buf/cmd/buf@v1.50.0 lint api
go run github.com/bufbuild/buf/cmd/buf@v1.50.0 generate
git diff --exit-code -- gen/go
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
go build ./...
git diff --check
git status --short --branch
```

Report exact command results, benchmark availability, whether the opt-in 128 MiB test ran, branch state, and commit list. Do not push unless the user explicitly requests it.
