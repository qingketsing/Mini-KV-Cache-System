# MiniKV gRPC Gateway Proxy Design

Date: 2026-07-22

Status: Approved for implementation planning

## 1. Purpose

This milestone exposes the completed single-node Store kernel through gRPC and
adds a gateway that proxies all client data to one statically configured storage
node. It establishes the data-plane contract that later etcd routing,
replication, and shard fencing will reuse.

The first version optimizes for a small, verifiable design:

- clients connect only to the gateway;
- the gateway proxies every request and every payload byte;
- Put and Get transfer values as bounded chunks;
- the storage node streams directly into and out of the existing Store API;
- routing uses one static backend behind a replaceable Router interface;
- the protocol reserves shard and epoch fields for the distributed milestones.

## 2. Existing Constraints

The transport must preserve the Store kernel's guarantees:

- 1024 logical shards using protocol-versioned XXH3-64 with seed zero;
- 64 GiB default node capacity;
- 128 MiB maximum object size;
- 512 MiB staging budget;
- complete staging before Put publication;
- immutable snapshot readers;
- exact TTL checks and generation-fenced expiration;
- byte-accounted capacity and SLRU eviction;
- context cancellation and safe Store shutdown.

Normal gRPC messages must remain far below the maximum object size. The default
payload chunk is 256 KiB and the gRPC message limit is 1 MiB. A 128 MiB object
is therefore a sequence of bounded messages, never one unary Protobuf message.

## 3. Scope

### 3.1 Included

- versioned Protobuf definitions for public and node-internal cache services;
- generated Go bindings;
- client-streaming Put;
- server-streaming Get;
- unary Delete;
- gateway-to-node stream proxying;
- a single-backend StaticRouter behind a Router interface;
- shared protocol shard hashing;
- reusable backend gRPC connections;
- Store-to-gRPC error translation;
- standard gRPC health services;
- `minikv node` and `minikv gateway` process roles;
- graceful process shutdown;
- in-memory integration tests and end-to-end benchmarks.

### 3.2 Excluded

- etcd membership, watches, placement, leases, and leader election;
- primary/replica writes and replication streams;
- epoch enforcement or promotion fencing;
- retries or request deduplication for Put;
- smart clients or direct client-to-node traffic;
- batch, range, CAS, or transaction operations;
- compression and application-level checksums;
- TLS, mTLS, authentication, authorization, or tenant quotas;
- Prometheus and OpenTelemetry exporters;
- Python client generation and Python benchmarks;
- WAL, snapshots, restart recovery, and shard migration.

These exclusions are deliberate. The protocol fields and package boundaries
must allow the features to be added without changing the public streaming
semantics.

## 4. Topology

```text
Client
  |
  | CacheService
  v
Gateway
  |-- StaticRouter.Resolve(key)
  |-- BackendPool.Get(address)
  |
  | NodeService
  v
Storage Node
  |
  | Store interface
  v
CoreStore / HeapArena
```

The gateway is the only public gRPC endpoint. A storage node exposes NodeService
to gateways. The first StaticRouter maps every valid key to one configured
backend while still calculating the protocol shard ID for internal requests and
observability.

## 5. Service Contracts

The Protobuf package is `minikv.v1`. Public and internal services use separate
service names so that future authentication and authorization can distinguish
the trust boundaries. Payload message types are shared where their semantics are
identical.

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

Head, batch methods, and administrative methods are not included in v1.

### 5.1 Public Messages

```protobuf
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

message GetRequest {
  bytes key = 1;
}

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

message DeleteRequest {
  bytes key = 1;
}

message DeleteResponse {
  bool deleted = 1;
}
```

`ttl_milliseconds == 0` means no expiration. `request_id` is optional and is
carried through for logs and future deduplication, but it has no idempotency
semantics in this milestone. When supplied, it must be exactly 16 bytes.

### 5.2 Internal Routing Messages

```protobuf
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

Static routing uses epoch zero. The node validates the shard ID against the key
and configured shard count, but epoch ownership enforcement is deferred until
the etcd milestone.

## 6. Framing Rules

### 6.1 Put

- The first frame must be one non-nil header.
- A header may not appear after the first frame.
- Data chunk sequence numbers start at zero and increase by one.
- Empty data chunks are invalid.
- A data chunk may contain at most the configured chunk size.
- Client half-close is the end-of-object marker.
- The sum of chunk bytes must equal `value_size` exactly.
- Zero-byte values contain a header and no data chunks.
- `value_size` may not exceed the Store maximum object size.
- `value_size` must fit in Go `int64` before conversion.
- `ttl_milliseconds` must fit in `time.Duration` after conversion to
  nanoseconds.
- A malformed stream fails before Store publication.

The node consumes the first header, creates a stream-backed `io.Reader`, and
passes that reader to `Store.Put`. The reader retains any unread suffix when the
consumer buffer is smaller than a received chunk. Store.Put performs the final
short/long-source check and controls atomic visibility.

### 6.2 Get

- The first response frame is exactly one GetHeader.
- Data chunks follow with sequence numbers starting at zero.
- Chunk payloads are no larger than the configured chunk size.
- The sum of chunk bytes equals the size in GetHeader.
- A zero-byte value returns only GetHeader.
- The node closes the Store Object on success, cancellation, or send failure.

## 7. Put Data Flow

1. The gateway receives and validates PutHeader.
2. The gateway resolves the key through Router.
3. The gateway obtains a cached backend connection.
4. The gateway opens NodeService.Put with the inbound stream context.
5. The gateway sends NodePutHeader with the resolved shard and epoch.
6. For each client chunk, the gateway validates framing and performs one
   receive followed by one backend send.
7. On client EOF, the gateway calls `CloseAndRecv` on the backend stream.
8. The node completes Store.Put and returns PutResponse.
9. The gateway returns the response with `SendAndClose`.

The gateway does not run ahead of the backend stream. Waiting in backend Send
stops the next client Recv, propagating HTTP/2 flow control back to the client.
The gateway may hold one decoded inbound chunk plus gRPC's bounded transport
buffers, but never the complete object.

## 8. Get Data Flow

1. The gateway validates GetRequest and resolves the key.
2. The gateway calls NodeService.Get using the inbound context.
3. The node calls Store.Get and obtains a pinned snapshot reader.
4. The node sends GetHeader and then reads and sends bounded chunks.
5. The gateway performs one backend receive followed by one client send.
6. Backend or client cancellation stops the pipeline.
7. The node always closes the Store Object.

Normal generated Protobuf handling copies chunk bytes at each proxy hop. This is
accepted for the first all-proxy implementation. Memory remains bounded by chunk
and transport-window sizes. A later benchmark may justify custom codecs or a
direct data path, but those are not part of this milestone.

## 9. Delete Data Flow

Delete is unary and idempotent:

1. The gateway resolves the key.
2. The gateway calls NodeService.Delete.
3. The node calls Store.Delete.
4. The boolean result is returned unchanged.

An absent or expired key returns `deleted == false` with status OK.

## 10. Routing and Connection Management

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
```

StaticRouter owns one backend endpoint and returns it for all 1024 shards with
epoch zero. The shard ID is computed by a shared `internal/sharding` package.
The Store and Router must use the same hash implementation and test vectors;
copying hash logic between packages is forbidden.

BackendPool lazily creates and reuses one `grpc.ClientConn` per address. It does
not Dial per RPC. Concurrent requests share the connection. The pool closes all
connections during gateway shutdown.

## 11. Configuration

The one binary supports two explicit roles:

```text
minikv node --listen 127.0.0.1:9091
minikv gateway --listen 127.0.0.1:9090 --backend 127.0.0.1:9091
```

Relevant defaults:

| Setting | Default |
|---|---:|
| logical shards | 1024 |
| payload chunk | 256 KiB |
| gRPC receive message limit | 1 MiB |
| gRPC send message limit | 1 MiB |
| node listen address | `127.0.0.1:9091` |
| gateway listen address | `127.0.0.1:9090` |
| gateway backend | `127.0.0.1:9091` |
| graceful shutdown timeout | 30 seconds |

Configuration validation rejects a chunk size that is non-positive, larger than
the message limit after framing overhead, or larger than the Store maximum.

Compression is disabled by default. Clients must set request deadlines; the
server does not invent one for an already accepted stream.

## 12. Error Mapping

| Condition | gRPC status |
|---|---|
| invalid key, TTL, declared size, request ID, sequence, or frame order | `INVALID_ARGUMENT` |
| object exceeds configured maximum | `INVALID_ARGUMENT` |
| staging or live capacity unavailable | `RESOURCE_EXHAUSTED` |
| missing or expired key | `NOT_FOUND` |
| Store or server is closing | `UNAVAILABLE` |
| caller cancellation | `CANCELLED` |
| caller deadline | `DEADLINE_EXCEEDED` |
| backend route or connection unavailable | `UNAVAILABLE` |
| impossible internal invariant or unexpected Store error | `INTERNAL` |

The gateway preserves status codes returned by the node. It may attach a short,
stable message, but must not expose Go error strings or internal addresses.

Put is not automatically retried. A stream may have reached the node even when
the client sees `UNAVAILABLE`; `request_id` is not yet a deduplication key.
Get remains safe for caller-managed retry within its deadline. Repeating Delete
is safe when the caller only needs the key to be absent, but a retry after a lost
response may change `deleted` from true to false. The gateway does not
automatically retry either method in this milestone.

## 13. Cancellation, Flow Control, and Shutdown

- Gateway outbound RPCs use the inbound RPC context.
- Client cancellation therefore cancels gateway proxy work and the node RPC.
- Node handlers pass the RPC context into Store operations.
- Put stream readers check context between received chunks.
- Get closes its Store Object whenever sending stops.
- No handler starts an untracked goroutine per payload chunk.

On shutdown, each process stops admitting new requests, invokes graceful gRPC
shutdown, waits up to the configured timeout, and then forces transport stop.
The node closes CoreStore only after gRPC handlers have stopped. The gateway
closes backend connections only after its public handlers have stopped.

## 14. Health and Security

Both roles register the standard gRPC health service.

- Node is serving only after the Store and listener are ready.
- Gateway is serving only after configuration is valid and a backend connection
  can be created. It does not require every health probe to contact the backend.
- Shutdown sets health to not-serving before graceful stop.

The first milestone uses plaintext gRPC and defaults to loopback addresses. It
is a development and trusted-network transport, not a production security
boundary. TLS, mTLS, authentication, and authorization require a separate
approved design before internet-facing deployment.

## 15. Package Ownership

```text
api/minikv/v1/cache.proto           source protocol
gen/go/minikv/v1/                   generated Go code
internal/sharding/                  shared protocol hash and shard mapping
internal/routing/                   Router and StaticRouter
internal/transport/grpccommon/      limits, validation, status mapping
internal/transport/grpcnode/        NodeService implementation
internal/transport/grpcgateway/     CacheService proxy implementation
internal/server/                    role assembly and lifecycle
internal/config/                    CLI and process configuration
cmd/minikv/                          role selection and signal handling
```

Generated files are committed so consumers and CI do not need `protoc` merely
to build. A repeatable generation command and version-pinned tool declarations
must also be committed.

## 16. Testing Strategy

### 16.1 Protocol and Reader Tests

- header-first and header-only zero-byte Put;
- duplicate or missing header;
- chunk before header;
- empty, oversized, skipped, and duplicate chunks;
- short and long declared sources;
- request ID validation;
- stream reader suffix preservation and EOF behavior.

### 16.2 Node Tests

- Put/Get/Delete against a real CoreStore;
- partial Put remains invisible;
- TTL and expiration metadata round trip;
- cancellation releases staging bytes;
- Get cancellation releases the Arena reader;
- every stable Store error maps to the expected gRPC code.

### 16.3 Gateway Tests

- Router is called once from the header or unary request;
- route context is injected correctly;
- Put and Get frames remain ordered;
- backend status codes pass through;
- inbound cancellation reaches the backend;
- one slow backend stream applies backpressure instead of unbounded buffering;
- backend connections are reused and closed.

### 16.4 End-to-End Tests

Use `bufconn` for Client -> Gateway -> Node -> CoreStore tests without real
ports. Cover 0 B, 1 B, cross-chunk, 1 MiB, binary keys, TTL, Delete, short/long
Put, cancellation, not-found, and capacity exhaustion.

An opt-in test transfers one 128 MiB object through both gRPC hops. Benchmarks
cover 1 KiB, 1 MiB, and 32 MiB Put/Get operations and report throughput,
allocations, and bytes allocated.

Race tests cover concurrent streams, connection reuse, and graceful shutdown.

## 17. Acceptance Criteria

The milestone is complete when:

- a client can Put, Get, and Delete through the gateway without importing Store;
- no normal Protobuf message exceeds the configured 1 MiB limit;
- Gateway and Node never assemble a complete multi-MiB object for transport;
- short, long, canceled, and malformed Put streams never publish an object;
- Get streams from an Arena reader and releases it on every exit path;
- deadlines and cancellations propagate across both gRPC hops;
- Store errors have stable documented gRPC status mappings;
- shard IDs are identical in Store and Router vector tests;
- backend connections are reused rather than dialed per request;
- graceful shutdown leaves no active handlers or maintenance goroutines;
- full tests, race tests, protocol generation checks, and vet pass;
- the opt-in 128 MiB end-to-end test passes;
- benchmarks establish the baseline cost of the all-proxy architecture.

## 18. Deferred Follow-Up

The next distributed milestone replaces StaticRouter with an etcd-backed router,
enforces shard ownership and epoch fencing in NodeService, and adds one
asynchronous replica per shard. Those additions must preserve this public
CacheService and its streaming, cancellation, and bounded-memory behavior.
