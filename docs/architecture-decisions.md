# MiniKV Architecture Decisions

Date: 2026-07-20

This document records the architecture decisions approved before implementation.
It is a living decision record; the implementation specification will refine these
constraints without changing their semantics unless a new decision supersedes them.

## Product Direction

- Build a deployable distributed cache prototype rather than an algorithm-only demo.
- Implement a general-purpose distributed byte cache first.
- Add an LLM prefix and tensor KV-cache adapter after the general data plane is stable.
- Use Go for the service implementation and Python for benchmarks and AI integrations.

## Deployment Architecture

Use one Go module with independently deployable process roles:

- `minikv node`: storage, TTL, eviction, WAL, snapshots, and replication.
- `minikv gateway`: stateless routing, retries, limits, and authentication.
- `minikv controller`: etcd-backed placement, failover, and migration scheduling.
- `minikvctl`: cluster initialization and administrative operations.

The initial Python client connects through gateways. A later Go smart client can
route directly to storage nodes using the same shard map and protocol.

## Consistency And Durability

- Optimize for cache availability and request latency.
- Store one primary and one asynchronous replica for each logical shard.
- Allow loss of the most recent writes during failover when replication is behind.
- Support asynchronous WAL plus periodic snapshots for warm restart.
- Keep a configurable memory-only mode.
- Treat persistence as cache recovery, not database-grade durability.

## Protocols And Observability

- Use gRPC and Protobuf for client, internal, and replication APIs.
- Use streaming RPCs for large cache objects.
- Expose HTTP endpoints for health, readiness, and Prometheus metrics.
- Keep RESP3 outside the first implementation; it can be added as an adapter.

## Initial Scale Boundary

- Storage nodes: 3.
- Logical shards: 1024.
- Replication factor: 2, represented as one primary and one replica.
- Maximum cache object size: 128 MiB, configurable.
- Effective cache capacity per storage node: 64 GiB.
- Calculate capacity, eviction, quotas, placement, and migration by bytes rather
  than key counts.

The raw three-node capacity is 192 GiB. With two copies per object, the maximum
logical capacity is 96 GiB before operational headroom is reserved.

## LLM KV-Cache Compatibility

- Keep the general key-value API suitable for small objects.
- Model AI cache data as token-range chunks instead of one value per request.
- Support 16-token logical blocks and configurable 16-256-token transfer chunks.
- Include model identity, model version, tensor dtype, parallel layout, prefix
  hash, and token range in the AI cache identity.
- Make the storage and transport path capable of handling multi-MiB objects
  without requiring one contiguous in-memory copy per RPC.

## Control Plane

etcd owns strongly consistent cluster metadata but is not part of normal
`Get`, `Put`, or `Delete` request execution.

The key-space layout is:

```text
/minikv/nodes/<node-id>       node lease, addresses, capacity, and usage
/minikv/shards/<shard-id>     primary, replica, epoch, and state
/minikv/controller/leader     controller leader election
/minikv/cluster/config        shard count, replication factor, and hash version
```

Keys map to shards with a protocol-versioned XXH3-64 hash and a fixed seed:

```text
shard_id = XXH3_64(key) % 1024
```

The controller persists placement in etcd. Gateways and future smart clients
watch the shard map and cache it locally.

## Routing And Fencing

Each routed request carries its shard ID and assignment epoch. A storage node
returns `WrongOwner` when the epoch or role does not match its active assignment.
The gateway then refreshes its route and may perform one safe retry.

Node leases use these initial timing values:

- Lease duration: 10 seconds.
- Renewal interval: 3 seconds.
- Failover objective: restore shard writes within 15 seconds of node failure.

A node that cannot renew its lease stops accepting writes when the current lease
expires. The controller waits for the old lease to expire before promoting the
replica, then increments the shard epoch. The incremented epoch is the fencing
token that prevents a recovered former primary from accepting stale writes.

During an etcd outage, nodes may continue serving their current assignments only
until their leases expire. After expiry they reject writes; stale reads may remain
available when explicitly enabled.
