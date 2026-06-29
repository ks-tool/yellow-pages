<!--
 Copyright © 2026 Alexey Shulutkov <github@shulutkov.ru>
 Licensed under the Apache License, Version 2.0.
-->

# Native gRPC API (`discovery.v1`)

The wire contract under the Consul-compatible surfaces. Use it directly for the
lowest-overhead path; for most apps the [Go SDK and `yp://` resolver](clients.md)
wrap it.

- Proto: `proto/discovery/v1/discovery.proto`, package `discovery.v1`.
- Go: `discoveryv1 "github.com/ks-tool/yellow-pages/proto/discovery/v1"`.
- Served on `listeners.grpc` (default `:9900`, always on), plus gRPC **health**
  (`grpc.health.v1`) and **reflection**.
- **Errors are gRPC status codes, never in the body** (`NotFound`,
  `InvalidArgument`, `ResourceExhausted`, `Unavailable`, `PermissionDenied`, …).
- **Append-only contract**: fields/RPCs are only added, never renumbered or
  removed; `make buf-breaking` gates it. Old clients keep working.

A **seed** serves this over its registry store; an **agent** serves the same
service as the local-agent-proxy (fan-out + merge). Talk to either.

## `AgentService`

| RPC                                                       | Purpose                                                           |
|-----------------------------------------------------------|-------------------------------------------------------------------|
| `Register(RegisterRequest) → RegisterResponse`            | Create/refresh a node and its services.                           |
| `Renew(RenewRequest) → RenewResponse`                     | Refresh leases (node-scoped, optionally narrowed to service ids). |
| `Deregister(DeregisterRequest) → DeregisterResponse`      | Remove a node and all its services.                               |
| `DeregisterService(DeregisterServiceRequest) → …Response` | Remove one service.                                               |
| `Lookup(LookupRequest) → LookupResponse`                  | Read instances for a `Query` (merged across seeds on an agent).   |
| `Watch(WatchRequest) → stream WatchResponse`              | Stream changes from an `index` (push updates / blocking queries). |

### Register

```protobuf
message Registration {
  Node node = 1;                 // id (stable identity), name, address, datacenter, meta
  repeated Service services = 2; // id (=name if unset), address, port, tags, meta, weights, ttl_seconds
  uint64 generation = 3;         // data version; bump only when endpoint/tags/meta change
}
```

`generation` is the client-supplied data version — identical on all seeds for one
registration, and the **primary LWW key** (then `last_seen`). `ttl_seconds` is the
per-service lease window (server-clamped); renew within it or the instance expires.

### Renew

`RenewRequest{ node_id, service_ids? }` refreshes leases. Empty `service_ids`
renews **all** of the node's services (the agent's heartbeat path). Idempotent.

### Lookup

```protobuf
message Query {string name; string datacenter; repeated string tags; bool only_healthy;}
message LookupResponse {repeated ServiceEntry entries; uint64 index;}
```

`tags` are matched against the raw tag strings (AND). An empty `name` lists all
services (catalog). On an agent the entries are the **LWW merge** across seeds;
`index` is the agent-synthesised monotonic index for use with `Watch`.

A `ServiceEntry` is the merged result: `node`, `service`, derived
`health` (`PASSING`/`WARNING`/`CRITICAL`), `maintenance`, `generation`,
`last_seen_unix_nano`.

### Watch (streaming)

```protobuf
message WatchRequest  {Query query; uint64 index;}       // index 0 = current state now
message WatchResponse {ChangeEvent event; uint64 index; bool snapshot_done;}
message ChangeEvent   {ChangeType type; ServiceEntry entry;}  // PUT | DELETE
```

The stream starts with a **snapshot** (a burst of `PUT`s for existing entries),
terminated by `snapshot_done: true`, then emits live `PUT`/`DELETE` deltas. Each
response carries the new monotonic `index`; reconnect with the last `index` to
resume. This is what backs the Consul HTTP **blocking query** (`?index`/`?wait`).

## `BootstrapService`

```protobuf
service BootstrapService {rpc GetConfig(GetConfigRequest) returns (GetConfigResponse);}
message GetConfigRequest  {string role;}        // "agent" | "seed"
message GetConfigResponse {bytes config;}        // sanitized YAML
```

Seed-only, off by default. Returns a sanitized config for a joining node; the
short-lived token travels in the `bootstrap-token` metadata header. See
[bootstrap.md](bootstrap.md).

## Quick client (Go)

```go
conn, _ := grpc.NewClient("127.0.0.1:9900", grpc.WithTransportCredentials(insecure.NewCredentials()))
c := discoveryv1.NewAgentServiceClient(conn)

_, _ = c.Register(ctx, &discoveryv1.RegisterRequest{Registration: &discoveryv1.Registration{
Node:     &discoveryv1.Node{Id: "agent-1", Datacenter: "dc1"},
Services: []*discoveryv1.Service{{Name: "web", Address: "10.0.0.5", Port: 8080, TtlSeconds: 30}},
Generation: 1,
}})

resp, _ := c.Lookup(ctx, &discoveryv1.LookupRequest{Query: &discoveryv1.Query{Name: "web", OnlyHealthy: true}})
```

Prefer [`client/sdk`](clients.md) (typed Register/Renew/Deregister/Discover/Watch,
no internal imports) and the `yp://` gRPC resolver for real apps.
