<!--
 Copyright © 2026 Alexey Shulutkov <github@shulutkov.ru>
 Licensed under the Apache License, Version 2.0.
-->

# Architecture

A user-facing overview of how yellow-pages works and why. The full design lives in
`new/ARCHITECTURE.md`; this is the short version for operators and integrators.

## The model: AP, not CP

Consul (and etcd, ZooKeeper) are **CP**: a Raft quorum gives linearizable reads
but a write needs a leader and a majority — a partition can stall the whole
control plane. yellow-pages is **AP**: it has **no Raft, no leader, no quorum**.
Seeds are independent registries; writes go to every seed and reads merge across
them. The cost is *bounded staleness* instead of linearizability — appropriate for
service discovery, where "a few seconds stale" beats "unavailable during a
partition".

```
                writes fan out to EVERY seed (k-of-N)
                reads fan out to ALL seeds, merge by LWW
   ┌───────┐                                   ┌──────────┐
   │ app   │── register/lookup ──► ┌───────┐──►│  seed 1  │  (independent
   └───────┘   (Consul HTTP/DNS    │ agent │   └──────────┘   registries —
                or native gRPC)    │       │──►┌──────────┐   they do NOT
                                   └───────┘   │  seed 2  │   talk to each
                                       ▲       └──────────┘   other on the
                                       └ local cache + merge   hot path)
```

## Roles

Every node is the same binary; `role:` decides what it runs.

- **Seed** — the in-memory **registry**. Serves the native `AgentService` over its
  store, GCs expired leases, and (when enabled) the Consul HTTP/DNS surfaces. Seeds
  hold no app services of their own.
- **Agent** — the **local-agent-proxy**. Apps register/lookup against their local
  agent; the agent fans writes out to every seed (k-of-N quorum), heartbeats, and
  serves reads from a bounded-staleness cache backed by a cross-seed merge. Holds
  no inbound registry.

## Writes: fan-out + lease

A registration is sent to **every** seed; it succeeds when `write_quorum` (k-of-N)
acknowledge. Each service carries a **lease** (`ttl`): the agent renews it every
`heartbeat_interval`. A seed reaps a lease that is `ttl` + grace past its last
renew. So a crashed agent's services disappear automatically — TTL is the
tombstone (no explicit deregister needed, though shutdown does deregister).

Each registration has a **`generation`** (a client data-version, bumped only when
the endpoint/tags/meta change — not on renew) and a server-stamped **`last_seen`**.

## Reads: fan-out + LWW merge

A lookup queries **all** seeds and merges the results **last-writer-wins**: for
each `(node, service)` key, keep the instance with the highest `generation`, then
the latest `last_seen`. This reconciles seeds that briefly disagree (e.g. a write
reached seed 1 before seed 2). The merge is the same code on every surface
(`internal/health.MergeLWW`, `model.WinsLWW`).

Health is derived from the lease: alive → passing, expired-in-grace → critical
(visible), past-grace → gone. `?passing` filters to passing (warning counts as
passing); maintenance and active-check failures show critical.

## Bounded staleness & the cache

Agents serve reads from a local cache refreshed every `agent.cache_max_age`. So a
read is at most that stale. Consul-style consistency knobs map onto this:

- default / `?stale` → the cache (fast, bounded-stale), with `X-Consul-LastContact`.
- `?consistent` → a fresh fan-out (best-effort freshness) — **not** linearizable.

## Watch & the monotonic index

`Watch` streams entry-level changes (`PUT`/`DELETE`) gated by a monotonic
**index**, so clients (and Consul **blocking queries** `?index`/`?wait`) get push
updates without polling. A seed's index comes from its store; an **agent
synthesises one monotonic index over N seeds** so a single client sees a coherent
stream even though the seeds are independent. The stream begins with a snapshot of
current entries, terminated by `snapshot_done`, then live deltas.

## Surfaces: a gRPC core, Consul layered on top

The contract is the native gRPC `discovery.v1` API (see
[grpc-api.md](grpc-api.md)). The Consul **HTTP** and **DNS** surfaces are
projections of the same `Resolve`/`Register` operations — they translate Consul's
wire shapes (flat catalog vs nested health, PascalCase, SRV semantics) but add no
new state. The pure domain model (`internal/model`) never imports protobuf; the
one place proto and model mix is `internal/protoconv`.

## Process model

A node is a set of `app.Component{Name, Start, Stop}` run under one errgroup:
first failure cancels all, shutdown is bounded and ordered (an agent drains as
readiness-off → drain-window → stop-accept → deregister → close). Time flows
through a `clock.Clock` seam so every time-dependent path is deterministically
testable with a fake clock.

## What's deferred (feature-flagged)

The base model has **independent seeds, single DC**. Two optional, off-by-default
extensions layer on without changing the core:

- **Cross-DC federation** — [federation.md](federation.md).
- **Seed anti-entropy** (seeds self-heal/converge) — [membership.md](membership.md).

## See also

- [grpc-api.md](grpc-api.md) — the native API contract.
- [consul-compat.md](consul-compat.md) — Consul HTTP/DNS parity + AP limits.
- [operations.md](operations.md) — running it.
- [slo.md](slo.md) — the SLIs that make AP observable.
