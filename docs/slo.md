<!--
 Copyright © 2026 Alexey Shulutkov <github@shulutkov.ru>
 Licensed under the Apache License, Version 2.0.
-->

# SLIs, SLOs and alerts

yellow-pages is AP: reads are bounded-stale, not linearizable. These SLIs make
the staleness and the main failure mode (per-seed divergence) measurable, and
the SLOs make them enforceable. All series are on `/metrics` (default :9901).

## SLIs → SLOs

| SLI | Metric | SLO |
|---|---|---|
| register-to-visible | `yp_propagation_register_to_visible_seconds` (histogram) | p99 < 3 × `heartbeat_interval` |
| deregister-to-removed | `yp_propagation_deregister_to_removed_seconds` | p99 < `ttl` + 3 × `heartbeat_interval` |
| read-cache staleness | `yp_agent_cache_age_seconds` (gauge) | < `agent.cache_max_age` |
| seed clock skew | `yp_agent_seed_clock_skew_seconds` | p99 < 1s (NTP precondition) |
| per-seed divergence | `yp_agent_seed_divergence` (gauge) | steady-state 0 |
| per-seed fan-out success | `yp_agent_seed_fanout_total{result}` | success ratio > 0.99 |
| read-path availability | `yp_rpc_requests_total{method="…/Lookup",code}` | error ratio < 0.1% |

## Supporting series

- `yp_registry_services` — seed registry size.
- `yp_registry_ttl_evictions_total` — leases reaped after TTL+grace.
- `yp_consul_blocking_query_waiters` — in-flight Consul blocking queries.
- `yp_consul_surface_requests_total{surface}` — Consul HTTP / DNS request rate.
- `yp_rpc_{requests_total,latency_seconds}{side,method,code}` — gRPC rate/latency/errors.

**Cardinality:** hot-path series carry only low-cardinality labels
(`result`, `surface`, `code`, `side`, `method`) — never a service name.

## Alerts (Prometheus rules — operator-owned)

- **SeedDivergence** — `yp_agent_seed_divergence > 0` for 5m: seeds disagree;
  inspect with the read-only registry dump (`GET /v1/internal/registry-dump?service=<name>`),
  which returns each seed's raw instance set.
- **ClockSkewHigh** — `yp_agent_seed_clock_skew_seconds > 1`: NTP drift threatens
  the LWW last_seen tiebreak (generation still dominates).
- **AgentNotReady** — gRPC health NOT_SERVING: fewer than `agent.ready_min_seeds`
  seeds reachable.
- **PropagationSLOBurn** — register-to-visible p99 over the SLO above.
