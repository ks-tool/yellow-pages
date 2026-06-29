<!--
 Copyright © 2026 Alexey Shulutkov <github@shulutkov.ru>
 Licensed under the Apache License, Version 2.0.
-->

# Metrics & Grafana dashboard

yellow-pages exposes Prometheus metrics on `/metrics` (enable
`listeners.metrics`; RPC telemetry is recorded regardless, the listener only
exposes it). A ready-made dashboard lives at
[`deploy/grafana/yellow-pages.json`](../deploy/grafana/yellow-pages.json). For the
SLOs and alert thresholds behind these series, see [slo.md](slo.md).

## Getting the dashboard

**Test stand (auto-provisioned).** The [docker compose stand](../deploy/stand/README.md)
ships Grafana with the datasource and dashboard already wired:

```bash
docker compose --profile monitoring up -d --build
open http://localhost:3000        # anonymous, no login → "yellow-pages" dashboard
```

**Existing Grafana.** Dashboards → Import → upload
`deploy/grafana/yellow-pages.json`, then pick your Prometheus datasource (the
dashboard uses a `datasource` template variable, so it binds to any Prometheus).

The dashboard has `datasource`, `job` and `instance` variables (multi-select), so
one board covers every node; filter to a seed or agent as needed.

## Metric reference

All metrics are namespaced `yp_`. Baseline `go_*` / `process_*` collectors are also
exported.

| Metric | Type | Labels | Role | Meaning |
|---|---|---|---|---|
| `yp_rpc_requests_total` | counter | `side`, `method`, `code` | both | RPCs by side (`server`/`client`), gRPC method and status code. |
| `yp_rpc_latency_seconds` | histogram | `side`, `method`, `code` | both | RPC latency (default buckets). |
| `yp_propagation_register_to_visible_seconds` | histogram | — | agent | Time from a register to it being visible via the merged read (SLI). |
| `yp_propagation_deregister_to_removed_seconds` | histogram | — | agent | Time from a deregister to it disappearing (SLI). |
| `yp_agent_cache_age_seconds` | gauge | — | agent | Age of the local read cache. |
| `yp_agent_seed_clock_skew_seconds` | gauge | — | agent | Estimated clock skew vs seeds (threatens the LWW tiebreak). |
| `yp_agent_seed_divergence` | gauge | — | agent | Spread in instance count returned by seeds for the last read. |
| `yp_agent_seed_fanout_total` | counter | `result` | agent | Per-seed fan-out attempts by `result` (`success`/`failure`). |
| `yp_registry_services` | gauge | — | seed | Service instances held in the seed registry. |
| `yp_registry_ttl_evictions_total` | counter | — | seed | Instances reaped by GC after TTL+grace. |
| `yp_consul_blocking_query_waiters` | gauge | — | both | In-flight Consul blocking queries. |
| `yp_consul_surface_requests_total` | counter | `surface` | both | Requests to the Consul-compatible surfaces. |
| `yp_seed_convergence_lag` | gauge | — | seed | Entries applied by the last anti-entropy pass (0 once converged; M18). |

## Panels

| Panel | Query basis |
|---|---|
| Registry size | `sum(yp_registry_services)` |
| RPC error ratio (5m) | non-`OK` `yp_rpc_requests_total` rate ÷ total rate |
| Max per-seed divergence | `max(yp_agent_seed_divergence)` |
| Max seed clock skew | `max(yp_agent_seed_clock_skew_seconds)` |
| RPC rate / p99 latency by method | `yp_rpc_requests_total`, `histogram_quantile(0.99, yp_rpc_latency_seconds_bucket)` |
| Propagation p99 (SLI) | p99 of the two `yp_propagation_*_bucket` histograms |
| Divergence & convergence lag | `yp_agent_seed_divergence`, `yp_seed_convergence_lag` |
| Seed clock skew / cache age | `yp_agent_seed_clock_skew_seconds`, `yp_agent_cache_age_seconds` |
| Seed fan-out result rate | `rate(yp_agent_seed_fanout_total)` by `result` |
| Consul surface requests | `rate(yp_consul_surface_requests_total)` by `surface` |
| Blocking-query waiters | `yp_consul_blocking_query_waiters` |
| Registry TTL evictions | `rate(yp_registry_ttl_evictions_total)` |

## Scrape config

The stand's [`prometheus.yml`](../deploy/stand/prometheus.yml) is a working
example. Prometheus adds the `job` and `instance` labels the dashboard variables
filter on; add `role`/`cluster` labels if you want to slice seeds vs agents.
