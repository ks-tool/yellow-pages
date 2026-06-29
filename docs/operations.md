<!--
 Copyright © 2026 Alexey Shulutkov <github@shulutkov.ru>
 Licensed under the Apache License, Version 2.0.
-->

# Operations runbook

Running yellow-pages in practice. See [configuration.md](configuration.md) for
keys, [slo.md](slo.md) for the metrics, and [security.md](../SECURITY.md) for
hardening.

## Topology & sizing

- **Seeds** hold the registry. Run **2–3** for redundancy. There is **no quorum**
  (AP), so any number works and an even count is fine — agents write to all of
  them and reads merge. More seeds = more write fan-out and more read merge work,
  not more consistency.
- **Agents** run next to the workloads (one per host/pod). Apps talk to their
  **local** agent over loopback; the agent fans out to the seeds.
- Seeds need stable, reachable gRPC addresses (put them in every agent's
  `cluster.seeds`, or behind `cluster.discovery`).

A single seed is valid for dev/small setups; you just lose redundancy.

## DNS delegation (`.consul` → yp)

yp DNS listens on `:8600` by default (not `:53`). Point a resolver at it:

**dnsmasq** (simplest):

```
# /etc/dnsmasq.d/consul.conf
server=/consul/127.0.0.1#8600
```

**CoreDNS** (forward the zone):

```
consul:53 { forward . 127.0.0.1:8600 }
.:53      { forward . /etc/resolv.conf }
```

**systemd-resolved** does not forward a single domain to a non-53 port; either run
yp DNS on `:53` (bind a dedicated address) or chain through a local dnsmasq/CoreDNS
as above. To serve your own zone instead of `.consul`, set `dns.domain` (and
`dns.alt_domain` to serve both during a cutover) — see
[consul-compat.md](consul-compat.md).

## Node identity & restarts

`node_name` is the stable identity. If empty:

- with `data_dir` → a UUID is generated **and persisted**, so identity survives
  restarts;
- without `data_dir` → an **ephemeral** UUID. On restart the old node id's
  registrations linger as ghosts until their TTL.

Set a `node_name` (or a `data_dir`) on anything long-lived.

## Graceful shutdown

On `SIGTERM`/`SIGINT` an agent drains in order: **readiness → NOT_SERVING**, wait
`agent.drain_window` (load balancers stop sending), **stop accepting**,
**deregister** from seeds, **close**. The whole sequence is bounded by
`shutdown_timeout`. Seeds shut down by stopping the registry server + GC loop.

## Monitoring

Scrape `/metrics` (enable `listeners.metrics`). The SLIs and their SLOs/alerts are
in [slo.md](slo.md); a metric reference and a ready Grafana dashboard are in
[dashboard.md](dashboard.md). The signals to alert on:

| Signal              | Metric                                       | Watch for                                 |
|---------------------|----------------------------------------------|-------------------------------------------|
| per-seed divergence | `yp_agent_seed_divergence`                   | `> 0` sustained — seeds disagree          |
| clock skew          | `yp_agent_seed_clock_skew_seconds`           | `> 1s` — threatens the LWW tiebreak       |
| readiness           | gRPC health `NOT_SERVING`                    | agent can't reach `ready_min_seeds` seeds |
| propagation         | `yp_propagation_register_to_visible_seconds` | p99 over your SLO                         |
| convergence (M18)   | `yp_seed_convergence_lag`                    | non-zero when anti-entropy is on          |

## Troubleshooting

**Seeds disagree / a service shows on one agent but not another.** Inspect the raw
per-seed view (read-only admin endpoint on the Consul HTTP surface):

```bash
curl -s localhost:8500/v1/internal/registry-dump?service=web
```

It returns each seed's unmerged instance set, so you can see which seed is missing
or stale. Transient divergence self-heals on the next write/refresh; persistent
divergence means a seed missed writes — check connectivity, or enable seed
anti-entropy ([membership.md](membership.md)).

**Agent never becomes ready.** It can't reach `ready_min_seeds` seeds. Check
`cluster.seeds` addresses, network/TLS to the seeds, and the seed logs. The agent
serves NOT_SERVING (so LBs skip it) until enough seeds answer.

**Ghost registrations after a restart.** The node had no stable identity — set
`node_name` or `data_dir`. Existing ghosts clear after one `ttl`.

**Writes rejected with `ResourceExhausted`.** The seed hit `max_services`, or a
rate limit (`consul_rate_limit` / `dns.rate_limit` / `bootstrap.rate_limit`)
tripped. Raise the cap or the limit if legitimate.

**Clock skew warnings.** Generation dominates the LWW merge, but `last_seen` is
the tiebreak — keep nodes on NTP.

## Upgrades

The proto is **append-only**, so mixed versions interoperate — roll nodes one at a
time. Recommended order: **seeds first** (one at a time; the others keep serving),
then **agents** (each drains gracefully, so in-flight reads are unaffected). The
registry is in-memory and rebuilt continuously from agent heartbeats, so a seed
restart re-fills within a `ttl` as agents renew.

## Backup / restore

There is little durable state to back up: the registry is **in-memory** and
self-rebuilding from agents. The only persisted file is the node id (and watch
base) under `data_dir` — back that up if you want stable seed identities across
host replacement; otherwise nodes regenerate identity on restart.
