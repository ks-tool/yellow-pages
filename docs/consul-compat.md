<!--
 Copyright © 2026 Alexey Shulutkov <github@shulutkov.ru>
 Licensed under the Apache License, Version 2.0.
-->

# Consul compatibility matrix

yellow-pages is a drop-in for Consul **service discovery** (HTTP + DNS) over a
clean gRPC core. Status as of v1.

## HTTP (`:8500`)

| Method | Endpoint                                                                            | Priority  | Status                                                             |
|--------|-------------------------------------------------------------------------------------|-----------|--------------------------------------------------------------------|
| GET    | `/v1/agent/self`                                                                    | 🔴 must   | ✅ Datacenter/NodeName/Version                                      |
| PUT    | `/v1/agent/service/register`                                                        | 🔴 must   | ✅ lenient (TTL + active HTTP/TCP/UDP/exec checks; unknown ignored) |
| PUT    | `/v1/agent/service/deregister/:id`                                                  | 🔴 must   | ✅                                                                  |
| GET    | `/v1/agent/services`                                                                | 🟠 should | ✅                                                                  |
| PUT    | `/v1/agent/service/maintenance/:id`                                                 | 🟠 should | ✅ (best-effort on agent)                                           |
| PUT    | `/v1/agent/check/{pass,warn,fail,update}/:id`                                       | 🟠 should | ✅ TTL bridge                                                       |
| PUT    | `/v1/agent/check/{register,deregister}`                                             | 🟠 should | ✅ accepted                                                         |
| GET    | `/v1/agent/checks`                                                                  | 🟡 could  | ✅                                                                  |
| GET    | `/v1/agent/health/service/name/:name`                                               | 🟠 should | ✅ (+`format=text`)                                                 |
| GET    | `/v1/catalog/services`                                                              | 🔴 must   | ✅                                                                  |
| GET    | `/v1/catalog/service/:service`                                                      | 🔴 must   | ✅ flat schema                                                      |
| GET    | `/v1/catalog/nodes`                                                                 | 🟠 should | ✅                                                                  |
| GET    | `/v1/catalog/datacenters`                                                           | 🟠 should | ✅ single-DC                                                        |
| PUT    | `/v1/catalog/register` · `/deregister`                                              | 🟠 should | ✅ backfill                                                         |
| GET    | `/v1/health/service/:service`                                                       | 🔴 must   | ✅ nested schema + `?passing`                                       |
| GET    | `/v1/health/checks/:service`                                                        | 🟠 should | ✅                                                                  |
| GET    | `/v1/health/state/:state`                                                           | 🟠 should | ✅                                                                  |
| GET    | `/v1/status/leader` · `/peers`                                                      | 🔴 must   | ✅ shim (`addr:8300`)                                               |
| —      | blocking `?index`/`?wait`, `?stale`/`?consistent`, `?tag`, `?filter`(subset), token | 🔴 must   | ✅                                                                  |
| —      | `/v1/kv` · `/connect` · `/session` · `/txn` · `/operator` · `/query`                | —         | ❌ out of scope                                                     |

## DNS (`:8600`, UDP+TCP)

| Form                                                               | Status           |
|--------------------------------------------------------------------|------------------|
| `<service>.service[.<dc>.dc].consul` (canonical + legacy short dc) | ✅ A/AAAA/SRV/TXT |
| `<tag>.<service>.service.consul`                                   | ✅ raw-tag filter |
| `_<service>._<proto>.service.consul` (RFC2782, proto = tag)        | ✅ SRV            |
| `<node>.node.consul`                                               | ✅ A/AAAA/TXT     |
| SOA / NS                                                           | ✅                |
| `*.connect/virtual/ingress/ns/ap/peer/query.consul`                | ❌ NXDOMAIN       |

SRV targets always resolve to the **service** address (`<node>.node.<dc>` when it
inherits the node address, else a synthetic `<hexip>.addr.<dc>`). NXDOMAIN for an
unknown name; NOERROR + empty + SOA for an existing service with no healthy
instance.

The served zone is configurable: `dns.domain` **replaces** `.consul` (e.g.
`mycorp.` → `web.service.mycorp`), and `dns.alt_domain` adds a **second** zone
answered alongside it (Consul's `alt_domain`) — useful during a cutover to keep
`.consul` working while rolling out your own. Records (SOA/NS/SRV targets) are
rendered under whichever zone the query arrived on.

## AP semantics & limits

- **Bounded staleness, not linearizable.** Reads are merged from seeds with
  last-writer-wins by data `generation`, then `last_seen`. `?consistent` forces a
  fresh fan-out (best-effort freshness) but does **not** promise linearizability.
- **TTL is a tombstone.** A missed deregister leaves a ghost until its TTL; lower
  TTL for faster removal, at the cost of more heartbeats.
- **Active checks gate the lease.** A service registered with an `HTTP`/`TCP`/`UDP`
  or script (`Args`) check is probed by the agent on its `Interval`: while it
  passes the agent refreshes the lease, when it fails the agent stops, so the
  registry lets the instance go critical and then drops it (TTL + grace). Set the
  check `TTL` field instead for a passive heartbeat. Script checks run a local
  binary, so they require `enable_script_checks` and an absolute path (no shell).
- **NTP is a precondition** for the `last_seen` tiebreak (generation dominates);
  `yp_agent_seed_clock_skew_seconds` is exposed and alertable.
- **Single-DC in v1.** `datacenter` is always present (DNS/`?dc`); cross-DC
  fan-out is deferred (M17).
- See `docs/slo.md` for SLOs and `docs/migration.md` for the cutover runbook.
