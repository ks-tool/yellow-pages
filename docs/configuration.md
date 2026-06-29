<!--
 Copyright © 2026 Alexey Shulutkov <github@shulutkov.ru>
 Licensed under the Apache License, Version 2.0.
-->

# Configuration reference

One snake_case schema, parsed by a single library (`gopkg.in/yaml.v3`). YAML is a
JSON superset, so `.yaml`, `.yml` and `.json` all work. **Unknown keys are
rejected** — a typo fails the load instead of booting with silent zeros. Load
order is: parse → apply defaults → validate (all errors reported at once).

```bash
yp --config /etc/yp/config.yaml [--role agent|seed]   # --role overrides role:
```

Durations are Go duration strings (`"30s"`, `"5m"`), never bare integers.

## Top level

| Key | Type | Default | Notes |
|---|---|---|---|
| `role` | `agent` \| `seed` | `agent` | Selects node behaviour. `--role` overrides it. |
| `node_name` | string | — | Stable node identity. Empty → a UUID, persisted under `data_dir` if set, else ephemeral (a restart ghosts old registrations until TTL). |
| `datacenter` | string | `dc1` | Present on every Consul surface (`?dc`, `.dc.consul`). |
| `data_dir` | path | — | Persists the node id (and watch base). Recommended for seeds. |
| `config_dir` | path | — | Directory of Consul service-definition `*.json` files, loaded at start and re-read on `SIGHUP`. |
| `enable_script_checks` | bool | `false` | Allow exec/script health checks (run a local binary). Off by default. |
| `ttl` | duration | `30s` | Per-service lease/tombstone window. |
| `heartbeat_interval` | duration | `10s` | How often an agent renews leases. **Must be `< ttl`.** |
| `shutdown_timeout` | duration | `15s` | Bounds graceful shutdown. |
| `max_services` | int | `0` | Seed registry size cap (`0` = unlimited); a new write past it is rejected (`ResourceExhausted`). |
| `consul_rate_limit` | int | `0` | Consul HTTP requests/sec per client (`0` = unlimited) → 429. |

## `cluster`

| Key | Type | Default | Notes |
|---|---|---|---|
| `cluster.name` | string | — | **Required.** |
| `cluster.seeds` | []string | — | Seed addresses `host:port`. An agent needs this **or** `discovery`. |
| `cluster.discovery.name` | path | — | External seed-discovery plugin (required when `discovery` is set). |
| `cluster.discovery.update_interval` | duration | `30s` | Re-run cadence. |
| `cluster.discovery.options` | map | — | Passed to the plugin as YAML in `YP_SEED_PLUGIN_OPTIONS`. |

## `listeners`

Each listener is `{ enabled, address, port }`; `address` defaults to `127.0.0.1`.
Binding `0.0.0.0` is explicit (firewall it).

| Listener | Default port | Default state | Surface |
|---|---|---|---|
| `listeners.grpc` | `9900` | **always on** | native `discovery.v1` (AgentService + BootstrapService) |
| `listeners.consul_http` | `8500` | off | Consul-compatible HTTP API |
| `listeners.dns` | `8600` | off | Consul-compatible DNS (UDP+TCP) |
| `listeners.metrics` | `9901` | off | Prometheus `/metrics` (telemetry is recorded regardless; this only exposes it) |

## `tls`  (transport security — off by default)

| Key | Type | Default | Notes |
|---|---|---|---|
| `tls.enabled` | bool | `false` | Turn on TLS. Then `cert_file` + `key_file` are required. |
| `tls.cert_file` / `tls.key_file` | path | — | Node certificate + key (PEM). **Hot-reload** on rotation — no restart. |
| `tls.ca_file` | path | — | Trust anchor (PEM). Required when `mutual_tls`. |
| `tls.mutual_tls` | bool | `false` | Require + verify client certs (and present the node cert when dialing). Requires `enabled`. Identity becomes the verified cert subject. |

## `acl`  (write authorization — disabled by default)

| Key | Type | Default | Notes |
|---|---|---|---|
| `acl.mode` | `disabled` \| `allow` \| `enforce` | `disabled` | `enforce` checks write ownership (the caller must own the node a write targets). |
| `acl.default_policy` | `allow` \| `deny` | — | `allow` + `deny` only logs a loud migration warning (silent enforcement loss after a Consul cutover). |
| `acl.tokens_file` | path | — | YAML token→principal map; a caller-identity source under `enforce`. |

`enforce` requires an identity source: `tls.mutual_tls` **or** `acl.tokens_file`.

## `agent`  (agent role tuning)

| Key | Type | Default | Notes |
|---|---|---|---|
| `agent.seed_timeout` | duration | `3s` | Per-seed RPC deadline during fan-out. |
| `agent.write_quorum` | int | `1` | Min seeds a write must reach (k-of-N). |
| `agent.ready_min_seeds` | int | `1` | Min reachable seeds for the agent to report READY. |
| `agent.drain_window` | duration | `5s` | Lame-duck wait after readiness goes NOT_SERVING, before stop-accept + deregister. |
| `agent.cache_max_age` | duration | `5s` | Max read staleness before a refetch; also the cache refresh cadence. |

## `dns`  (Consul DNS tuning)

| Key | Type | Default | Notes |
|---|---|---|---|
| `dns.domain` | string | `consul.` | Served zone (replaces `.consul`). A trailing dot is enforced. |
| `dns.alt_domain` | string | — | Optional **second** zone served alongside `domain` (Consul `alt_domain`); records render under whichever zone the query arrived on. |
| `dns.service_ttl` / `dns.node_ttl` | duration | `0s` | Record TTLs. |
| `dns.only_passing` | bool | `false` | Drop warning instances too (default keeps warning as passing). |
| `dns.a_record_limit` | int | `0` | Cap A/AAAA records per answer (`0` = no limit). |
| `dns.rate_limit` | int | `0` | Queries/sec per client (`0` = unlimited) → REFUSED (RRL). |
| `dns.recursors` | []string | — | Forward out-of-zone queries (best-effort). |
| `dns.enable_truncate` | bool | **forced `true`** | Sets the TC bit on UDP overflow (amplification safety; not configurable-off). |

## `federation`  (cross-DC, v1.x — off by default)

| Key | Type | Default | Notes |
|---|---|---|---|
| `federation.enabled` | bool | `false` | Route `?dc`/`.dc.consul` to a remote cluster's seeds. |
| `federation.max_hops` | int | `1` | Loop guard. |
| `federation.datacenters` | map[string][]string | — | Remote dc name → its seed addresses. |

## `membership`  (seed anti-entropy, v1.x — off by default)

| Key | Type | Default | Notes |
|---|---|---|---|
| `membership.enabled` | bool | `false` | Snapshot-on-join (gates readiness) + pull-based anti-entropy. |
| `membership.peers` | []string | — | Other seeds' gRPC addresses to sync from. |
| `membership.sync_interval` | duration | `30s` | Anti-entropy pull cadence. |

## `bootstrap`  (seed-only config distribution — off by default)

Serves generated, sanitized configs over the gRPC `BootstrapService`. See
[docs/bootstrap.md](bootstrap.md) and [SECURITY.md](../SECURITY.md).

| Key | Type | Default | Notes |
|---|---|---|---|
| `bootstrap.enabled` | bool | `false` | Register the RPC (seed role only). |
| `bootstrap.signing_key` / `signing_key_file` | string / path | — | HMAC secret signing short-lived tokens (never sent on the wire). One is **required** when enabled. |
| `bootstrap.token_ttl` | duration | `30s` | Lifetime of a minted token. |
| `bootstrap.allow_seed_join` | bool | `false` | Permit `role=seed` configs (high risk). |
| `bootstrap.advertise_seeds` | []string | — | Seed list written into served configs (falls back to `cluster.seeds`). |
| `bootstrap.rate_limit` | int | `10` | Requests/sec per client (must be `>= 0`). |

## Active health checks

Declared per service via the Consul register body (`Check` / `Checks`) or a
service-definition file. The agent runs them and gates the lease — see
[docs/consul-compat.md](consul-compat.md#ap-semantics--limits).

| Field | Meaning |
|---|---|
| `TTL` | Passive heartbeat (bridged to the lease; not actively probed). |
| `HTTP` + `Method` + `Header` + `TLSSkipVerify` | HTTP(S) probe; 2xx passes. |
| `TCP` | `host:port` dial. |
| `UDP` | `host:port` datagram (best-effort; no reply = passing). |
| `Args` | Script: argv, `Args[0]` an **absolute path**, no shell. Needs `enable_script_checks`. |
| `Interval` / `Timeout` | Probe cadence / per-probe deadline (default `10s` / `5s`). |

## Validation (rejected at load)

- `role` ∈ {agent, seed}; `cluster.name` required.
- `ttl`, `heartbeat_interval`, `shutdown_timeout` positive; `heartbeat_interval < ttl`.
- Each **enabled** listener needs an `address` and a port in 1–65535.
- Agent role requires `cluster.seeds` or `cluster.discovery`.
- `tls.enabled` ⇒ `cert_file` + `key_file`; `mutual_tls` ⇒ `ca_file` and `tls.enabled`.
- `acl.mode=enforce` ⇒ `tls.mutual_tls` or `acl.tokens_file`.
- `bootstrap.enabled` ⇒ seed role + a signing key + (`advertise_seeds` or `cluster.seeds`); `rate_limit >= 0`.
- `cluster.discovery` ⇒ `name`; `update_interval >= 0`.
