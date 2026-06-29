<!--
 Copyright © 2026 Alexey Shulutkov <github@shulutkov.ru>
 Licensed under the Apache License, Version 2.0.
-->

# yellow-pages

**Lightweight peer-to-peer service discovery for on-premise environments — and a
drop-in replacement for Consul service discovery.**

yellow-pages targets networks with strict policies: **no DNS multicast, no
gossip, no external dependencies** — just explicit gRPC over TCP. Every node runs
the same binary (`yp`); its role is chosen by config. On top of a clean gRPC core
it layers Consul-compatible **HTTP** and **DNS**, so existing Consul clients,
`dig`, Spring Cloud, Nomad, etc. work unchanged.

It is **AP** (available + partition-tolerant): no Raft, no leader election. Seeds
are independent and never sync on the hot path; availability never depends on a
quorum.

## Roles

Every node runs `yp`; `role:` in the config selects behaviour.

- **Seed** (`role: seed`) — holds the in-memory registry of agents/services for
  the cluster, serves reads/writes, and GCs expired leases. Seeds are independent
  and do **not** coordinate (optional self-healing anti-entropy exists behind a
  flag, off by default).
- **Agent** (`role: agent`) — hosts services and proxies registrations to **every**
  seed (k-of-N quorum), heartbeats, and serves local reads by fanning out to the
  seeds and merging. Holds no inbound registry.

Writes go to every seed; reads query all seeds and merge **last-writer-wins** by
data `generation`, then `last_seen`. This gives availability without consensus.

```
        register (fan-out, k-of-N)          ┌──────────┐
   app ───────────────► agent ─────────────►│  seed 1  │
        Consul HTTP/DNS   │  ◄── Lookup ────│ registry │
        + native gRPC     │   (fan-out      └──────────┘
                          │    + LWW merge) ┌──────────┐
                          └────────────────►│  seed 2  │
                                            └──────────┘
```

## Quickstart (test stand)

A 2-seed / 2-agent cluster via docker compose:

```bash
docker compose up --build

# register a service on agent1, discover it on agent2 (fan-out + cross-seed merge)
curl -XPUT localhost:8511/v1/agent/service/register \
     -d '{"Name":"web","Port":8080,"Address":"10.1.2.3"}'
curl -s localhost:8512/v1/health/service/web
dig @127.0.0.1 -p 8612 web.service.consul +short        # -> 10.1.2.3
```

See [`deploy/stand/README.md`](deploy/stand/README.md) for the full topology and a
Prometheus profile.

## Served surfaces

| Surface                        | Default               | Purpose                                                                                                                                                |
|--------------------------------|-----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Native gRPC** `discovery.v1` | `:9900` (always on)   | `AgentService` (Register/Renew/Deregister/Lookup/Watch) + `BootstrapService`. Use the [Go SDK / `yp://` resolver](docs/clients.md).                    |
| **Consul HTTP**                | `:8500` (off)         | `/v1/agent`, `/v1/catalog`, `/v1/health`, blocking queries, `?stale`/`?consistent`, `?filter`, tokens. See the [compat matrix](docs/consul-compat.md). |
| **Consul DNS**                 | `:8600` UDP+TCP (off) | A/AAAA/SRV/TXT/SOA/NS; configurable zone (`dns.domain` / `dns.alt_domain`).                                                                            |
| **Prometheus** `/metrics`      | `:9901` (off)         | SLIs (propagation, divergence, clock skew, fan-out). See [`docs/slo.md`](docs/slo.md).                                                                 |

All listeners bind `127.0.0.1` by default; binding to `0.0.0.0` is an explicit,
firewall-it choice.

## Building & testing

```bash
make build        # -> bin/yp
make test         # go test -race ./...
make e2e          # real yp binary + real Consul (testcontainers); needs Docker
make verify       # full local gate: tidy + vet + lint + buf + test + vuln
```

`yp --config <file> [--role agent|seed]`. Subcommands: `yp import` (backfill a
Consul catalog), `yp bootstrap` / `yp bootstrap create-token` (config bootstrap).

## Configuration

Single snake_case schema, YAML or JSON (one parser; YAML is a JSON superset),
unknown keys rejected. Minimal agent:

```yaml
role: agent
datacenter: dc1
cluster:
  name: prod
  seeds: [ seed-a:9900, seed-b:9900 ]
listeners:
  consul_http: { enabled: true }
  dns: { enabled: true }
```

Full reference: [`docs/configuration.md`](docs/configuration.md). Samples:
[`configs/agent.yaml`](configs/agent.yaml), [`configs/seed.yaml`](configs/seed.yaml).

## Documentation

| Doc                                            | About                                                                    |
|------------------------------------------------|--------------------------------------------------------------------------|
| [docs/architecture.md](docs/architecture.md)   | How it works: AP model, fan-out/merge/LWW, watch index, why no Raft      |
| [docs/configuration.md](docs/configuration.md) | Every config key, type, default and rule                                 |
| [docs/grpc-api.md](docs/grpc-api.md)           | Native `discovery.v1` gRPC contract (AgentService / BootstrapService)    |
| [docs/clients.md](docs/clients.md)             | Go SDK, `yp://` gRPC resolver, cross-language stubs                      |
| [docs/consul-compat.md](docs/consul-compat.md) | Consul HTTP/DNS compatibility matrix + AP limits                         |
| [docs/operations.md](docs/operations.md)       | Runbook: topology, DNS delegation, monitoring, troubleshooting, upgrades |
| [docs/slo.md](docs/slo.md)                     | SLIs / SLOs / alerts                                                     |
| [docs/dashboard.md](docs/dashboard.md)         | Prometheus metric reference + Grafana dashboard                          |
| [docs/migration.md](docs/migration.md)         | Consul → yp cutover runbook (import, shadow-diff, rollback)              |
| [docs/bootstrap.md](docs/bootstrap.md)         | Central config bootstrap over gRPC (short-lived tokens)                  |
| [docs/federation.md](docs/federation.md)       | Cross-DC reads (v1.x, feature-flagged)                                   |
| [docs/membership.md](docs/membership.md)       | Seed anti-entropy / self-healing (v1.x, feature-flagged)                 |
| [SECURITY.md](SECURITY.md)                     | Security model, TLS/mTLS, ACLs, hardening                                |

## What it is not

Service **discovery** only. KV, Connect/mesh, sessions, prepared queries and ACL
**token management** are out of scope — audit consumers for those before a Consul
cutover (see the migration runbook). Cross-DC federation is feature-flagged.

## License

Apache-2.0. See [LICENSE](LICENSE).
