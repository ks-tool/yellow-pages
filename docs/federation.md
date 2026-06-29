<!--
 Copyright © 2026 Alexey Shulutkov <github@shulutkov.ru>
 Licensed under the Apache License, Version 2.0.
-->

# Cross-DC federation (v1.x)

**Feature-flagged, off by default.** The base model is single-datacenter; this
adds cross-DC **reads** so a query tagged with another datacenter is answered by
that cluster's seeds.

## What it does

A lookup carrying a remote datacenter — Consul `?dc=<name>` on HTTP, or
`<svc>.service.<dc>.dc.consul` on DNS — is routed to that datacenter's seeds,
merged the same way (LWW), and returned. The local datacenter and the `dc1` alias
resolve locally; an empty dc is local. `GET /v1/catalog/datacenters` lists the
local plus all configured remote datacenters.

Only reads federate. Registrations stay in their home datacenter.

## Configuration (agent)

```yaml
datacenter: dc1
federation:
  enabled: true
  max_hops: 1                # loop guard
  datacenters:
    dc2: [seed-a.dc2:9900, seed-b.dc2:9900]
    dc3: [seed-a.dc3:9900]
```

The agent dials one seed-client per remote datacenter. A query for a configured
remote dc fans out to its seeds; a query for an **unknown** dc returns **empty**
(Consul-like) rather than fanning out — the storm guard.

## Trust & security

Cross-DC reads go over the same gRPC transport, so they inherit your
[TLS/mTLS](../SECURITY.md#transport-tls--mtls): enable `tls.mutual_tls` so a
cluster only answers federated reads from peers presenting a trusted client cert.
A remote cluster authorizes the incoming read with its own ACLs. Provenance is
inherent — returned entries carry their source datacenter on each node.

## Caveats

- **Reads only.** No cross-DC writes or replication.
- Remote seeds serve their own registry and do **not** re-federate, so a query is
  a single hop — no loops; `max_hops` bounds the configured depth.
- Latency: a federated read waits on the remote seeds (subject to
  `agent.seed_timeout`).
- Clocks across datacenters should be NTP-synced for the LWW tiebreak.
- This is a v1.x extension; keep it off unless you need cross-DC discovery.

See also [membership.md](membership.md) (seed self-healing within a DC) and
[architecture.md](architecture.md).
