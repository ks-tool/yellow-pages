<!--
 Copyright © 2026 Alexey Shulutkov <github@shulutkov.ru>
 Licensed under the Apache License, Version 2.0.
-->

# Migrating from Consul (single-DC)

yellow-pages is a drop-in for Consul **service discovery**. KV, Connect/mesh,
sessions, prepared queries and ACL token management are out of scope — audit for
those first (below). Cross-DC federation is deferred (M17), so migrate one DC.

## 0. Pre-cutover audit

Confirm consumers use only service discovery, not the out-of-scope perimeter:

```bash
grep -REn '/v1/(kv|connect|session|txn|operator|query)/' <consumer-configs>/
```

Any hit must be addressed (kept on Consul, or removed) before cutover. Also check
whether consumers set a token (`CONSUL_HTTP_TOKEN`); under `acl.mode=enforce` a
write needs a token that owns the node (see CLAUDE.md acl section).

## 1. Co-existence (run both)

Run yp alongside Consul on the same host using alternate ports (every listener is
configurable), or on separate hosts. Point a subset of consumers at yp via
split-horizon (`CONSUL_HTTP_ADDR`, a separate DNS view) to validate.

```yaml
listeners:
  consul_http: { enabled: true, address: 127.0.0.1, port: 18500 }  # yp; Consul keeps 8500
  dns:         { enabled: true, address: 127.0.0.1, port: 18600 }
```

## 2. Backfill the catalog

Import the existing Consul catalog into yp before cutover (idempotent, repeatable):

```bash
yp import --from http://127.0.0.1:8500 --to http://127.0.0.1:18500
```

Keep **dual-registration** active (apps register to both) until yp is trusted.

## 3. Validate with a normalized shadow-diff

Compare yp against Consul on `/v1/health/service`, `/v1/catalog/services`,
`/v1/catalog/nodes`. The diff is a set keyed by
`(Node, ServiceID, Address, Port, Tags, status)`, ignoring `X-Consul-Index`,
timestamps, `X-Consul-LastContact` and order (`internal/migrate.ShadowDiff`). It
must be **empty** before cutover. DNS: `dig` parity for A/SRV against both.

## 4. Cutover

Switch `.consul` delegation (dnsmasq / systemd-resolved / CoreDNS forward) and
`CONSUL_HTTP_ADDR` to yp's ports. Watch the SLOs (`docs/slo.md`), especially
`yp_agent_seed_divergence`.

## 5. Rollback

Reverse the `.consul` delegation and `CONSUL_HTTP_ADDR`. To avoid losing
registrations made in yp **after** cutover, reverse-import them into Consul
(`yp import --from <yp> --to <consul>` resolves to the same `/v1/catalog/register`
contract). Dual-registration during the soak window minimizes this risk.
