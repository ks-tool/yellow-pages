<!--
 Copyright © 2026 Alexey Shulutkov <github@shulutkov.ru>
 Licensed under the Apache License, Version 2.0.
-->

# Config bootstrap

`yp bootstrap` lets a node fetch a generated config from a seed instead of having
one pushed by Ansible/Chef/Puppet. Change a cluster parameter on the seeds once;
re-running `yp bootstrap` on a node writes the updated config, and a restart
applies it. No config-management tool required.

It rides the **existing seed↔agent gRPC server** as a `BootstrapService` RPC — no
extra listener or port — reusing that server's TLS/mTLS and interceptor chain
(so every call is access-logged and metered for free). It is treated as
sensitive: off by default, gated by **short-lived tokens**, sanitized, and with
seed-join separately locked down.

## Short-lived tokens

There is **no static token in the config** — only a **signing key** that never
leaves the seed. To bootstrap a node, an operator mints a token on the seed that
node will bootstrap from (each seed may hold its own signing key):

```bash
yp bootstrap create-token --config /etc/yp/seed.yaml      # prints a token, default TTL 30s
yp bootstrap create-token --config /etc/yp/seed.yaml --ttl 2m
```

The token is `base64url(expiry ‖ nonce ‖ HMAC-SHA256(signing_key, …))` — stateless:
the serving seed verifies the signature and expiry with the `signing_key` from
**its own** config, so `create-token` needs no running server or shared state.
The signing key is the only secret in the config and is never transmitted; only
the expiring token is. A leaked token is useless after its TTL.

**Each seed has its own `signing_key` / `signing_key_file`.** A token is valid
only at seeds that hold the key that signed it, so:

- **Per-seed keys (default).** Mint the token on a seed and run `yp bootstrap`
  against **that same seed** — `create-token --config <seedX.yaml>` pairs with
  `yp bootstrap --seed <seedX>`. A token minted from seed A's key is rejected by
  seed B (HMAC mismatch → `Unauthenticated`).
- **Cluster-wide key (optional).** Provision the *same* `signing_key` on every
  seed; then a token minted on any seed validates on any other, and `--seed` can
  target any of them.

Either way, expiry relies on the seed clocks being in sync (NTP) for the
`token_ttl` window.

## How it works

```
yp bootstrap create-token ─(reads signing_key)─▶ prints a short-lived token
                                                          │
yp bootstrap ──gRPC GetConfig(role)──▶ seed   (token in "bootstrap-token" metadata)
   ▲                                     │  rate-limit → verify token (sig+expiry) → role gate
   │        sanitized YAML config        ▼
   └──────────── write --out ◀──── render (no TLS keys / ACL tokens)
```

The fetch is a **single unary call**, not a stream: `yp bootstrap` fetches the
config once, writes the file, and exits. There is **no agent-side polling and no
hot-reload** — a node picks up changes only when `yp bootstrap` is re-run and the
process is restarted. Drive the re-run however you already provision (image
build, cloud-init, a refresh job, or a systemd timer); the agent's own seed
connection is never used for config.

## Server (seed) — opt-in

```yaml
# seed config — bootstrap runs on listeners.grpc, no extra port
bootstrap:
  enabled: true
  signing_key_file: /etc/yp/bootstrap.key  # or signing_key: "<openssl rand -base64 48>"
  token_ttl: 30s                    # default lifetime of a minted token
  allow_seed_join: false            # default: only agent configs are served
  advertise_seeds:                  # seeds written into served configs
    - seed-a.internal:9900
    - seed-b.internal:9900
  rate_limit: 10                    # per-client req/s (default 10)
```

`yp` refuses to start if bootstrap is enabled without a signing key, on a
non-seed role, or with no advertisable seed list (`advertise_seeds` or
`cluster.seeds`). Generate a key with e.g. `openssl rand -base64 48` and keep it
in `signing_key_file` (mode 0600), not inline in YAML.

## Client

```bash
yp bootstrap --seed seed-a.internal:9900 --token "$YP_BOOTSTRAP_TOKEN" \
    --role agent --out /etc/yp/config.yaml
systemctl restart yp        # apply
```

| Flag | Meaning |
|---|---|
| `--seed` | seed gRPC address `host:port` — **must be a seed whose `signing_key` minted the `--token`** (each seed may hold its own key) (required) |
| `--token` | bootstrap token (or env `YP_BOOTSTRAP_TOKEN`) (required) |
| `--role` | `agent` (default) or `seed` |
| `--out`, `-o` | output file (default: stdout) |
| `--tls` | use TLS to reach the seed |
| `--ca` | CA bundle to verify the seed (PEM) |
| `--cert` / `--key` | client cert/key for mTLS (PEM) |
| `--insecure` | skip TLS verification (not recommended) |
| `--timeout` | request timeout (default 10s) |

`--role seed` is rejected with `PermissionDenied` unless the cluster set
`allow_seed_join: true`. A bad / missing / **expired** token returns
`Unauthenticated`.

Because tokens are short-lived, drive the flow from whatever has seed access
(your orchestrator / provisioning step): mint on a seed, then immediately fetch
on the node within the TTL. With per-seed keys, **`--seed` must be the same seed
the token was minted on** (here, `seed-a`).

```bash
# mint on seed-a (reads seed-a's signing_key):
TOKEN=$(ssh seed-a yp bootstrap create-token --config /etc/yp/seed.yaml)
# fetch from the SAME seed (seed-a), within token_ttl:
YP_BOOTSTRAP_TOKEN=$TOKEN yp bootstrap --seed seed-a.internal:9900 --tls \
    --ca /etc/yp/ca.crt --role agent --out /etc/yp/config.yaml
systemctl restart yp                # apply
```

A periodic config refresh repeats this mint→fetch→restart cycle from the
orchestrator; an unattended **agent** cannot mint its own token (it has no
signing key — by design), so it never holds a long-lived bootstrap credential.

## What the generated config contains

**Included** (the parameters a node needs): `role`, `datacenter`,
`cluster.{name,seeds}` (the advertised seeds), `ttl`, `heartbeat_interval`,
`shutdown_timeout`, `listeners` (loopback binds with the cluster's ports),
`dns.{domain,alt_domain}`, `agent` tuning (agent role), `max_services` (seed
role), and the `tls.enabled`/`mutual_tls` + `acl.mode` **flags**.

**Never included** (provision out-of-band): TLS `cert_file`/`key_file`/`ca_file`,
`acl.tokens_file`, the bootstrap `signing_key`, `node_name`, `data_dir`.
Bootstrap distributes **parameters, not identity** — each node sets its own name and
supplies its own credentials at the standard local paths.

## Threat model & compensating controls

An attacker who can reach the RPC could (a) harvest the cluster config / seed
topology, or (b) — far worse — generate a **seed** config and join the registry
tier to serve or harvest registrations. The controls:

| Risk | Control |
|---|---|
| Exposure by default | RPC **off by default** (`bootstrap.enabled`); registered only on a seed. |
| Anonymous pulls | **Token required**, HMAC-signed by `signing_key`; the signature is compared in constant time (`hmac.Equal`), then expiry is checked. Start-up fails without a signing key; a too-short key warns (mint hard-floors at 16 bytes). |
| Stolen / replayed token | Tokens are **short-lived** (`token_ttl`, default 30s) → useless after expiry; the signing key itself never goes on the wire. |
| Token sniffing | Runs on the gRPC server's **TLS/mTLS** (`tls.enabled`/`mutual_tls`). An insecure gRPC server logs a loud warning when bootstrap is on. |
| Rogue **seed** joining | **`allow_seed_join: false`** by default → `role=seed` returns `PermissionDenied`. Even when enabled, a new seed still needs valid credentials and to be in peers (bootstrap grants **no** trust by itself). |
| Secret leakage | Served config is **sanitized** (see above): no TLS keys, ACL tokens, or the signing key. |
| Amplification / DoS | Per-client **rate limit** (`rate_limit`, default 10/s) → `ResourceExhausted`. |
| Blind abuse | Every call (served **and** denied, with reason) is **audit-logged** with client addr and role. |
| Network exposure | Restrict `listeners.grpc.address` and firewall the gRPC port. |

### Hardening checklist

- Enable TLS (ideally mTLS) on the gRPC server; never run bootstrap on an insecure server off-loopback.
- Keep `allow_seed_join: false`; provision the (rare) new seeds manually.
- Keep `token_ttl` short (the default 30s is usually enough for an automated mint→fetch).
- Store each seed's `signing_key` in `signing_key_file` (mode 0600) and rotate it periodically — it is that seed's long-lived bootstrap secret. With per-seed keys, rotating one seed invalidates only its tokens; reuse one key across seeds only if you want tokens to validate cluster-wide (then rotate them together).
- Restrict `listeners.grpc.address` and firewall the gRPC port to the provisioning network.
- Provision each node's own TLS cert/key and ACL tokens out-of-band.
