<!--
 Copyright © 2026 Alexey Shulutkov <github@shulutkov.ru>
 Licensed under the Apache License, Version 2.0.
-->

# Security

This document is the security model and hardening guide for yellow-pages. For the
config keys it references, see [docs/configuration.md](docs/configuration.md).

## Threat model in one paragraph

yellow-pages distributes service-discovery data (who is where), not secrets. Its
default posture is **trusted L3**: every listener binds `127.0.0.1`, transport is
insecure plaintext, and writes are anonymous-allow. That is correct for a single
host or a trusted private network; for anything broader, turn on the controls
below. The system is **AP** — seeds are independent and never coordinate on the
hot path — so there is no quorum to attack and no single seed whose compromise
fails the cluster, but equally no global authority: trust is per-node.

## Transport: TLS / mTLS

Off by default. Turn it on cluster-wide:

```yaml
tls:
  enabled: true
  cert_file: /etc/yp/node.crt
  key_file:  /etc/yp/node.key
  ca_file:   /etc/yp/ca.crt      # required for mutual_tls
  mutual_tls: true               # require + verify client certs both ways
```

- It applies to **all** gRPC (agent↔seed, the SDK, and the bootstrap RPC) with no
  code change.
- Certs **hot-reload** on rotation (mtime-watched) — no restart.
- Under `mutual_tls`, the caller identity becomes the **verified certificate
  subject**, which the ACL layer can authorize.
- The Consul HTTP/DNS surfaces are local agent-facing; keep them on loopback.

## Authorization: ACLs

Write authorization for the registry. Modes (`acl.mode`):

| Mode | Behaviour |
|---|---|
| `disabled` (default) | No checks; any write accepted. |
| `allow` | Tokens/identities accepted but never denied (audited). |
| `enforce` | A write must be made by the **owner** of the node it targets. |

`enforce` needs a way to identify callers — **`tls.mutual_tls`** (cert subject)
or **`acl.tokens_file`** (a token→principal map) — else every write is anonymous
and denied. Every write is audit-logged (method, target node, principal, peer,
result). Setting `acl.default_policy: deny` while `mode: allow` logs a loud
warning (silent enforcement loss after a Consul cutover). ACL **token
management** itself is out of scope (provision tokens out-of-band).

## Config bootstrap (off by default)

A seed can serve generated configs to joining nodes over the gRPC
`BootstrapService` (see [docs/bootstrap.md](docs/bootstrap.md)). It is a sensitive
config-distribution channel; its controls:

- **Off by default** (`bootstrap.enabled`), seed role only.
- **Short-lived HMAC tokens** — the config holds only a `signing_key` (never sent
  on the wire); `yp bootstrap create-token` mints a token valid for `token_ttl`
  (default 30s). A leaked token is useless after expiry; each seed may use its own
  key (a token is valid only at seeds that hold the matching key).
- **Seed-join is separately gated** (`allow_seed_join`, default false): a caller
  cannot generate a `role=seed` config — so it cannot graft a rogue node into the
  registry tier — unless explicitly allowed.
- **Sanitized output** — served configs never carry TLS keys, ACL tokens, or the
  signing key.
- Per-client rate limit + audit on every (served and denied) request.

## Active health checks

Service health checks (HTTP/TCP/UDP) are network probes from the agent. **Script
(exec) checks run an arbitrary local binary**, so they are gated:

- Off unless `enable_script_checks: true`.
- The command must be an **absolute path** and is executed **directly — never via
  a shell** (no `sh -c`, no interpolation).

## Denial-of-service guards

| Surface | Guard | On exceed |
|---|---|---|
| Consul HTTP | `consul_rate_limit` (per-client req/s) | `429` |
| Consul DNS | `dns.rate_limit` (per-client RRL) + forced response truncation | `REFUSED` / TC bit |
| Bootstrap | `bootstrap.rate_limit` (per-client) | `ResourceExhausted` |
| Registry size | `max_services` cap | `ResourceExhausted` on new writes |
| Blocking queries | concurrent-waiter cap | `429` |

The DNS/filter parsers are fuzz-tested; rate-limit keys are the source **host**
(not host:port), so reconnecting cannot reset a client's counter.

## Network exposure

- All listeners default to `127.0.0.1`. Binding `0.0.0.0` is an explicit choice —
  **firewall the port** to the intended peers.
- Seeds must be reachable by agents on the gRPC port; restrict that path.
- The bootstrap and metrics endpoints, when enabled, should be bound to a
  controlled interface and firewalled.

## Supply chain

Release artifacts (`release.yml`) ship with an **SBOM** (syft), **cosign**
signatures, **SLSA** provenance, checksums, and a **trivy**-scanned multi-arch
**distroless** image (non-root). `govulncheck` is a pinned tool dependency and
runs in CI; the dependency budget is deliberately small (no HTTP framework, no
second JSON library).

## Out of scope

- **KV, Connect/mesh, sessions, prepared queries** — not implemented; audit
  consumers for them before a Consul cutover (see [docs/migration.md](docs/migration.md)).
- **Visible-critical from an agent.** An agent cannot push a "critical but
  visible" state to seeds (no force-critical RPC); a failing agent-side check lets
  the lease lapse instead. The seed-served surface is authoritative.
- **NTP is a precondition** for the `last_seen` LWW tiebreak (generation
  dominates); large clock skew is exposed as `yp_agent_seed_clock_skew_seconds`.

## Hardening checklist

- [ ] Enable `tls` (ideally `mutual_tls`) for any non-loopback deployment.
- [ ] `acl.mode: enforce` with an identity source if untrusted clients can write.
- [ ] Keep `bootstrap` off; if used, keep `allow_seed_join: false`, a short
      `token_ttl`, and the `signing_key` in a `signing_key_file` (mode 0600).
- [ ] Keep `enable_script_checks` off unless you control the binaries.
- [ ] Set `consul_rate_limit`, `dns.rate_limit`, `max_services` for exposed surfaces.
- [ ] Bind every listener to a controlled interface; firewall the gRPC/bootstrap ports.
- [ ] Run on NTP-synced clocks; alert on clock-skew and per-seed divergence.
- [ ] Provision each node's own cert/key and ACL tokens out-of-band.

## Reporting a vulnerability

Please report security issues **privately**, not via public issues — open a
GitHub security advisory on the repository (Security → *Report a vulnerability*),
or contact the maintainer at the address in the source-file copyright headers.
We aim to acknowledge within a few business days.
