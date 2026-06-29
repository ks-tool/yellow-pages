<!--
 Copyright © 2026 Alexey Shulutkov <github@shulutkov.ru>
 Licensed under the Apache License, Version 2.0.
-->

# Seed membership & anti-entropy (v1.x)

**Feature-flagged, off by default.** By default seeds are **independent** — they
never sync, and agents paper over any divergence by writing to every seed. That is
the AP design and needs no membership. This option makes the seed tier
**self-healing**: a joining or recovered seed catches up before serving, and seeds
reconcile divergence in the background.

## What it does

- **Snapshot-on-join.** A seed that (re)starts pulls a full snapshot from its peers
  **before** it reports ready. Until the snapshot completes its gRPC health is
  `NOT_SERVING`, so no client reads its empty registry — there is no
  false-negative "the service is gone" window.
- **Pull-based anti-entropy.** Every `sync_interval` a seed pulls its peers' state
  and merges it **last-writer-wins** (`store.Merge`, preserving each peer's
  `last_seen`). A concurrent live write is never lost — the newer copy always wins
  the merge.
- **`/v1/agent/members`** reports the live seed membership.
- The SLI `yp_seed_convergence_lag` exposes how much the last anti-entropy pass had
  to apply (0 once converged).

## Configuration (seed)

```yaml
role: seed
membership:
  enabled: true
  peers: [seed-b:9900, seed-c:9900]   # the OTHER seeds' gRPC addresses
  sync_interval: 30s
```

List the other seeds in `peers` (not self). It reuses the existing `Lookup`/merge
machinery — no new wire contract.

## When to use it

- A seed restarts often (containers/autoscaling) and you want it to rejoin without
  a cold window or relying solely on agents to re-fill it.
- You want seeds to actively converge instead of depending on agent re-writes.

If your seeds are stable and agents reliably write to all of them, the default
(independent seeds) is simpler and fine — divergence is already self-correcting on
the next write.

## Caveats

- Convergence is **eventual** (pull every `sync_interval`); it is not a synchronous
  replica.
- Anti-entropy and live writes are LWW-merged, so they don't lose updates, but a
  very large `sync_interval` widens the divergence window — watch
  `yp_agent_seed_divergence` and `yp_seed_convergence_lag`.
- This is a v1.x extension; it does not change the AP guarantee (still no quorum).

See also [federation.md](federation.md) (cross-DC reads) and
[operations.md](operations.md#troubleshooting) (inspecting divergence).
