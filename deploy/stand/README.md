<!--
 Copyright © 2026 Alexey Shulutkov <github@shulutkov.ru>
 Licensed under the Apache License, Version 2.0.
-->

# Test stand (docker compose)

A throwaway 2-seed / 2-agent yellow-pages cluster to exercise the AP model and
the Consul-compatible HTTP/DNS surfaces locally. Configs live here; the compose
file is at the repo root.

```bash
docker compose up --build          # seeds + agents
docker compose ps                  # 4 containers: yp-seed{1,2}, yp-agent{1,2}
docker compose down                # tear down
```

## Topology

| Node | Role | Consul HTTP | DNS | metrics |
|---|---|---|---|---|
| `seed1` | seed | `:8501` | — | `:9901` |
| `seed2` | seed | `:8502` | — | `:9902` |
| `agent1` | agent | `:8511` | `:8611` | `:9911` |
| `agent2` | agent | `:8512` | `:8612` | `:9912` |

Seeds are **independent** (they don't sync — anti-entropy is M18, off here).
Agents fan registrations out to *both* seeds and merge reads (LWW), so a write on
one agent is visible from the other even though the seeds never talk.

## Demo: register on one agent, discover on the other

```bash
# register a service via agent1's Consul HTTP API
curl -XPUT localhost:8511/v1/agent/service/register \
     -d '{"Name":"web","Port":8080,"Address":"10.1.2.3"}'

# discover it via agent2 (fetched from both seeds + merged)
curl -s localhost:8512/v1/health/service/web

# and over DNS
dig @127.0.0.1 -p 8612 web.service.consul +short        # -> 10.1.2.3
dig @127.0.0.1 -p 8612 -t SRV web.service.consul +short
```

## Metrics (optional)

```bash
docker compose --profile monitoring up -d --build       # adds Prometheus
open http://localhost:9090                               # all four nodes scraped
```

See `docs/slo.md` for the SLIs/SLOs exposed at each `/metrics`.

## Notes

- Configs bind `0.0.0.0` (containers must be reachable across the bridge network);
  the production default is loopback.
- Node identity is set via `node_name`; there is no `data_dir`, so restarts get a
  fresh identity (fine for a stand).
