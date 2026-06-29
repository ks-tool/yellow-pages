<!--
 Copyright © 2026 Alexey Shulutkov <github@shulutkov.ru>
 Licensed under the Apache License, Version 2.0.
-->

# End-to-end tests

These tests run the **real `yp` binary** (built from `../cmd/yp`) and, for the
conformance and migration suites, a **real Consul** in a container
([testcontainers-go]). They prove yp is a faithful Consul drop-in by driving both
yp and Consul through the official `github.com/hashicorp/consul/api` client and
comparing the results with a normalized shadow-diff (`internal/migrate.ShadowDiff`).

This is a **separate Go module** so its heavy, test-only dependencies
(testcontainers, consul/api) stay out of the main module's dependency budget and
supply-chain gate. The main module's `go test ./...` does not descend into it.

## Running

```bash
cd e2e
go test ./...                       # all suites (Consul tests need Docker)
go test -run TestDNSAgainstYP ./... # yp-only, no Docker
```

The Consul-backed tests **skip cleanly** when Docker is unavailable. Pinned
image: `hashicorp/consul:1.20`; pinned client: `consul/api v1.33.5`.

## Suites

| Test | Needs Docker | What it proves |
|---|---|---|
| `TestDNSAgainstYP` | no | DNS A/SRV/NXDOMAIN over the wire (dig path) |
| `TestBlockingQueryHandover` | no | `WaitIndex` blocking query advances, no busy-loop |
| `TestHealthServiceConformance` | yes | `/v1/health/service` ≡ Consul (normalized) |
| `TestCatalogServicesConformance` | yes | `/v1/catalog/services` ≡ Consul |
| `TestImportFromRealConsul` | yes | `migrate.Import` backfills a real Consul catalog; shadow-diff empty |

[testcontainers-go]: https://golang.testcontainers.org/
