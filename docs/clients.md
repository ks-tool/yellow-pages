<!--
 Copyright © 2026 Alexey Shulutkov <github@shulutkov.ru>
 Licensed under the Apache License, Version 2.0.
-->

# Consuming yellow-pages

Every app talks to its **local agent** (`127.0.0.1:9900` by default). The agent
proxies writes to the seeds and serves merged reads, so consumers never address
seeds directly.

This guide covers the Go SDK, the `yp://` gRPC resolver, the cross-language
stubs, and the Consul-compatible path for clients that already speak Consul.

## Go SDK (`client/sdk`)

`client/sdk` depends only on the generated `discovery.v1` proto — never on
internal packages — so it is a stable import.

```go
import (
    "context"

    "github.com/ks-tool/yellow-pages/client/sdk"
    discoveryv1 "github.com/ks-tool/yellow-pages/proto/discovery/v1"
)

cli, err := sdk.Dial(sdk.DefaultAgentAddress) // 127.0.0.1:9900, insecure loopback
if err != nil { /* ... */ }
defer cli.Close()

// Register this app's service. Register is idempotent.
err = cli.Register(ctx, &discoveryv1.Registration{
    Node:     &discoveryv1.Node{Id: "node-1", Address: "10.0.0.5", Datacenter: "dc1"},
    Services: []*discoveryv1.Service{{Name: "user-api", Address: "10.0.0.5", Port: 8080, TtlSeconds: 30}},
    Generation: 1,
})

// Keep the lease alive (or let the agent's renew loop do it).
_ = cli.Renew(ctx, "node-1")

// Discover healthy instances of another service.
entries, _ := cli.Discover(ctx, &discoveryv1.Query{Name: "billing", OnlyHealthy: true})

// Watch for live changes (each value is the full current set).
updates, _ := cli.Watch(ctx, &discoveryv1.Query{Name: "billing", OnlyHealthy: true})
for set := range updates { /* react to the new instance set */ }
```

## gRPC resolver (`client/grpcresolver`)

Register the `yp://` scheme once, then dial logical service names. Addresses are
discovered through the local agent and updated live on register/deregister — no
restart. Per-instance `Weights` are attached to each address for weight-aware
balancers; the default policy is `round_robin`.

```go
import "github.com/ks-tool/yellow-pages/client/grpcresolver"

grpcresolver.Register() // installs the yp:// resolver (insecure loopback to the agent)

// yp://[agent-host:port]/service-name — empty authority uses the default agent.
conn, _ := grpc.NewClient("yp:///billing", grpc.WithTransportCredentials(insecure.NewCredentials()))
client := billingpb.NewBillingClient(conn)
```

## Cross-language stubs (Java / Python)

Non-Go consumers use the generated `discovery.v1` stubs. Generate them with buf's
hosted plugins (no local toolchains needed):

```bash
make proto-stubs   # buf generate -> gen/java, gen/python
```

Then call the same `AgentService` RPCs against the local agent:

- **Java** (grpc-java): build an `AgentServiceGrpc.AgentServiceBlockingStub` over a
  `ManagedChannel` to `127.0.0.1:9900` and call `register`, `renew`, `deregister`,
  `lookup`, `watch`.
- **Python** (grpcio): `discovery_pb2_grpc.AgentServiceStub(channel)` against the
  same address.

The RPC set and message shapes are identical to the Go SDK above.

## Consul-compatible path (any language)

Clients that already speak Consul need no yellow-pages stubs at all: point them
at the agent's Consul-compatible HTTP API (`:8500`) and DNS (`:8600`). Set
`CONSUL_HTTP_ADDR=127.0.0.1:8500` and register/discover via `/v1/agent/service/*`,
`/v1/catalog/*`, `/v1/health/*`, or resolve `*.service.consul` over DNS. This is
the drop-in migration path (see the Consul-compat milestones M10–M13).
