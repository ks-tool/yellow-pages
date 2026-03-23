# Yellow Pages – Service Discovery

> **⚠️ Early Development**: This project is in active development and not yet production-ready. APIs, configuration
> formats, and behavior may change without notice. Use at your own risk.

Yellow Pages is a lightweight, flexible service discovery system designed for on‑premise environments with strict
network policies. It allows services to register themselves and discover other services across clusters, using a
peer‑to‑peer model where every node can act as either an agent (service provider) or a seed (registry + agent).

## Features

- **Dual‑role nodes** – a single binary can run as a plain agent or as a seed that also maintains a registry.
- **Plugin‑based seed discovery** – obtain the list of seed addresses via an external executable, falling back to a
  static list.
- **gRPC API** – all communication between nodes uses gRPC (AgentService). Seeds additionally expose registration and
  lookup methods.
- **Eventual consistency** – agents register on all known seeds; during look‑up they query all seeds and pick the most
  recent data.
- **Cross‑cluster & cross‑datacenter** – seeds can be configured to know about seeds in other clusters, and agents carry
  a datacenter label that can be used for filtering.
- **Low footprint** – no external dependencies, no HTTP server, no DNS multicast – only explicit TCP connections.

## Architecture

Every node runs the same binary. The role is determined by the `seed` field in the configuration file:

- **Agent** (`seed: false` or omitted) – a node that hosts services. It connects to all seeds in its cluster, registers
  its services, sends periodic heartbeats, and answers `GetEndpoints` calls from other agents.
- **Seed** (`seed: true`) – a node that, in addition to being an agent, maintains an in‑memory registry of all agents
  and services in the cluster. It handles `RegisterAgent`, `Heartbeat`, `DeregisterAgent`, and `GetServiceAgents` gRPC
  calls.

All seeds are **equal**: they do not synchronise data with each other. Agents write to every seed, so each seed holds an
independent copy of the registry. When an agent looks up a service, it queries all seeds, merges the results, and keeps
the freshest information (based on the `last_seen` timestamp). This design provides high availability without complex
consensus protocols.

Seeds can also be configured with a list of remote clusters (`clusters`), allowing them to forward cross‑cluster lookup
requests via the `GetRemoteService` gRPC method. Agents include a `datacenter` label, which can be used in filters to
restrict lookups to a specific datacenter.

### Discovery of seeds

The list of seed addresses that an agent (or seed) should connect to is obtained in one of two ways:

1. **Static list** – provided in the `seeds` field of the configuration.
2. **Plugin** – an external executable that prints a JSON object with a `"seeds"` array. The plugin is invoked with an
   environment variable containing its options (if any). The path to the executable and its options are specified under
   the `discovery` section.

### Communication

- All gRPC methods are defined in `proto/service_discovery.proto`.
- The service `AgentService` is implemented by every node.
- Seeds implement the additional methods (Register, Heartbeat, etc.) – they are simply marked as `Unimplemented` on
  non‑seed nodes.
- Connections are established using plain TCP with `grpc.WithInsecure()`; production setups should use mTLS.

### Registry structure

Each seed maintains:

- A map `agentsByID` storing `AgentRecord` (agent info, services, last seen, TTL).
- An index `serviceIndex` mapping service names to agent records.
- A background goroutine that periodically removes expired records.

When an agent registers, it provides a TTL (seconds). The seed updates `last_seen` on every heartbeat. If no heartbeat
arrives within the TTL, the agent is considered dead and removed.

## Configuration

The configuration file can be in JSON or YAML format. The following fields are recognised:

| Field                     | Type              | Description                                                                 |
|---------------------------|-------------------|-----------------------------------------------------------------------------|
| `cluster_name`            | string            | **Required**. Logical name of the cluster this node belongs to.             |
| `datacenter`              | string            | **Required**. Logical datacenter name. Used for cross‑datacenter filtering. |
| `seed`                    | bool              | If `true`, the node starts as a seed. Default `false`.                      |
| `seeds`                   | []string          | Static list of seed addresses (host:port).                                  |
| `discovery`               | object            | Plugin configuration for dynamic seed discovery.                            |
| `discovery.name`          | string            | Path to the executable.                                                     |
| `discovery.options`       | map[string]any    | Options that will be passed to the plugin via environment variable.         |
| `port`                    | uint16            | gRPC port the node listens on.                                              |
| `services`                | []object          | List of services this node provides (only used in agent mode).              |
| `services[].name`         | string            | Service name.                                                               |
| `services[].endpoints`    | []object          | Endpoints where the service can be reached.                                 |
| `services[].tags`         | []string          | Optional tags for filtering.                                                |
| `services[].metadata`     | map[string]string | Arbitrary metadata.                                                         |
| `ttl_seconds`             | int64             | TTL for agent registrations (seconds). Default 30.                          |
| `heartbeat_interval_sec`  | int64             | Interval between heartbeats (seconds). Default 10.                          |
| `clusters`                | []object          | (Seed only) List of remote clusters for cross‑cluster lookups.              |
| `clusters[].cluster_name` | string            | Name of the remote cluster.                                                 |
| `clusters[].seeds`        | []string          | Seed addresses of that cluster.                                             |

When an agent registers, it sends its `datacenter` value. A seed can then filter lookup results by datacenter using the
`filters` map in `GetServiceAgentsRequest`. For example, a client can pass `{"datacenter": "eu-west-1"}` to only receive
agents from that datacenter.

### Example configuration (agent)

```yaml
cluster_name: "production"
datacenter: "eu-west-1"
seed: false
seeds:
  - "10.0.0.1:9900"
  - "10.0.0.2:9900"
port: 8501
ttl_seconds: 30
heartbeat_interval_sec: 10
services:
  - name: "user-api"
    endpoints:
      - name: "grpc"
        protocol: "grpc"
        address: "10.0.0.10"
        port: 9000
    tags: [ "v2" ]
    metadata:
      env: "prod"
```

### Example configuration (seed)

```yaml
cluster_name: "production"
datacenter: "eu-west-1"
seed: true
port: 9900
ttl_seconds: 60
clusters:
  - cluster_name: "staging"
    seeds:
      - "10.1.0.1:9900"
      - "10.1.0.2:9900"
```

### Plugin configuration

If a plugin is used, it must be an executable that prints a JSON object like:

```json
{
  "seeds": [
    "10.0.0.1:9900",
    "10.0.0.2:9900"
  ]
}
```

The plugin receives options via the environment variable `YELLOW_PAGES_PLUGIN_OPTIONS` as a JSON string.

Example config using a plugin:

```yaml
cluster_name: "production"
discovery:
  name: "/usr/local/bin/seed-fetcher"
  options:
    file: "/etc/seeds.json"
port: 9900
services: ...
```

## Building and running

### Prerequisites

- Go 1.26+ or later
- protoc with protoc-gen-go and protoc-gen-go-grpc

### Build

```bash
make build
```

This generates the gRPC code from the proto file and compiles the binary into `bin/yp`.

### Run

```bash
./bin/yp -config /path/to/config.yaml
```

The node will start and either act as an agent or a seed based on the configuration.

## Embedding a Java agent

If you want to integrate a Java service with Yellow Pages, you can implement a small client that uses the gRPC stubs to
register and heartbeat. Below is a minimal example.

### Step 1: Generate Java classes from the proto file

Use the protobuf compiler with the gRPC Java plugin:

```bash
protoc --proto_path=proto \
    --java_out=src/main/java \
    --grpc-java_out=src/main/java \
    proto/service_discovery.proto
```

Add the necessary dependencies (grpc-netty, grpc-stub, protobuf-java) to your `pom.xml` or Gradle build.

### Step 2: Write a Java agent that registers a service

```java
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import discovery.AgentServiceGrpc;
import discovery.Discovery.*;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ServiceRegistrar {

    private final ManagedChannel channel;
    private final AgentServiceGrpc.AgentServiceBlockingStub stub;
    private final String agentId;
    private final Service service;
    private final long ttlSeconds;
    private final String datacenter;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public ServiceRegistrar(String seedAddr, String agentId, Service service, 
                            long ttlSeconds, String datacenter) {
        this.channel = ManagedChannelBuilder.forTarget(seedAddr).usePlaintext().build();
        this.stub = AgentServiceGrpc.newBlockingStub(channel);
        this.agentId = agentId;
        this.service = service;
        this.ttlSeconds = ttlSeconds;
        this.datacenter = datacenter;
    }

    public void start() {
        // Register once
        Agent agent = Agent.newBuilder()
                .setId(agentId)
                .setAddress(getLocalIp())
                .setPort(9000) // port where the service listens
                .setDatacenter(datacenter)
                .build();

        RegisterAgentRequest req = RegisterAgentRequest.newBuilder()
                .setAgent(agent)
                .addServices(service)
                .setTtlSeconds(ttlSeconds)
                .build();

        RegisterAgentResponse resp = stub.registerAgent(req);
        if (!resp.getSuccess()) {
            System.err.println("Registration failed: " + resp.getMessage());
            return;
        }
        System.out.println("Registered with seed: " + seedAddr);

        // Schedule heartbeats
        scheduler.scheduleAtFixedRate(this::sendHeartbeat, ttlSeconds / 2, ttlSeconds / 2, TimeUnit.SECONDS);
    }

    private void sendHeartbeat() {
        HeartbeatRequest req = HeartbeatRequest.newBuilder()
                .setAgentId(agentId)
                .setTtlSeconds(ttlSeconds)
                .build();
        try {
            HeartbeatResponse resp = stub.heartbeat(req);
            if (!resp.getSuccess()) {
                System.err.println("Heartbeat failed: " + resp.getMessage());
            }
        } catch (Exception e) {
            System.err.println("Heartbeat error: " + e.getMessage());
        }
    }

    public void stop() {
        try {
            DeregisterAgentRequest req = DeregisterAgentRequest.newBuilder()
                    .setAgentId(agentId)
                    .build();
            stub.deregisterAgent(req);
        } finally {
            scheduler.shutdown();
            channel.shutdown();
        }
    }

    private static String getLocalIp() {
        // Implement your logic to get the container/host IP
        return "10.0.0.10";
    }

    public static void main(String[] args) {
        // Example: register a service called "order-api"
        Endpoint endpoint = Endpoint.newBuilder()
                .setName("http")
                .setProtocol("http")
                .setAddress("10.0.0.10")
                .setPort(8080)
                .setPath("/api")
                .build();

        Service service = Service.newBuilder()
                .setName("order-api")
                .addEndpoints(endpoint)
                .addTags("v2")
                .putMetadata("env", "prod")
                .build();

        ServiceRegistrar registrar = new ServiceRegistrar(
                "10.0.0.1:9900",   // seed address
                "my-hostname",      // unique agent id
                service,
                30,                 // TTL seconds
                "eu-west-1"         // datacenter
        );

        Runtime.getRuntime().addShutdownHook(new Thread(registrar::stop));
        registrar.start();
    }
}
```

### Important notes

- The Java client must **register with every seed** in the cluster, not just one. The example above only shows a single
  seed; in production you would iterate over the list of seeds obtained from the configuration or a discovery plugin.
- Heartbeats should be sent to all seeds as well.
- The `agentId` must be unique across the cluster. A good practice is to combine the cluster name and the hostname.
- The TTL should be long enough to tolerate network hiccups, but short enough to detect failures quickly (e.g., 30
  seconds with heartbeat every 15 seconds).
- Use TLS in production (replace `usePlaintext()` with proper SSL credentials).

## Contributing

Pull requests and issues are welcome. Please follow the existing code style and add tests for new features.

## License

Apache 2.0
