/*
 Copyright © 2026 Alexey Shulutkov <github@shulutkov.ru>

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 	http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

// Package config defines the yellow-pages configuration schema and a strict
// loader. The on-disk format is snake_case YAML or JSON; YAML is a superset of
// JSON, so a single library (gopkg.in/yaml.v3) parses both — there is no second
// JSON dependency and no GOEXPERIMENT-gated path. Load applies defaults and then
// validates, rejecting unknown keys so a typo never boots with silent zeros.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// defaultBind is the default listener address. Per the security posture all
// listeners bind loopback by default; rebinding to 0.0.0.0 is an explicit,
// warned-about choice (DNS amplification) handled by the listener owners.
const defaultBind = "127.0.0.1"

// Role selects the node's behaviour. Every node runs the same binary.
type Role string

const (
	// RoleAgent hosts services and proxies reads/writes to seeds; it holds no
	// inbound registry.
	RoleAgent Role = "agent"
	// RoleSeed additionally maintains the in-memory registry for the cluster.
	RoleSeed Role = "seed"
)

// Config is the root configuration object.
type Config struct {
	// Role is "agent" or "seed" (default "agent").
	Role Role `yaml:"role"`
	// NodeName is the stable agent identity carried on every RPC. When empty a
	// persisted UUID is used (M4); a stable name is recommended.
	NodeName string `yaml:"node_name"`
	// Datacenter is required by the Consul surfaces (?dc, .dc.consul). Default "dc1".
	Datacenter string `yaml:"datacenter"`
	// DataDir holds persisted state (e.g. node-id). Optional in M0.
	DataDir string `yaml:"data_dir"`
	// ConfigDir is a directory of Consul-compatible service-definition JSON files
	// loaded at start and re-read on SIGHUP (M13). Optional.
	ConfigDir string `yaml:"config_dir"`

	// Cluster describes membership and seed discovery.
	Cluster Cluster `yaml:"cluster"`
	// Listeners configures the addresses/ports of every served surface.
	Listeners Listeners `yaml:"listeners"`

	// MaxServices caps the seed registry size (0 = unlimited); a write past the
	// cap is rejected (write-DoS guard).
	MaxServices int `yaml:"max_services"`
	// ConsulRateLimit caps Consul HTTP requests-per-second per client (0 =
	// unlimited; read+write DoS guard).
	ConsulRateLimit int `yaml:"consul_rate_limit"`
	// TTL is the per-service lease/tombstone window. Default 30s.
	TTL Duration `yaml:"ttl"`
	// HeartbeatInterval is how often the agent renews leases. Default 10s.
	HeartbeatInterval Duration `yaml:"heartbeat_interval"`
	// ShutdownTimeout bounds graceful shutdown. Default 15s.
	ShutdownTimeout Duration `yaml:"shutdown_timeout"`

	// TLS configures transport security (insecure trusted-L3 by default).
	TLS TLS `yaml:"tls"`
	// ACL configures write authorization (disabled by default).
	ACL ACL `yaml:"acl"`
	// Agent tunes the agent's seed fan-out, readiness and drain behaviour.
	Agent Agent `yaml:"agent"`
	// DNS configures the Consul-compatible DNS interface (M12).
	DNS DNSConfig `yaml:"dns"`
	// Federation configures cross-datacenter lookups (M17, v1.x; off by default).
	Federation Federation `yaml:"federation"`
	// Membership configures seed snapshot-on-join + anti-entropy (M18, v1.x; off).
	Membership Membership `yaml:"membership"`
	// Bootstrap is the seed-side policy for the config-bootstrap endpoint.
	Bootstrap Bootstrap `yaml:"bootstrap"`
}

// Bootstrap is the seed-side policy for serving generated agent/seed configs to
// joining nodes (so configs can be refreshed centrally without Ansible/Chef).
// The BootstrapService RPC runs on the existing gRPC server (no extra listener)
// and is served only by a seed. It is SENSITIVE — a config-distribution channel
// — so it is off by default, the served config is sanitized (never carries TLS
// keys or ACL tokens), every request needs a token, and joining as a SEED is
// separately gated. See docs/bootstrap.md for the threat model and controls.
type Bootstrap struct {
	// Enabled registers the BootstrapService on the seed's gRPC server. Default
	// false (no config bootstrap).
	Enabled bool `yaml:"enabled"`
	// SigningKey is the HMAC secret used to sign and verify short-lived bootstrap
	// tokens. It NEVER travels on the wire (only minted tokens do). Prefer
	// SigningKeyFile so the secret is not inline in YAML. One of the two is
	// REQUIRED when bootstrap is enabled. Generate e.g. `openssl rand -base64 48`.
	SigningKey string `yaml:"signing_key"`
	// SigningKeyFile reads the signing key from a file (takes precedence).
	SigningKeyFile string `yaml:"signing_key_file"`
	// TokenTTL is the default lifetime of a minted token (default 30s). Operators
	// run `yp bootstrap create-token` on a seed to mint one within this window.
	TokenTTL Duration `yaml:"token_ttl"`
	// AllowSeedJoin permits a caller to bootstrap as a SEED (join the registry
	// tier). HIGH RISK — a rogue seed serves/harvests the registry. Default false:
	// only agent configs are served. Even when true, a new seed still needs valid
	// credentials and to be listed in peers; bootstrap alone grants no trust.
	AllowSeedJoin bool `yaml:"allow_seed_join"`
	// AdvertiseSeeds is the seed list written into served configs (the reachable
	// seed addresses). Falls back to cluster.seeds when empty.
	AdvertiseSeeds []string `yaml:"advertise_seeds"`
	// RateLimit caps bootstrap requests-per-second per client (default 10).
	RateLimit int `yaml:"rate_limit"`
}

// Federation configures cross-DC lookups (?dc / .dc.consul). Disabled by default.
type Federation struct {
	Enabled bool `yaml:"enabled"`
	// MaxHops bounds federated request depth (loop guard). Default 1.
	MaxHops int `yaml:"max_hops"`
	// Datacenters maps a remote datacenter name to its seed addresses.
	Datacenters map[string][]string `yaml:"datacenters"`
}

// Membership configures the seed tier's self-healing (snapshot-on-join +
// pull-based anti-entropy). Disabled by default.
type Membership struct {
	Enabled bool `yaml:"enabled"`
	// Peers are the other seeds' gRPC addresses to sync from.
	Peers []string `yaml:"peers"`
	// SyncInterval is the anti-entropy pull cadence. Default 30s.
	SyncInterval Duration `yaml:"sync_interval"`
}

// DNSConfig configures the Consul DNS interface (the listener address/port is
// configured under listeners.dns).
type DNSConfig struct {
	// Domain is the served zone (default "consul."; a trailing dot is enforced).
	// Set it to serve your own zone instead of ".consul" (e.g. "mycorp.").
	Domain string `yaml:"domain"`
	// AltDomain is an optional SECOND served zone, answered alongside Domain
	// (Consul's alt_domain). Useful during a cutover: serve both ".consul" and
	// your own domain so existing clients keep working. Empty disables it.
	AltDomain string `yaml:"alt_domain"`
	// ServiceTTL / NodeTTL are the record TTLs in seconds (default 0).
	ServiceTTL Duration `yaml:"service_ttl"`
	NodeTTL    Duration `yaml:"node_ttl"`
	// OnlyPassing drops warning instances too (default keeps warning as passing).
	OnlyPassing bool `yaml:"only_passing"`
	// ARecordLimit caps A/AAAA records per answer (0 = no limit).
	ARecordLimit int `yaml:"a_record_limit"`
	// EnableTruncate sets the TC bit when a UDP answer overflows (default true).
	EnableTruncate bool `yaml:"enable_truncate"`
	// Recursors forward queries outside the served zone (best-effort).
	Recursors []string `yaml:"recursors"`
	// RateLimit caps DNS queries-per-second per client (0 = unlimited; RRL,
	// amplification guard).
	RateLimit int `yaml:"rate_limit"`
}

// Agent tunes the agent role's seed fan-out, readiness gate and drain sequence.
type Agent struct {
	// SeedTimeout bounds a single RPC to one seed during fan-out. Default 3s.
	SeedTimeout Duration `yaml:"seed_timeout"`
	// WriteQuorum is the minimum number of seeds a write must reach to be
	// considered successful (k-of-N). Default 1.
	WriteQuorum int `yaml:"write_quorum"`
	// ReadyMinSeeds is the minimum number of reachable seeds for the agent to
	// report READY. Default 1.
	ReadyMinSeeds int `yaml:"ready_min_seeds"`
	// DrainWindow is the lame-duck window: after readiness goes NOT_SERVING the
	// agent waits this long before it stops accepting and deregisters. Default 5s.
	DrainWindow Duration `yaml:"drain_window"`
	// CacheMaxAge bounds how stale a local read may be before it refetches from
	// the seeds; the cache refresh loop also runs at this cadence. Default 5s.
	CacheMaxAge Duration `yaml:"cache_max_age"`
}

// TLS configures TLS/mTLS transport security, off by default. Enabling it (and
// optionally mutual_tls) turns on TLS/mTLS without any code change.
type TLS struct {
	// Enabled turns on TLS. CertFile/KeyFile are then required.
	Enabled bool `yaml:"enabled"`
	// CertFile and KeyFile are the node's certificate and key (PEM); they
	// hot-reload on rotation without a restart.
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
	// CAFile is the trust anchor for verifying peers (PEM); required for mutual_tls.
	CAFile string `yaml:"ca_file"`
	// MutualTLS requires and verifies client certificates (and presents the node
	// cert when dialing seeds). Identity is then the verified cert subject.
	MutualTLS bool `yaml:"mutual_tls"`
}

// ACL configures write authorization. Disabled by default (anonymous-allow).
type ACL struct {
	// Mode is disabled|allow|enforce. enforce checks write ownership.
	Mode string `yaml:"mode"`
	// DefaultPolicy (allow|deny) only drives a loud migration warning when set to
	// deny while Mode is allow (silent enforcement loss after a Consul cutover).
	DefaultPolicy string `yaml:"default_policy"`
	// TokensFile is a YAML token→principal map; a source of caller identity in
	// enforce mode (alongside mutual TLS).
	TokensFile string `yaml:"tokens_file"`
}

// Cluster groups the cluster name and how seeds are discovered.
type Cluster struct {
	// Name is the cluster name (required).
	Name string `yaml:"name"`
	// Seeds is a static list of seed addresses (host:port).
	Seeds []string `yaml:"seeds"`
	// Discovery, when set, resolves seeds via an external plugin; it merges
	// non-destructively with Seeds (M5).
	Discovery *Discovery `yaml:"discovery"`
}

// Discovery configures plugin-based seed discovery.
type Discovery struct {
	// Name is the plugin executable name/path (required when discovery is set).
	Name string `yaml:"name"`
	// UpdateInterval is how often the plugin is re-run. Default 30s.
	UpdateInterval Duration `yaml:"update_interval"`
	// Options are passed to the plugin as JSON via an environment variable.
	Options map[string]any `yaml:"options"`
}

// Listeners holds the configuration of all served surfaces. Every address/port
// is overridable so yp can co-exist with a real Consul on one host.
type Listeners struct {
	// GRPC is the native discovery.v1 gRPC API (always enabled).
	GRPC Listener `yaml:"grpc"`
	// ConsulHTTP is the Consul-compatible HTTP API (default :8500, off).
	ConsulHTTP Listener `yaml:"consul_http"`
	// DNS is the Consul-compatible DNS interface (default :8600, off).
	DNS Listener `yaml:"dns"`
	// Metrics is the Prometheus /metrics HTTP endpoint (default :9901, off).
	// RPC telemetry is always recorded; this endpoint exposes it when enabled.
	Metrics Listener `yaml:"metrics"`
}

// Listener is a single network listener.
type Listener struct {
	// Enabled turns the listener on. The gRPC listener is always enabled.
	Enabled bool `yaml:"enabled"`
	// Address is the bind address (default 127.0.0.1).
	Address string `yaml:"address"`
	// Port is the TCP/UDP port.
	Port uint16 `yaml:"port"`
}

// Addr returns the host:port form of the listener.
func (l Listener) Addr() string {
	return net.JoinHostPort(l.Address, strconv.Itoa(int(l.Port)))
}

// Load reads, parses, defaults and validates a config file. The extension
// selects nothing about parsing (YAML handles both YAML and JSON); it is only
// used to reject obviously unsupported files.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path comes from a trusted operator flag
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	cfg, err := Parse(data, filepath.Ext(path))
	if err != nil {
		return nil, fmt.Errorf("config %q: %w", path, err)
	}
	return cfg, nil
}

// Parse decodes config bytes (ext is the originating file extension, may be "").
func Parse(data []byte, ext string) (*Config, error) {
	switch strings.ToLower(ext) {
	case "", ".yaml", ".yml", ".json":
	default:
		return nil, fmt.Errorf("unsupported extension %q (want .yaml, .yml or .json)", ext)
	}

	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true) // reject unknown keys: a typo must fail loudly

	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("file is empty")
		}
		return nil, err
	}

	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) applyDefaults() {
	if c.Role == "" {
		c.Role = RoleAgent
	}
	if c.Datacenter == "" {
		c.Datacenter = "dc1"
	}
	if c.TTL == 0 {
		c.TTL = Duration(30 * time.Second)
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = Duration(10 * time.Second)
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = Duration(15 * time.Second)
	}
	if c.Federation.MaxHops == 0 {
		c.Federation.MaxHops = 1
	}
	if c.Membership.SyncInterval == 0 {
		c.Membership.SyncInterval = Duration(30 * time.Second)
	}

	// The native gRPC API is the core surface and is always served.
	c.Listeners.GRPC.Enabled = true
	defaultListener(&c.Listeners.GRPC, 9900)
	defaultListener(&c.Listeners.ConsulHTTP, 8500)
	defaultListener(&c.Listeners.DNS, 8600)
	defaultListener(&c.Listeners.Metrics, 9901)
	if c.Bootstrap.RateLimit == 0 {
		c.Bootstrap.RateLimit = 10
	}
	if c.Bootstrap.TokenTTL == 0 {
		c.Bootstrap.TokenTTL = Duration(30 * time.Second)
	}

	if c.Cluster.Discovery != nil && c.Cluster.Discovery.UpdateInterval == 0 {
		c.Cluster.Discovery.UpdateInterval = Duration(30 * time.Second)
	}

	if c.ACL.Mode == "" {
		c.ACL.Mode = "disabled"
	}

	if c.Agent.SeedTimeout == 0 {
		c.Agent.SeedTimeout = Duration(3 * time.Second)
	}
	if c.Agent.WriteQuorum == 0 {
		c.Agent.WriteQuorum = 1
	}
	if c.Agent.ReadyMinSeeds == 0 {
		c.Agent.ReadyMinSeeds = 1
	}
	if c.Agent.DrainWindow == 0 {
		c.Agent.DrainWindow = Duration(5 * time.Second)
	}
	if c.Agent.CacheMaxAge == 0 {
		c.Agent.CacheMaxAge = Duration(5 * time.Second)
	}

	if c.DNS.Domain == "" {
		c.DNS.Domain = "consul."
	} else if !strings.HasSuffix(c.DNS.Domain, ".") {
		c.DNS.Domain += "."
	}
	if c.DNS.AltDomain != "" && !strings.HasSuffix(c.DNS.AltDomain, ".") {
		c.DNS.AltDomain += "."
	}
	// Truncation defaults on (amplification safety); it is forced rather than
	// configurable-off.
	c.DNS.EnableTruncate = true
}

func defaultListener(l *Listener, port uint16) {
	if l.Address == "" {
		l.Address = defaultBind
	}
	if l.Port == 0 {
		l.Port = port
	}
}

// Validate reports every configuration error at once (joined) so the operator
// can fix them in a single pass.
func (c *Config) Validate() error {
	var errs []error

	switch c.Role {
	case RoleAgent, RoleSeed:
	default:
		errs = append(errs, fmt.Errorf("role: must be %q or %q, got %q", RoleAgent, RoleSeed, c.Role))
	}

	if strings.TrimSpace(c.Cluster.Name) == "" {
		errs = append(errs, errors.New("cluster.name: is required"))
	}

	if c.TTL <= 0 {
		errs = append(errs, errors.New("ttl: must be positive"))
	}
	if c.HeartbeatInterval <= 0 {
		errs = append(errs, errors.New("heartbeat_interval: must be positive"))
	}
	if c.TTL > 0 && c.HeartbeatInterval > 0 && c.HeartbeatInterval >= c.TTL {
		errs = append(errs, fmt.Errorf("heartbeat_interval (%s) must be less than ttl (%s)",
			c.HeartbeatInterval, c.TTL))
	}
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, errors.New("shutdown_timeout: must be positive"))
	}

	errs = append(errs, validateListener("listeners.grpc", c.Listeners.GRPC)...)
	if c.Listeners.ConsulHTTP.Enabled {
		errs = append(errs, validateListener("listeners.consul_http", c.Listeners.ConsulHTTP)...)
	}
	if c.Listeners.DNS.Enabled {
		errs = append(errs, validateListener("listeners.dns", c.Listeners.DNS)...)
	}
	if c.Listeners.Metrics.Enabled {
		errs = append(errs, validateListener("listeners.metrics", c.Listeners.Metrics)...)
	}
	if c.Bootstrap.Enabled {
		if c.Role != RoleSeed {
			errs = append(errs, errors.New("bootstrap: only a seed may serve bootstrap"))
		}
		if strings.TrimSpace(c.Bootstrap.SigningKey) == "" && strings.TrimSpace(c.Bootstrap.SigningKeyFile) == "" {
			errs = append(errs, errors.New("bootstrap: a signing_key or signing_key_file is required when bootstrap is enabled (used to sign short-lived tokens)"))
		}
		if len(c.Bootstrap.AdvertiseSeeds) == 0 && len(c.Cluster.Seeds) == 0 {
			errs = append(errs, errors.New("bootstrap: advertise_seeds or cluster.seeds must be set so served configs can reach a seed"))
		}
		if c.Bootstrap.RateLimit < 0 {
			errs = append(errs, errors.New("bootstrap: rate_limit must be >= 0 (0 applies the default 10/s; a negative value would disable the guard)"))
		}
	}

	if d := c.Cluster.Discovery; d != nil {
		if strings.TrimSpace(d.Name) == "" {
			errs = append(errs, errors.New("cluster.discovery.name: is required when discovery is set"))
		}
		if d.UpdateInterval < 0 {
			errs = append(errs, errors.New("cluster.discovery.update_interval: must not be negative"))
		}
	}

	// An agent needs at least one way to find seeds.
	if c.Role == RoleAgent && len(c.Cluster.Seeds) == 0 && c.Cluster.Discovery == nil {
		errs = append(errs, errors.New("cluster: agent role requires cluster.seeds or cluster.discovery"))
	}

	errs = append(errs, c.validateTLS()...)
	errs = append(errs, c.validateACL()...)
	errs = append(errs, c.validateAgent()...)

	return errors.Join(errs...)
}

func (c *Config) validateAgent() []error {
	var errs []error
	if c.Agent.SeedTimeout <= 0 {
		errs = append(errs, errors.New("agent.seed_timeout: must be positive"))
	}
	if c.Agent.WriteQuorum < 1 {
		errs = append(errs, errors.New("agent.write_quorum: must be at least 1"))
	}
	if c.Agent.ReadyMinSeeds < 1 {
		errs = append(errs, errors.New("agent.ready_min_seeds: must be at least 1"))
	}
	if c.Agent.DrainWindow < 0 {
		errs = append(errs, errors.New("agent.drain_window: must not be negative"))
	}
	if c.Agent.CacheMaxAge <= 0 {
		errs = append(errs, errors.New("agent.cache_max_age: must be positive"))
	}
	return errs
}

func (c *Config) validateTLS() []error {
	var errs []error
	if c.TLS.Enabled {
		if strings.TrimSpace(c.TLS.CertFile) == "" {
			errs = append(errs, errors.New("tls.cert_file: is required when tls is enabled"))
		}
		if strings.TrimSpace(c.TLS.KeyFile) == "" {
			errs = append(errs, errors.New("tls.key_file: is required when tls is enabled"))
		}
		if c.TLS.MutualTLS && strings.TrimSpace(c.TLS.CAFile) == "" {
			errs = append(errs, errors.New("tls.ca_file: is required when tls.mutual_tls is set"))
		}
	}
	if c.TLS.MutualTLS && !c.TLS.Enabled {
		errs = append(errs, errors.New("tls.mutual_tls: requires tls.enabled"))
	}
	return errs
}

func (c *Config) validateACL() []error {
	var errs []error
	switch c.ACL.Mode {
	case "", "disabled", "allow", "enforce":
	default:
		errs = append(errs, fmt.Errorf("acl.mode: must be disabled|allow|enforce, got %q", c.ACL.Mode))
	}
	switch c.ACL.DefaultPolicy {
	case "", "allow", "deny":
	default:
		errs = append(errs, fmt.Errorf("acl.default_policy: must be allow or deny, got %q", c.ACL.DefaultPolicy))
	}
	// enforce needs a way to identify callers, else every write is anonymous and
	// would be denied.
	if c.ACL.Mode == "enforce" && !c.TLS.MutualTLS && strings.TrimSpace(c.ACL.TokensFile) == "" {
		errs = append(errs, errors.New(
			"acl.mode=enforce: requires tls.mutual_tls or acl.tokens_file to identify callers"))
	}
	return errs
}

func validateListener(path string, l Listener) []error {
	var errs []error
	if strings.TrimSpace(l.Address) == "" {
		errs = append(errs, fmt.Errorf("%s.address: is required", path))
	}
	if l.Port == 0 {
		errs = append(errs, fmt.Errorf("%s.port: must be in 1-65535", path))
	}
	return errs
}
