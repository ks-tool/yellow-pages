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

	// Cluster describes membership and seed discovery.
	Cluster Cluster `yaml:"cluster"`
	// Listeners configures the addresses/ports of every served surface.
	Listeners Listeners `yaml:"listeners"`

	// TTL is the per-service lease/tombstone window. Default 30s.
	TTL Duration `yaml:"ttl"`
	// HeartbeatInterval is how often the agent renews leases. Default 10s.
	HeartbeatInterval Duration `yaml:"heartbeat_interval"`
	// ShutdownTimeout bounds graceful shutdown. Default 15s.
	ShutdownTimeout Duration `yaml:"shutdown_timeout"`
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

	// The native gRPC API is the core surface and is always served.
	c.Listeners.GRPC.Enabled = true
	defaultListener(&c.Listeners.GRPC, 9900)
	defaultListener(&c.Listeners.ConsulHTTP, 8500)
	defaultListener(&c.Listeners.DNS, 8600)
	defaultListener(&c.Listeners.Metrics, 9901)

	if c.Cluster.Discovery != nil && c.Cluster.Discovery.UpdateInterval == 0 {
		c.Cluster.Discovery.UpdateInterval = Duration(30 * time.Second)
	}
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

	return errors.Join(errs...)
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
