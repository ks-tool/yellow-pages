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

package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParse_DefaultsAndValues(t *testing.T) {
	t.Parallel()

	const src = `
role: seed
node_name: seed-1
cluster:
  name: prod
listeners:
  consul_http:
    enabled: true
    port: 18500
`
	cfg, err := Parse([]byte(src), ".yaml")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if cfg.Role != RoleSeed {
		t.Errorf("Role = %q, want %q", cfg.Role, RoleSeed)
	}
	if cfg.Datacenter != "dc1" {
		t.Errorf("Datacenter default = %q, want dc1", cfg.Datacenter)
	}
	if cfg.TTL.Duration() != 30*time.Second {
		t.Errorf("TTL default = %s, want 30s", cfg.TTL)
	}
	if cfg.HeartbeatInterval.Duration() != 10*time.Second {
		t.Errorf("HeartbeatInterval default = %s, want 10s", cfg.HeartbeatInterval)
	}
	if !cfg.Listeners.GRPC.Enabled {
		t.Error("gRPC listener should always be enabled")
	}
	if got := cfg.Listeners.GRPC.Addr(); got != "127.0.0.1:9900" {
		t.Errorf("gRPC addr = %q, want 127.0.0.1:9900", got)
	}
	if got := cfg.Listeners.ConsulHTTP.Addr(); got != "127.0.0.1:18500" {
		t.Errorf("consul_http addr = %q, want 127.0.0.1:18500", got)
	}
	if cfg.Listeners.DNS.Enabled {
		t.Error("DNS listener should be disabled by default")
	}
}

func TestParse_JSONIsAccepted(t *testing.T) {
	t.Parallel()

	const src = `{"role":"seed","cluster":{"name":"prod"},"ttl":"45s"}`
	cfg, err := Parse([]byte(src), ".json")
	if err != nil {
		t.Fatalf("Parse JSON: %v", err)
	}
	if cfg.TTL.Duration() != 45*time.Second {
		t.Errorf("TTL = %s, want 45s", cfg.TTL)
	}
}

func TestParse_SecurityValid(t *testing.T) {
	t.Parallel()

	// ACL defaults to disabled when unset.
	cfg, err := Parse([]byte("role: seed\ncluster:\n  name: prod\n"), ".yaml")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.ACL.Mode != "disabled" {
		t.Errorf("acl.mode default = %q, want disabled", cfg.ACL.Mode)
	}
	if cfg.TLS.Enabled {
		t.Error("tls should be disabled by default")
	}

	// A valid mTLS + enforce config parses.
	src := `role: seed
cluster:
  name: prod
tls:
  enabled: true
  cert_file: /etc/yp/cert.pem
  key_file: /etc/yp/key.pem
  ca_file: /etc/yp/ca.pem
  mutual_tls: true
acl:
  mode: enforce`
	if _, err := Parse([]byte(src), ".yaml"); err != nil {
		t.Fatalf("valid mTLS+enforce config rejected: %v", err)
	}
}

func TestParse_Errors(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"missing required cluster.name": `role: agent
cluster:
  seeds: ["127.0.0.1:9900"]`,
		"unknown key": `role: agent
cluster:
  name: prod
  seeds: ["127.0.0.1:9900"]
bogus_key: 1`,
		"bad duration": `role: agent
cluster:
  name: prod
  seeds: ["127.0.0.1:9900"]
ttl: thirty-seconds`,
		"invalid role": `role: leader
cluster:
  name: prod`,
		"heartbeat not less than ttl": `role: seed
cluster:
  name: prod
ttl: 10s
heartbeat_interval: 10s`,
		"agent without seeds or discovery": `role: agent
cluster:
  name: prod`,
		"discovery without name": `role: agent
cluster:
  name: prod
  discovery:
    update_interval: 5s`,
		"tls enabled without cert": `role: seed
cluster:
  name: prod
tls:
  enabled: true`,
		"mutual_tls without ca": `role: seed
cluster:
  name: prod
tls:
  enabled: true
  cert_file: /c
  key_file: /k
  mutual_tls: true`,
		"mutual_tls without tls": `role: seed
cluster:
  name: prod
tls:
  mutual_tls: true`,
		"invalid acl mode": `role: seed
cluster:
  name: prod
acl:
  mode: strict`,
		"enforce without identity source": `role: seed
cluster:
  name: prod
acl:
  mode: enforce`,
		"agent negative write_quorum": `role: seed
cluster:
  name: prod
agent:
  write_quorum: -1`,
		"agent negative seed_timeout": `role: seed
cluster:
  name: prod
agent:
  seed_timeout: -5s`,
		"empty": ``,
	}

	for name, src := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := Parse([]byte(src), ".yaml"); err == nil {
				t.Fatalf("Parse(%q) = nil error, want error", name)
			}
		})
	}
}

func TestLoad_FromFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "agent.yaml")
	const src = `role: agent
cluster:
  name: prod
  seeds:
    - 127.0.0.1:9900
`
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Role != RoleAgent {
		t.Errorf("Role = %q, want agent", cfg.Role)
	}
	if cfg.ShutdownTimeout.Duration() != 15*time.Second {
		t.Errorf("ShutdownTimeout default = %s, want 15s", cfg.ShutdownTimeout)
	}
}

func TestLoad_UnsupportedExtension(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "agent.toml")
	if err := os.WriteFile(path, []byte("role = 'agent'"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("Load(.toml) = nil error, want error")
	}
}
