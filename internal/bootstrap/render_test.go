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

package bootstrap

import (
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/ks-tool/yellow-pages/internal/config"
)

func secretSeedConfig() *config.Config {
	return &config.Config{
		Role:              config.RoleSeed,
		Datacenter:        "dc1",
		Cluster:           config.Cluster{Name: "prod", Seeds: []string{"10.0.0.1:9900"}},
		TTL:               config.Duration(30 * time.Second),
		HeartbeatInterval: config.Duration(10 * time.Second),
		ShutdownTimeout:   config.Duration(15 * time.Second),
		MaxServices:       5000,
		DNS:               config.DNSConfig{Domain: "consul.", AltDomain: "mycorp."},
		Agent:             config.Agent{WriteQuorum: 2, ReadyMinSeeds: 1},
		TLS: config.TLS{
			Enabled: true, MutualTLS: true,
			CertFile: "/etc/yp/seed-SECRET.crt", KeyFile: "/etc/yp/seed-SECRET.key", CAFile: "/etc/yp/ca.crt",
		},
		ACL: config.ACL{Mode: "enforce", TokensFile: "/etc/yp/tokens-SECRET.yaml"},
	}
}

func TestRenderNeverLeaksSecrets(t *testing.T) {
	t.Parallel()
	cfg := secretSeedConfig()
	out, err := Render(cfg, config.RoleAgent, []string{"seed-a:9900", "seed-b:9900"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	body := string(out)

	// No secret-bearing VALUES on the wire (the header comment may name the
	// fields as guidance, but no actual path/token is emitted).
	for _, secret := range []string{"seed-SECRET.crt", "seed-SECRET.key", "tokens-SECRET.yaml"} {
		if strings.Contains(body, secret) {
			t.Errorf("rendered config leaked secret value %q:\n%s", secret, body)
		}
	}

	// And the parsed config carries no secret file references at all.
	var probe config.Config
	if err := yaml.Unmarshal(out, &probe); err != nil {
		t.Fatalf("rendered config does not parse: %v", err)
	}
	if probe.TLS.CertFile != "" || probe.TLS.KeyFile != "" || probe.TLS.CAFile != "" {
		t.Errorf("rendered TLS leaked file paths: %+v", probe.TLS)
	}
	if probe.ACL.TokensFile != "" {
		t.Errorf("rendered ACL leaked tokens_file: %q", probe.ACL.TokensFile)
	}

	// But the safe, useful parameters ARE present.
	for _, want := range []string{
		"role: agent", "name: prod", "seed-a:9900", "seed-b:9900",
		"domain: consul.", "alt_domain: mycorp.", "enabled: true", "mode: enforce",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("rendered config missing %q:\n%s", want, body)
		}
	}
}

func TestRenderIsValidLoadableConfig(t *testing.T) {
	t.Parallel()
	out, err := Render(secretSeedConfig(), config.RoleAgent, []string{"seed-a:9900"})
	if err != nil {
		t.Fatal(err)
	}
	// The rendered YAML must parse with strict unknown-field checking — i.e. it
	// only uses real config keys (no typo'd/leaked field).
	var probe config.Config
	dec := yaml.NewDecoder(strings.NewReader(string(out)))
	dec.KnownFields(true)
	if err := dec.Decode(&probe); err != nil {
		t.Fatalf("rendered config is not a strict-valid yp config: %v\n%s", err, out)
	}
	if probe.Role != config.RoleAgent || probe.Cluster.Name != "prod" {
		t.Errorf("decoded config = %+v, want agent/prod", probe)
	}
}

func TestRenderRoleGating(t *testing.T) {
	t.Parallel()
	cfg := secretSeedConfig() // has MaxServices + an Agent block set

	seed := string(render(t, cfg, config.RoleSeed))
	if !strings.Contains(seed, "role: seed") || !strings.Contains(seed, "max_services: 5000") {
		t.Errorf("seed render missing role/max_services:\n%s", seed)
	}
	if strings.Contains(seed, "write_quorum") || strings.Contains(seed, "seed_timeout") {
		t.Errorf("seed render leaked the agent-only block:\n%s", seed)
	}

	agent := string(render(t, cfg, config.RoleAgent))
	if !strings.Contains(agent, "role: agent") || !strings.Contains(agent, "write_quorum") {
		t.Errorf("agent render missing role/agent block:\n%s", agent)
	}
	if strings.Contains(agent, "max_services") {
		t.Errorf("agent render included seed-only max_services:\n%s", agent)
	}
}

func render(t *testing.T, cfg *config.Config, role config.Role) []byte {
	t.Helper()
	b, err := Render(cfg, role, []string{"seed-a:9900"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	return b
}
