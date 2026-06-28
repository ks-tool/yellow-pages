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

package main

import (
	"io"
	"log/slog"
	"testing"

	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/config"
	"github.com/ks-tool/yellow-pages/internal/cred"
	"github.com/ks-tool/yellow-pages/internal/observability"
)

func testSecurity() security {
	return security{
		nodeID:   "test-node",
		creds:    cred.Insecure(),
		identity: cred.NewIdentity(nil),
		authz:    cred.NewAuthorizer(cred.ModeDisabled),
	}
}

func componentNames(cfg *config.Config) []string {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	components := buildComponents(cfg, observability.NewPrometheus(), clock.System(), testSecurity(), logger)
	names := make([]string, 0, len(components))
	for _, c := range components {
		names = append(names, c.Name())
	}
	return names
}

func has(names []string, want string) bool {
	for _, n := range names {
		if n == want {
			return true
		}
	}
	return false
}

// TestRoleDeterminesRegistryListener verifies that only a seed opens the inbound
// gRPC registry server; an agent holds no inbound registry.
func TestRoleDeterminesRegistryListener(t *testing.T) {
	t.Parallel()

	seedCfg, err := config.Parse([]byte("role: seed\ncluster:\n  name: test\n"), ".yaml")
	if err != nil {
		t.Fatalf("parse seed config: %v", err)
	}
	if names := componentNames(seedCfg); !has(names, "grpc-server") {
		t.Errorf("seed components %v: expected a grpc-server", names)
	}

	agentCfg, err := config.Parse([]byte("role: agent\ncluster:\n  name: test\n  seeds: [\"127.0.0.1:9900\"]\n"), ".yaml")
	if err != nil {
		t.Fatalf("parse agent config: %v", err)
	}
	if names := componentNames(agentCfg); has(names, "grpc-server") {
		t.Errorf("agent components %v: must NOT open the inbound registry server", names)
	}
}
