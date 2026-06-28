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
	"slices"
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

func componentNames(t *testing.T, cfg *config.Config, seeds []string) []string {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	components, err := buildComponents(cfg, observability.NewPrometheus(), clock.System(), testSecurity(), seeds, logger)
	if err != nil {
		t.Fatalf("buildComponents: %v", err)
	}
	names := make([]string, 0, len(components))
	for _, c := range components {
		names = append(names, c.Name())
	}
	return names
}

// TestSeedHoldsRegistryAgentDoesNot verifies the role split: only a seed runs
// the registry (store + GC loop); an agent holds no inbound registry — its
// gRPC server is the local-agent-proxy, fanning out to seeds.
func TestSeedHoldsRegistryAgentDoesNot(t *testing.T) {
	t.Parallel()

	seedCfg, err := config.Parse([]byte("role: seed\ncluster:\n  name: test\n"), ".yaml")
	if err != nil {
		t.Fatalf("parse seed config: %v", err)
	}
	seedNames := componentNames(t, seedCfg, nil)
	if !slices.Contains(seedNames, "grpc-server") || !slices.Contains(seedNames, "store-gc") {
		t.Errorf("seed components %v: expected grpc-server and store-gc (registry)", seedNames)
	}

	agentCfg, err := config.Parse([]byte("role: agent\ncluster:\n  name: test\n  seeds: [\"127.0.0.1:9900\"]\n"), ".yaml")
	if err != nil {
		t.Fatalf("parse agent config: %v", err)
	}
	agentNames := componentNames(t, agentCfg, []string{"127.0.0.1:9900"})
	if slices.Contains(agentNames, "store-gc") {
		t.Errorf("agent components %v: must hold no inbound registry (no store-gc)", agentNames)
	}
}

// TestAgentDrainOrdering verifies the agent component start order yields the
// correct drain on Stop (reverse order): the grpc-server (which flips readiness
// off) stops before the deregistrar, so readiness goes NOT_SERVING before the
// shutdown deregister.
func TestAgentDrainOrdering(t *testing.T) {
	t.Parallel()

	agentCfg, err := config.Parse([]byte("role: agent\ncluster:\n  name: test\n  seeds: [\"127.0.0.1:9900\"]\n"), ".yaml")
	if err != nil {
		t.Fatalf("parse agent config: %v", err)
	}
	names := componentNames(t, agentCfg, []string{"127.0.0.1:9900"})

	deregIdx := slices.Index(names, "deregistrar")
	serverIdx := slices.Index(names, "grpc-server")
	if deregIdx < 0 || serverIdx < 0 {
		t.Fatalf("agent components %v missing deregistrar/grpc-server", names)
	}
	// Start order: deregistrar before grpc-server => Stop order (reverse): the
	// grpc-server (readiness off) stops before the deregistrar (deregister).
	if deregIdx > serverIdx {
		t.Errorf("components %v: deregistrar must start before grpc-server so it stops (deregisters) after readiness goes off", names)
	}
}
