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

package sdk_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/ks-tool/yellow-pages/client/sdk"
	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/server"
	"github.com/ks-tool/yellow-pages/internal/store"
	"github.com/ks-tool/yellow-pages/internal/transport"
	"github.com/ks-tool/yellow-pages/internal/watch"
	discoveryv1 "github.com/ks-tool/yellow-pages/proto/discovery/v1"
)

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// startSeed starts an in-process seed AgentService (with Watch) and returns its
// address. Tests may import internal/ — only the SDK's own code may not.
func startSeed(t *testing.T) string {
	t.Helper()
	w := watch.New(0, clock.System())
	st := store.NewMemory(store.Options{Clock: clock.System(), DefaultTTL: 30 * time.Second, OnChange: w.Notify})
	comp := server.NewComponent(server.Options{
		Addr:      "127.0.0.1:0",
		Service:   server.New(st, discard()).SetWatcher(w),
		Transport: transport.Insecure(),
		Log:       discard(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = comp.Start(ctx) }()
	addr := comp.Addr()
	if addr == nil {
		cancel()
		t.Fatal("seed failed to bind")
	}
	t.Cleanup(func() {
		sctx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer scancel()
		_ = comp.Stop(sctx)
		cancel()
	})
	return addr.String()
}

func registration(nodeID, service string) *discoveryv1.Registration {
	return &discoveryv1.Registration{
		Node:       &discoveryv1.Node{Id: nodeID, Address: "10.0.0.1", Datacenter: "dc1"},
		Services:   []*discoveryv1.Service{{Id: service, Name: service, Address: "10.0.0.1", Port: 8080, TtlSeconds: 30}},
		Generation: 1,
	}
}

func TestRegisterDiscoverIdempotent(t *testing.T) {
	t.Parallel()
	cli, err := sdk.Dial(startSeed(t))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	ctx := context.Background()

	if err := cli.Register(ctx, registration("agent-1", "web")); err != nil {
		t.Fatalf("Register: %v", err)
	}
	// Re-register the same data: idempotent, still one instance.
	if err := cli.Register(ctx, registration("agent-1", "web")); err != nil {
		t.Fatalf("re-Register: %v", err)
	}

	entries, err := cli.Discover(ctx, &discoveryv1.Query{Name: "web"})
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(entries) != 1 || entries[0].GetService().GetName() != "web" {
		t.Fatalf("Discover = %d entries, want 1 web", len(entries))
	}

	if err := cli.DeregisterService(ctx, "agent-1", "web"); err != nil {
		t.Fatalf("DeregisterService: %v", err)
	}
	entries, _ = cli.Discover(ctx, &discoveryv1.Query{Name: "web"})
	if len(entries) != 0 {
		t.Errorf("after deregister Discover = %d, want 0", len(entries))
	}
}

func TestWatchEmitsSnapshots(t *testing.T) {
	t.Parallel()
	cli, err := sdk.Dial(startSeed(t))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates, err := cli.Watch(ctx, &discoveryv1.Query{Name: "web", OnlyHealthy: true})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	if err := cli.Register(ctx, registration("agent-1", "web")); err != nil {
		t.Fatalf("Register: %v", err)
	}
	waitSnapshot(t, updates, 1)

	if err := cli.Deregister(ctx, "agent-1"); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	waitSnapshot(t, updates, 0)
}

func waitSnapshot(t *testing.T, updates <-chan []*discoveryv1.ServiceEntry, wantLen int) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case snap := <-updates:
			if len(snap) == wantLen {
				return
			}
		case <-deadline:
			t.Fatalf("did not observe a snapshot of length %d", wantLen)
		}
	}
}
