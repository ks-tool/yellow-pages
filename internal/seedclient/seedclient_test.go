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

package seedclient

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/model"
	"github.com/ks-tool/yellow-pages/internal/observability"
	"github.com/ks-tool/yellow-pages/internal/server"
	"github.com/ks-tool/yellow-pages/internal/store"
	"github.com/ks-tool/yellow-pages/internal/transport"
	discoveryv1 "github.com/ks-tool/yellow-pages/proto/discovery/v1"
)

var epoch = time.Unix(1_700_000_000, 0).UTC()

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

const deadAddr = "127.0.0.1:1" // a port nothing listens on

// startSeed starts a real seed gRPC server over an in-memory store on an
// ephemeral port and returns its address and store.
func startSeed(t *testing.T, clk clock.Clock) (string, *store.Memory) {
	t.Helper()
	st := store.NewMemory(store.Options{Clock: clk, DefaultTTL: 30 * time.Second})
	comp := server.NewComponent(server.Options{
		Addr:      "127.0.0.1:0",
		Service:   server.New(st, discardLogger()),
		Transport: transport.Insecure(),
		Log:       discardLogger(),
	})
	return serveComponent(t, comp), st
}

// blockingService is an AgentService whose Register blocks until its context is
// cancelled — a stand-in for a hung seed.
type blockingService struct {
	discoveryv1.UnimplementedAgentServiceServer
}

func (blockingService) Register(ctx context.Context, _ *discoveryv1.RegisterRequest) (*discoveryv1.RegisterResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func startHangingSeed(t *testing.T) string {
	t.Helper()
	comp := server.NewComponent(server.Options{
		Addr:      "127.0.0.1:0",
		Service:   blockingService{},
		Transport: transport.Insecure(),
		Log:       discardLogger(),
	})
	return serveComponent(t, comp)
}

func serveComponent(t *testing.T, comp *server.Component) string {
	t.Helper()
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

func newClient(t *testing.T, seeds []string, timeout time.Duration) *SeedClient {
	t.Helper()
	c, err := New(seeds, transport.Insecure(), timeout, discardLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func modelReg(nodeID, service string) model.Registration {
	return model.Registration{
		Node:       model.Node{ID: nodeID, Datacenter: "dc1"},
		Services:   []model.ServiceInstance{{ID: service, Name: service, TTL: 30 * time.Second}},
		Generation: 1,
	}
}

func present(st *store.Memory, name string) bool {
	return len(st.Lookup(model.Query{Name: name}).Entries) > 0
}

func TestFanOutWriteQuorum(t *testing.T) {
	t.Parallel()
	a1, s1 := startSeed(t, clock.System())
	a2, s2 := startSeed(t, clock.System())
	a3, s3 := startSeed(t, clock.System())

	// 3-of-3: every seed accepts the write.
	all := newClient(t, []string{a1, a2, a3}, 2*time.Second)
	res := all.Register(context.Background(), modelReg("agent-1", "web"))
	if res.Succeeded != 3 || !res.OK(3) {
		t.Fatalf("want 3-of-3, got %+v", res)
	}
	for i, st := range []*store.Memory{s1, s2, s3} {
		if !present(st, "web") {
			t.Errorf("seed %d missing the registration", i)
		}
	}

	// One seed down (dead address): the write still reaches the rest (k-of-N).
	partial := newClient(t, []string{a1, a2, deadAddr}, 2*time.Second)
	res = partial.Register(context.Background(), modelReg("agent-2", "web"))
	if res.Succeeded != 2 {
		t.Fatalf("want 2 successes with one seed down, got %+v", res)
	}
	if !res.OK(1) || res.OK(3) {
		t.Errorf("k-of-N wrong: %+v", res)
	}
	if _, ok := res.Errors[deadAddr]; !ok {
		t.Errorf("dead seed not recorded in errors: %+v", res.Errors)
	}
}

func TestPerSeedDeadlineDoesNotBlockOthers(t *testing.T) {
	t.Parallel()
	a1, s1 := startSeed(t, clock.System())
	a2, s2 := startSeed(t, clock.System())
	hung := startHangingSeed(t)

	client := newClient(t, []string{a1, a2, hung}, 400*time.Millisecond)
	start := time.Now()
	res := client.Register(context.Background(), modelReg("agent-1", "web"))
	elapsed := time.Since(start)

	if res.Succeeded != 2 {
		t.Fatalf("want 2 healthy successes, got %+v", res)
	}
	if elapsed > 2*time.Second {
		t.Errorf("hung seed delayed the others: %s", elapsed)
	}
	if !present(s1, "web") || !present(s2, "web") {
		t.Error("healthy seeds did not get the registration")
	}
}

func TestLookupFanOutMerges(t *testing.T) {
	t.Parallel()
	// Two seeds hold the same service at different generations; the merged read
	// must keep the higher-generation winner.
	a1, s1 := startSeed(t, clock.System())
	a2, s2 := startSeed(t, clock.System())

	stale := modelReg("agent-1", "web")
	stale.Generation = 1
	stale.Services[0].Address = "10.0.0.1"
	fresh := modelReg("agent-1", "web")
	fresh.Generation = 2
	fresh.Services[0].Address = "10.0.0.2"
	if err := s1.Register(stale); err != nil {
		t.Fatal(err)
	}
	if err := s2.Register(fresh); err != nil {
		t.Fatal(err)
	}

	client := newClient(t, []string{a1, a2}, 2*time.Second)
	lr, err := client.Lookup(context.Background(), model.Query{Name: "web"})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(lr.Entries) != 1 {
		t.Fatalf("want 1 merged entry, got %d", len(lr.Entries))
	}
	if lr.Entries[0].Service.Generation != 2 || lr.Entries[0].Service.Address != "10.0.0.2" {
		t.Errorf("merge kept the wrong winner: %+v", lr.Entries[0].Service)
	}
}

func TestRenewMaintainsLease(t *testing.T) {
	t.Parallel()
	seedClock := clock.NewFake(epoch)
	addr, st := startSeed(t, seedClock)
	client := newClient(t, []string{addr}, 2*time.Second)
	proxy := NewProxy(ProxyOptions{Client: client, Node: model.Node{ID: "agent-1", Datacenter: "dc1"}, Quorum: 1, Log: discardLogger()})

	if _, err := proxy.Register(context.Background(), &discoveryv1.RegisterRequest{
		Registration: &discoveryv1.Registration{
			Services:   []*discoveryv1.Service{{Name: "web", TtlSeconds: 30}},
			Generation: 1,
		},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Renewing every 20s (< 30s TTL) keeps the lease alive across the TTL.
	for i := 0; i < 5; i++ {
		seedClock.Advance(20 * time.Second)
		if res := proxy.renewAll(context.Background()); res.Succeeded == 0 {
			t.Fatalf("renew failed at step %d: %+v", i, res.Errors)
		}
		st.GC()
		if !present(st, "web") {
			t.Fatalf("lease expired while being renewed (step %d)", i)
		}
	}

	// Once renews stop, the lease expires past its TTL.
	seedClock.Advance(40 * time.Second)
	st.GC()
	if present(st, "web") {
		t.Error("lease did not expire after renews stopped")
	}
}

func TestRenewLoopComponentRenewsOnTick(t *testing.T) {
	t.Parallel()
	seedClock := clock.NewFake(epoch)
	addr, st := startSeed(t, seedClock)
	client := newClient(t, []string{addr}, 2*time.Second)
	proxy := NewProxy(ProxyOptions{Client: client, Node: model.Node{ID: "agent-1", Datacenter: "dc1"}, Quorum: 1, Log: discardLogger()})
	if _, err := proxy.Register(context.Background(), &discoveryv1.RegisterRequest{
		Registration: &discoveryv1.Registration{Services: []*discoveryv1.Service{{Name: "web", TtlSeconds: 30}}, Generation: 1},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	before := st.Lookup(model.Query{Name: "web"}).Entries[0].Service.LastSeen

	loopClock := clock.NewFake(epoch)
	loop := NewRenewLoop(proxy, 10*time.Second, loopClock, discardLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = loop.Start(ctx) }()

	loopClock.BlockUntil(1)             // wait until the ticker is armed
	seedClock.Advance(5 * time.Second)  // so a renew stamps a later LastSeen
	loopClock.Advance(10 * time.Second) // fire one renew tick

	deadline := time.Now().Add(2 * time.Second)
	for {
		got := st.Lookup(model.Query{Name: "web"}).Entries[0].Service.LastSeen
		if got.After(before) {
			break // the loop renewed
		}
		if time.Now().After(deadline) {
			t.Fatal("renew loop did not renew within the deadline")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestReadinessThresholdKofN(t *testing.T) {
	t.Parallel()
	a1, _ := startSeed(t, clock.System())
	client := newClient(t, []string{a1, deadAddr}, 500*time.Millisecond) // 1 reachable of 2

	status := func(hs *health.Server) healthpb.HealthCheckResponse_ServingStatus {
		resp, err := hs.Check(context.Background(), &healthpb.HealthCheckRequest{})
		if err != nil {
			t.Fatalf("health Check: %v", err)
		}
		return resp.GetStatus()
	}

	// minSeeds=2 with only 1 reachable -> NOT_SERVING.
	hs := health.NewServer()
	NewReadinessProbe(client, observability.NewReadiness(hs, ""), 2, time.Hour, clock.System(), discardLogger()).
		probe(context.Background())
	if got := status(hs); got != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Errorf("minSeeds=2: status = %v, want NOT_SERVING", got)
	}

	// minSeeds=1 with 1 reachable -> SERVING.
	hs2 := health.NewServer()
	NewReadinessProbe(client, observability.NewReadiness(hs2, ""), 1, time.Hour, clock.System(), discardLogger()).
		probe(context.Background())
	if got := status(hs2); got != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("minSeeds=1: status = %v, want SERVING", got)
	}
}

func TestDeregistrarRemovesFromSeeds(t *testing.T) {
	t.Parallel()
	addr, st := startSeed(t, clock.System())
	client := newClient(t, []string{addr}, 2*time.Second)
	proxy := NewProxy(ProxyOptions{Client: client, Node: model.Node{ID: "agent-1", Datacenter: "dc1"}, Quorum: 1, Log: discardLogger()})
	if _, err := proxy.Register(context.Background(), &discoveryv1.RegisterRequest{
		Registration: &discoveryv1.Registration{Services: []*discoveryv1.Service{{Name: "web", TtlSeconds: 30}}, Generation: 1},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if !present(st, "web") {
		t.Fatal("service not registered on seed")
	}

	if err := NewDeregistrar(client, "agent-1", discardLogger()).Stop(context.Background()); err != nil {
		t.Fatalf("Deregistrar.Stop: %v", err)
	}
	if present(st, "web") {
		t.Error("shutdown deregister did not remove the registration from the seed")
	}
}
