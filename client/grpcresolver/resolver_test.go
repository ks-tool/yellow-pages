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

package grpcresolver_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/resolver"

	"github.com/ks-tool/yellow-pages/client/grpcresolver"
	"github.com/ks-tool/yellow-pages/client/sdk"
	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/server"
	"github.com/ks-tool/yellow-pages/internal/store"
	"github.com/ks-tool/yellow-pages/internal/transport"
	"github.com/ks-tool/yellow-pages/internal/watch"
	discoveryv1 "github.com/ks-tool/yellow-pages/proto/discovery/v1"
)

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

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

// startBackend starts a plain gRPC server exposing only the health service.
func startBackend(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(s, hs)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}

func TestResolverReachesAndRemovesInstance(t *testing.T) {
	grpcresolver.Register(grpc.WithTransportCredentials(insecure.NewCredentials()))

	backend := startBackend(t)
	host, portStr, _ := net.SplitHostPort(backend)
	port, _ := strconv.Atoi(portStr)

	seedAddr := startSeed(t)
	agent, err := sdk.Dial(seedAddr)
	if err != nil {
		t.Fatalf("Dial agent: %v", err)
	}
	t.Cleanup(func() { _ = agent.Close() })

	// Register the backend instance through the agent.
	if err := agent.Register(context.Background(), &discoveryv1.Registration{
		Node:       &discoveryv1.Node{Id: "node-b", Address: host, Datacenter: "dc1"},
		Services:   []*discoveryv1.Service{{Id: "backend", Name: "backend", Address: host, Port: uint32(port), TtlSeconds: 30}},
		Generation: 1,
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	conn, err := grpc.NewClient("yp://"+seedAddr+"/backend", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	hc := healthpb.NewHealthClient(conn)

	// The resolver discovers the live instance and the RPC reaches the backend.
	waitUntil(t, 5*time.Second, func() bool {
		resp, herr := hc.Check(context.Background(), &healthpb.HealthCheckRequest{})
		return herr == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING
	}, "resolver did not reach the live instance")

	// Deregister: the resolver drops the address within the wait window.
	if err := agent.DeregisterService(context.Background(), "node-b", "backend"); err != nil {
		t.Fatalf("DeregisterService: %v", err)
	}
	waitUntil(t, 5*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		_, herr := hc.Check(ctx, &healthpb.HealthCheckRequest{})
		return herr != nil // no addresses left -> RPC fails
	}, "resolver did not remove the deregistered instance")
}

func TestWeightAttachedToAddress(t *testing.T) {
	t.Parallel()
	a := resolver.Address{Addr: "10.0.0.1:8080"}
	if got := grpcresolver.Weight(a); got != 1 {
		t.Errorf("default weight = %d, want 1", got)
	}
}

func waitUntil(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal(msg)
}
