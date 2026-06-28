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

package server

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	reflectpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"

	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/model"
	"github.com/ks-tool/yellow-pages/internal/observability"
	"github.com/ks-tool/yellow-pages/internal/store"
	"github.com/ks-tool/yellow-pages/internal/transport"
	discoveryv1 "github.com/ks-tool/yellow-pages/proto/discovery/v1"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// dial starts a Component over st on an ephemeral port and returns a client conn.
func dial(t *testing.T, st store.Store) (*Component, *grpc.ClientConn) {
	t.Helper()
	comp := NewComponent(Options{
		Addr:      "127.0.0.1:0",
		Service:   New(st, testLogger()),
		Transport: transport.Insecure(),
		Metrics:   observability.Nop{},
		Log:       testLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = comp.Start(ctx) }()

	addr := comp.Addr()
	if addr == nil {
		cancel()
		t.Fatal("server failed to bind")
	}
	conn, err := transport.Insecure().Dial(ctx, addr.String())
	if err != nil {
		cancel()
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		sctx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer scancel()
		_ = comp.Stop(sctx)
		cancel()
	})
	return comp, conn
}

func memStore(t *testing.T) *store.Memory {
	t.Helper()
	return store.NewMemory(store.Options{Clock: clock.System(), DefaultTTL: 30 * time.Second})
}

func sampleRegistration() *discoveryv1.Registration {
	return &discoveryv1.Registration{
		Node:       &discoveryv1.Node{Id: "agent-1", Name: "node-a", Address: "10.0.0.5", Datacenter: "dc1"},
		Services:   []*discoveryv1.Service{{Id: "web-1", Name: "web", Address: "10.0.0.5", Port: 8080, TtlSeconds: 30}},
		Generation: 1,
	}
}

func TestRegisterAndLookup(t *testing.T) {
	t.Parallel()
	_, conn := dial(t, memStore(t))
	cli := discoveryv1.NewAgentServiceClient(conn)
	ctx := context.Background()

	if _, err := cli.Register(ctx, &discoveryv1.RegisterRequest{Registration: sampleRegistration()}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	resp, err := cli.Lookup(ctx, &discoveryv1.LookupRequest{Query: &discoveryv1.Query{Name: "web"}})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(resp.GetEntries()) != 1 {
		t.Fatalf("Lookup returned %d entries, want 1", len(resp.GetEntries()))
	}
	e := resp.GetEntries()[0]
	if e.GetNode().GetId() != "agent-1" || e.GetService().GetPort() != 8080 {
		t.Errorf("entry = %s/%d, want agent-1/8080", e.GetNode().GetId(), e.GetService().GetPort())
	}
	if e.GetHealth() != discoveryv1.HealthState_HEALTH_STATE_PASSING {
		t.Errorf("health = %v, want PASSING", e.GetHealth())
	}
}

func TestStatusCodeMapping(t *testing.T) {
	t.Parallel()
	_, conn := dial(t, memStore(t))
	cli := discoveryv1.NewAgentServiceClient(conn)
	ctx := context.Background()

	cases := []struct {
		name string
		call func() error
		want codes.Code
	}{
		{
			name: "renew unknown node -> NotFound",
			call: func() error {
				_, err := cli.Renew(ctx, &discoveryv1.RenewRequest{NodeId: "ghost"})
				return err
			},
			want: codes.NotFound,
		},
		{
			name: "register without node -> InvalidArgument",
			call: func() error {
				_, err := cli.Register(ctx, &discoveryv1.RegisterRequest{
					Registration: &discoveryv1.Registration{Services: []*discoveryv1.Service{{Name: "web"}}},
				})
				return err
			},
			want: codes.InvalidArgument,
		},
		{
			name: "deregister unknown -> NotFound",
			call: func() error {
				_, err := cli.Deregister(ctx, &discoveryv1.DeregisterRequest{NodeId: "ghost"})
				return err
			},
			want: codes.NotFound,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := status.Code(tc.call()); got != tc.want {
				t.Errorf("code = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHealthServing(t *testing.T) {
	t.Parallel()
	_, conn := dial(t, memStore(t))
	hc := healthpb.NewHealthClient(conn)

	resp, err := hc.Check(context.Background(), &healthpb.HealthCheckRequest{Service: discoveryv1.AgentService_ServiceDesc.ServiceName})
	if err != nil {
		t.Fatalf("health Check: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("health = %v, want SERVING", resp.GetStatus())
	}
}

func TestReflectionListsService(t *testing.T) {
	t.Parallel()
	_, conn := dial(t, memStore(t))

	stream, err := reflectpb.NewServerReflectionClient(conn).ServerReflectionInfo(context.Background())
	if err != nil {
		t.Fatalf("reflection stream: %v", err)
	}
	if err := stream.Send(&reflectpb.ServerReflectionRequest{
		MessageRequest: &reflectpb.ServerReflectionRequest_ListServices{ListServices: ""},
	}); err != nil {
		t.Fatalf("reflection send: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("reflection recv: %v", err)
	}

	found := false
	for _, s := range resp.GetListServicesResponse().GetService() {
		if s.GetName() == discoveryv1.AgentService_ServiceDesc.ServiceName {
			found = true
		}
	}
	if !found {
		t.Errorf("reflection did not list %s", discoveryv1.AgentService_ServiceDesc.ServiceName)
	}
}

func TestPanicRecoveryKeepsServerAlive(t *testing.T) {
	t.Parallel()
	_, conn := dial(t, panicOnLookup{memStore(t)})
	cli := discoveryv1.NewAgentServiceClient(conn)
	ctx := context.Background()

	// The panicking handler must surface as Internal, not crash the process.
	_, err := cli.Lookup(ctx, &discoveryv1.LookupRequest{Query: &discoveryv1.Query{Name: "web"}})
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("panic code = %v, want Internal", got)
	}

	// The server is still alive: a subsequent RPC succeeds.
	if _, err := cli.Register(ctx, &discoveryv1.RegisterRequest{Registration: sampleRegistration()}); err != nil {
		t.Fatalf("server died after panic: %v", err)
	}
}

func TestGracefulStopDrainsInflight(t *testing.T) {
	t.Parallel()
	bs := &blockingStore{Store: memStore(t), entered: make(chan struct{}), release: make(chan struct{})}
	comp, conn := dial(t, bs)
	cli := discoveryv1.NewAgentServiceClient(conn)

	errc := make(chan error, 1)
	go func() {
		_, err := cli.Register(context.Background(), &discoveryv1.RegisterRequest{Registration: sampleRegistration()})
		errc <- err
	}()

	<-bs.entered // handler is in-flight

	stopped := make(chan struct{})
	go func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = comp.Stop(sctx)
		close(stopped)
	}()

	close(bs.release) // let the in-flight RPC finish

	if err := <-errc; err != nil {
		t.Errorf("in-flight RPC was aborted by graceful stop: %v", err)
	}
	<-stopped
}

// panicOnLookup wraps a Store and panics on Lookup, leaving other methods intact.
type panicOnLookup struct{ store.Store }

func (panicOnLookup) Lookup(model.Query) model.LookupResult { panic("boom") }

// blockingStore blocks in Register until release is closed, signalling entry on
// the entered channel, to exercise graceful drain of in-flight RPCs.
type blockingStore struct {
	store.Store
	entered  chan struct{}
	release  chan struct{}
	signaled bool
}

func (b *blockingStore) Register(reg model.Registration) error {
	if !b.signaled {
		b.signaled = true
		close(b.entered)
	}
	<-b.release
	return b.Store.Register(reg)
}
