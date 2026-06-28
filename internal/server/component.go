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
	"errors"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/cred"
	"github.com/ks-tool/yellow-pages/internal/observability"
	"github.com/ks-tool/yellow-pages/internal/transport"
	discoveryv1 "github.com/ks-tool/yellow-pages/proto/discovery/v1"
)

// Component serves the native gRPC surface: AgentService, the grpc.health.v1
// health service and server reflection, instrumented with the recovery,
// access-log and metrics interceptor chain. It satisfies app.Component.
type Component struct {
	addr        string
	log         *slog.Logger
	grpc        *grpc.Server
	health      *health.Server
	readiness   *observability.Readiness
	drainWindow time.Duration
	clock       clock.Clock
	startNotRdy bool          // leave NOT_SERVING after Start (external owner gates)
	ready       chan struct{} // closed once the listener is bound (or binding failed)
	lis         net.Listener
}

// Options configures a gRPC server Component.
type Options struct {
	// Addr is the listen address (host:port).
	Addr string
	// Service is the AgentService implementation to serve.
	Service discoveryv1.AgentServiceServer
	// Transport supplies the transport security (insecure or TLS/mTLS).
	Transport transport.Transport
	// Metrics is the RPC metrics seam (defaults to a no-op when nil).
	Metrics observability.Metrics
	// Identity resolves the caller Principal for authz and audit.
	Identity cred.Identity
	// Authz enforces write ownership (defaults to disabled when nil).
	Authz *cred.Authorizer
	// DrainWindow, when > 0, is how long Stop waits after flipping readiness
	// NOT_SERVING before it stops accepting (lame-duck). Default 0 (no wait).
	DrainWindow time.Duration
	// StartNotReady, when true, leaves the server NOT_SERVING after Start so an
	// external owner (e.g. the membership snapshot) drives readiness. Default
	// false: Start marks the server SERVING immediately.
	StartNotReady bool
	// Clock is the time seam used for the drain wait (defaults to System).
	Clock clock.Clock
	// Log is the structured logger.
	Log *slog.Logger
}

// NewComponent assembles the gRPC server from opts. The interceptor chain is, in
// order: recovery+access-log+metrics, write authorization, then audit; followed
// by the AgentService, grpc.health.v1 and reflection.
func NewComponent(opts Options) *Component {
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	authz := opts.Authz
	if authz == nil {
		authz = cred.NewAuthorizer(cred.ModeDisabled)
	}
	t := opts.Transport
	if t == nil {
		t = transport.Insecure()
	}
	clk := opts.Clock
	if clk == nil {
		clk = clock.System()
	}

	gs := t.NewServer(
		grpc.ChainUnaryInterceptor(
			observability.UnaryServerInterceptor(log, opts.Metrics),
			UnaryAuthzInterceptor(opts.Identity, authz, log),
			UnaryAuditInterceptor(opts.Identity, log),
		),
		grpc.ChainStreamInterceptor(observability.StreamServerInterceptor(log, opts.Metrics)),
	)
	discoveryv1.RegisterAgentServiceServer(gs, opts.Service)

	hs := health.NewServer()
	healthpb.RegisterHealthServer(gs, hs)
	reflection.Register(gs)

	// Gate the overall server ("") and the AgentService: NOT_SERVING until Start
	// flips them once the listener is up.
	readiness := observability.NewReadiness(hs, "", discoveryv1.AgentService_ServiceDesc.ServiceName)

	return &Component{
		addr:        opts.Addr,
		log:         log,
		grpc:        gs,
		health:      hs,
		readiness:   readiness,
		drainWindow: opts.DrainWindow,
		clock:       clk,
		startNotRdy: opts.StartNotReady,
		ready:       make(chan struct{}),
	}
}

// Name identifies the component.
func (c *Component) Name() string { return "grpc-server" }

// Readiness returns the server's readiness gate so an owner (e.g. the agent's
// readiness prober) can drive SERVING/NOT_SERVING from seed connectivity.
func (c *Component) Readiness() *observability.Readiness { return c.readiness }

// Addr blocks until the listener has bound and returns its address, or nil if
// binding failed. It is intended for tests that bind on an ephemeral port.
func (c *Component) Addr() net.Addr {
	<-c.ready
	if c.lis == nil {
		return nil
	}
	return c.lis.Addr()
}

// Start binds the listener, marks the server ready (health SERVING) and serves
// until ctx is cancelled. Stop performs the graceful drain.
func (c *Component) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", c.addr)
	if err != nil {
		close(c.ready) // unblock Addr() waiters; lis stays nil
		return err
	}
	c.lis = lis
	// Mark readiness before unblocking Addr() so a concurrent Stop (which flips
	// NOT_SERVING) is always sequenced after this SERVING, never racing it.
	if !c.startNotRdy {
		c.readiness.SetReady(true)
	}
	close(c.ready)
	c.log.Info("gRPC server serving", "addr", lis.Addr().String())
	if c.startNotRdy {
		c.log.Info("gRPC server NOT_SERVING until external readiness gate (membership snapshot)")
	}

	errCh := make(chan error, 1)
	go func() {
		if serveErr := c.grpc.Serve(lis); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			errCh <- serveErr
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}

// Stop flips the server NOT_SERVING, waits the lame-duck drain window (so load
// balancers notice before traffic stops), then gracefully drains in-flight RPCs
// within ctx's deadline, forcing a hard stop if the deadline is exceeded.
func (c *Component) Stop(ctx context.Context) error {
	c.readiness.SetReady(false)

	if c.drainWindow > 0 {
		select {
		case <-c.clock.After(c.drainWindow):
		case <-ctx.Done():
			c.grpc.Stop()
			return ctx.Err()
		}
	}

	done := make(chan struct{})
	go func() {
		c.grpc.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		c.grpc.Stop() // deadline exceeded: abandon in-flight RPCs
		return ctx.Err()
	}
}
