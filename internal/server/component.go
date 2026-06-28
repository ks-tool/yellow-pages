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

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/ks-tool/yellow-pages/internal/observability"
	"github.com/ks-tool/yellow-pages/internal/transport"
	discoveryv1 "github.com/ks-tool/yellow-pages/proto/discovery/v1"
)

// Component serves the native gRPC surface: AgentService, the grpc.health.v1
// health service and server reflection, instrumented with the recovery,
// access-log and metrics interceptor chain. It satisfies app.Component.
type Component struct {
	addr      string
	log       *slog.Logger
	grpc      *grpc.Server
	health    *health.Server
	readiness *observability.Readiness
	ready     chan struct{} // closed once the listener is bound (or binding failed)
	lis       net.Listener
}

// NewComponent assembles the gRPC server for svc on addr. The transport supplies
// the security (insecure in M3); metrics and log drive the interceptor chain.
func NewComponent(
	addr string,
	svc discoveryv1.AgentServiceServer,
	t transport.Transport,
	m observability.Metrics,
	log *slog.Logger,
) *Component {
	if log == nil {
		log = slog.Default()
	}

	gs := t.NewServer(
		grpc.ChainUnaryInterceptor(observability.UnaryServerInterceptor(log, m)),
		grpc.ChainStreamInterceptor(observability.StreamServerInterceptor(log, m)),
	)
	discoveryv1.RegisterAgentServiceServer(gs, svc)

	hs := health.NewServer()
	healthpb.RegisterHealthServer(gs, hs)
	reflection.Register(gs)

	// Gate the overall server ("") and the AgentService: NOT_SERVING until Start
	// flips them once the listener is up.
	readiness := observability.NewReadiness(hs, "", discoveryv1.AgentService_ServiceDesc.ServiceName)

	return &Component{addr: addr, log: log, grpc: gs, health: hs, readiness: readiness, ready: make(chan struct{})}
}

// Name identifies the component.
func (c *Component) Name() string { return "grpc-server" }

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
	close(c.ready)
	c.log.Info("gRPC server serving", "addr", lis.Addr().String())
	c.readiness.SetReady(true)

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

// Stop flips the server NOT_SERVING and gracefully drains in-flight RPCs within
// ctx's deadline, forcing a hard stop if the deadline is exceeded.
func (c *Component) Stop(ctx context.Context) error {
	c.readiness.SetReady(false)

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
