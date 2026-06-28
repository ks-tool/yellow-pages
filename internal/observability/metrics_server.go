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

package observability

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsServer is an app.Component that serves the Prometheus registry over
// HTTP at /metrics. It uses the standard library net/http (no framework) to keep
// the footprint low.
type MetricsServer struct {
	addr string
	log  *slog.Logger
	srv  *http.Server
}

// NewMetricsServer builds the /metrics HTTP component for reg, listening on addr.
func NewMetricsServer(addr string, reg *prometheus.Registry, log *slog.Logger) *MetricsServer {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	return &MetricsServer{
		addr: addr,
		log:  orDefault(log),
		srv: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
	}
}

// Name identifies the component.
func (s *MetricsServer) Name() string { return "metrics-http" }

// Start serves /metrics until ctx is cancelled.
func (s *MetricsServer) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.log.Info("metrics endpoint serving", "addr", lis.Addr().String(), "path", "/metrics")

	errCh := make(chan error, 1)
	go func() {
		if serveErr := s.srv.Serve(lis); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
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

// Stop gracefully shuts the HTTP server down within ctx's deadline.
func (s *MetricsServer) Stop(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}
