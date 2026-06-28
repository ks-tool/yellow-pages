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
	"bytes"
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// recorder is a Metrics that captures observations for assertions.
type recorder struct {
	mu    sync.Mutex
	calls []string
}

func (r *recorder) ObserveRPC(side, method, code string, _ time.Duration) {
	r.mu.Lock()
	r.calls = append(r.calls, side+" "+method+" "+code)
	r.mu.Unlock()
}

func (r *recorder) last() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.calls) == 0 {
		return ""
	}
	return r.calls[len(r.calls)-1]
}

func TestPrometheusMetricsByCode(t *testing.T) {
	t.Parallel()
	p := NewPrometheus()
	p.ObserveRPC(SideServer, "/discovery.v1.AgentService/Lookup", codes.OK.String(), 2*time.Millisecond)
	p.ObserveRPC(SideServer, "/discovery.v1.AgentService/Renew", codes.NotFound.String(), time.Millisecond)

	rr := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	promhttp.HandlerFor(p.Registry(), promhttp.HandlerOpts{}).ServeHTTP(w, rr)

	body := w.Body.String()
	if w.Code != http.StatusOK {
		t.Fatalf("/metrics status = %d", w.Code)
	}
	for _, want := range []string{
		`yp_rpc_requests_total{`,
		`code="OK"`,
		`code="NotFound"`,
		`yp_rpc_latency_seconds_bucket{`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics body missing %q", want)
		}
	}
}

func TestUnaryServerInterceptorRecordsOK(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))
	rec := &recorder{}
	intc := UnaryServerInterceptor(log, rec)

	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 5555}})
	handler := func(context.Context, any) (any, error) { return "ok", nil }
	resp, err := intc(ctx, "req", &grpc.UnaryServerInfo{FullMethod: "/svc/M"}, handler)
	if err != nil || resp != "ok" {
		t.Fatalf("handler result = %v, %v", resp, err)
	}

	if got := rec.last(); got != "server /svc/M OK" {
		t.Errorf("metric = %q, want %q", got, "server /svc/M OK")
	}
	logLine := buf.String()
	for _, want := range []string{`"code":"OK"`, `"peer":"10.0.0.1:5555"`, `"latency"`, `"method":"/svc/M"`} {
		if !strings.Contains(logLine, want) {
			t.Errorf("access-log missing %q in %s", want, logLine)
		}
	}
}

func TestUnaryServerInterceptorRecoversPanic(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))
	rec := &recorder{}
	intc := UnaryServerInterceptor(log, rec)

	handler := func(context.Context, any) (any, error) { panic("boom") }
	resp, err := intc(context.Background(), "req", &grpc.UnaryServerInfo{FullMethod: "/svc/Boom"}, handler)

	if resp != nil {
		t.Errorf("resp = %v, want nil after panic", resp)
	}
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("panic code = %v, want Internal", got)
	}
	if got := rec.last(); got != "server /svc/Boom Internal" {
		t.Errorf("metric = %q, want server /svc/Boom Internal", got)
	}
	if !strings.Contains(buf.String(), "panic recovered") {
		t.Errorf("panic was not logged: %s", buf.String())
	}
}

func TestReadinessGate(t *testing.T) {
	t.Parallel()
	hs := health.NewServer()
	const svc = "discovery.v1.AgentService"
	r := NewReadiness(hs, "", svc)

	check := func() healthpb.HealthCheckResponse_ServingStatus {
		resp, err := hs.Check(context.Background(), &healthpb.HealthCheckRequest{Service: svc})
		if err != nil {
			t.Fatalf("health Check: %v", err)
		}
		return resp.GetStatus()
	}

	if got := check(); got != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Errorf("initial = %v, want NOT_SERVING", got)
	}
	r.SetReady(true)
	if got := check(); got != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("after ready = %v, want SERVING", got)
	}
	r.SetReady(false)
	if got := check(); got != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Errorf("after drain = %v, want NOT_SERVING", got)
	}
}
