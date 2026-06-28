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

// Package observability holds the cross-cutting serving concerns wired in from
// M3: the interceptor chain (recovery, access-log, metrics) for both the server
// and client side, the Prometheus metrics seam, the gRPC health readiness gate
// and the /metrics HTTP endpoint. The interceptors depend on the Metrics
// interface, not on Prometheus directly, so the served surfaces stay decoupled
// from the metrics backend.
package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Side labels which end of an RPC produced an observation.
const (
	SideServer = "server"
	SideClient = "client"
)

// Metrics records RPC-level telemetry. It is the seam between the interceptor
// chain and the metrics backend.
type Metrics interface {
	// ObserveRPC records one completed RPC: which side observed it, the
	// fully-qualified method, the gRPC status code name and the latency.
	ObserveRPC(side, method, code string, latency time.Duration)
}

// Nop is a Metrics that records nothing. It is the default when metrics are off
// and keeps tests that don't assert on telemetry simple.
type Nop struct{}

// ObserveRPC implements Metrics and does nothing.
func (Nop) ObserveRPC(string, string, string, time.Duration) {}

// Prometheus is a Metrics backed by a Prometheus registry. The M3 core records
// RPC rate/latency/error by code on both sides; M14 adds the registry/fan-out
// series and the compat-surface metrics.
type Prometheus struct {
	reg     *prometheus.Registry
	total   *prometheus.CounterVec
	latency *prometheus.HistogramVec
}

// compile-time assertion that *Prometheus satisfies Metrics.
var _ Metrics = (*Prometheus)(nil)

// NewPrometheus builds a Prometheus metrics seam over a fresh registry.
func NewPrometheus() *Prometheus {
	labels := []string{"side", "method", "code"}
	total := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "yp",
		Subsystem: "rpc",
		Name:      "requests_total",
		Help:      "Total RPCs by side, method and gRPC status code.",
	}, labels)
	latency := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "yp",
		Subsystem: "rpc",
		Name:      "latency_seconds",
		Help:      "RPC latency in seconds by side, method and gRPC status code.",
		Buckets:   prometheus.DefBuckets,
	}, labels)

	reg := prometheus.NewRegistry()
	// Baseline runtime/process series so /metrics is useful before any RPC; the
	// process collector is a no-op on platforms that don't support it.
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		total,
		latency,
	)
	return &Prometheus{reg: reg, total: total, latency: latency}
}

// ObserveRPC records the RPC into the counter and latency histogram.
func (p *Prometheus) ObserveRPC(side, method, code string, latency time.Duration) {
	p.total.WithLabelValues(side, method, code).Inc()
	p.latency.WithLabelValues(side, method, code).Observe(latency.Seconds())
}

// Registry exposes the underlying registry for the /metrics handler.
func (p *Prometheus) Registry() *prometheus.Registry { return p.reg }
