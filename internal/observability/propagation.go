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
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Propagation holds the agent's AP-consistency SLIs: how long a registration
// takes to become visible across the cluster and to disappear after a
// deregister, the age of the read cache it serves, and an estimate of the
// clock-skew between seeds. All methods are nil-safe so callers can hold a nil
// *Propagation when metrics are disabled.
type Propagation struct {
	registerToVisible   prometheus.Histogram
	deregisterToRemoved prometheus.Histogram
	cacheAge            prometheus.Gauge
	clockSkew           prometheus.Gauge

	// Cluster SLIs (M14). Labels are deliberately low-cardinality — never a
	// service name — to avoid label explosion on hot paths.
	registrySize    prometheus.Gauge
	evictions       prometheus.Counter
	fanout          *prometheus.CounterVec // {result: success|failure}
	blockingWaiters prometheus.Gauge
	surfaceRequests *prometheus.CounterVec // {surface: consul_http|dns}
	divergence      prometheus.Gauge
	convergenceLag  prometheus.Gauge
}

// NewPropagation registers the propagation SLIs into reg.
func NewPropagation(reg *prometheus.Registry) *Propagation {
	p := &Propagation{
		registerToVisible: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "yp", Subsystem: "propagation", Name: "register_to_visible_seconds",
			Help:    "Seconds from registering a service to it becoming visible via a cluster lookup.",
			Buckets: prometheus.DefBuckets,
		}),
		deregisterToRemoved: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "yp", Subsystem: "propagation", Name: "deregister_to_removed_seconds",
			Help:    "Seconds from deregistering a service to it disappearing from a cluster lookup.",
			Buckets: prometheus.DefBuckets,
		}),
		cacheAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "yp", Subsystem: "agent", Name: "cache_age_seconds",
			Help: "Age of the most recently served agent read-cache entry.",
		}),
		clockSkew: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "yp", Subsystem: "agent", Name: "seed_clock_skew_seconds",
			Help: "Estimated clock skew between seeds (spread of server-stamped last_seen for one registration).",
		}),
		registrySize: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "yp", Subsystem: "registry", Name: "services",
			Help: "Number of service instances in the seed registry.",
		}),
		evictions: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "yp", Subsystem: "registry", Name: "ttl_evictions_total",
			Help: "Total service instances reaped by GC after TTL+grace.",
		}),
		fanout: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "yp", Subsystem: "agent", Name: "seed_fanout_total",
			Help: "Per-seed fan-out attempts by result.",
		}, []string{"result"}),
		blockingWaiters: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "yp", Subsystem: "consul", Name: "blocking_query_waiters",
			Help: "Current number of in-flight Consul blocking queries.",
		}),
		surfaceRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "yp", Subsystem: "consul", Name: "surface_requests_total",
			Help: "Requests to the Consul-compatible surfaces.",
		}, []string{"surface"}),
		divergence: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "yp", Subsystem: "agent", Name: "seed_divergence",
			Help: "Spread in instance count returned by seeds for the last read (per-seed divergence).",
		}),
		convergenceLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "yp", Subsystem: "seed", Name: "convergence_lag",
			Help: "Entries applied by the last anti-entropy pass (0 once seeds have converged).",
		}),
	}
	reg.MustRegister(
		p.registerToVisible, p.deregisterToRemoved, p.cacheAge, p.clockSkew,
		p.registrySize, p.evictions, p.fanout, p.blockingWaiters, p.surfaceRequests,
		p.divergence, p.convergenceLag,
	)
	return p
}

// SetConvergenceLag records how many entries the last anti-entropy pass applied
// (0 means the seed has converged with its peers).
func (p *Propagation) SetConvergenceLag(n int) {
	if p != nil {
		p.convergenceLag.Set(float64(n))
	}
}

// SetRegistrySize records the seed registry size.
func (p *Propagation) SetRegistrySize(n int) {
	if p != nil {
		p.registrySize.Set(float64(n))
	}
}

// AddEvictions records GC evictions.
func (p *Propagation) AddEvictions(n int) {
	if p != nil && n > 0 {
		p.evictions.Add(float64(n))
	}
}

// ObserveFanout records a write fan-out's per-seed successes and failures.
func (p *Propagation) ObserveFanout(succeeded, total int) {
	if p == nil {
		return
	}
	p.fanout.WithLabelValues("success").Add(float64(succeeded))
	if f := total - succeeded; f > 0 {
		p.fanout.WithLabelValues("failure").Add(float64(f))
	}
}

// IncWaiters / DecWaiters track in-flight blocking queries.
func (p *Propagation) IncWaiters() {
	if p != nil {
		p.blockingWaiters.Inc()
	}
}

// DecWaiters decrements the in-flight blocking-query gauge.
func (p *Propagation) DecWaiters() {
	if p != nil {
		p.blockingWaiters.Dec()
	}
}

// CountRequest records a request to a Consul surface (consul_http | dns).
func (p *Propagation) CountRequest(surface string) {
	if p != nil {
		p.surfaceRequests.WithLabelValues(surface).Inc()
	}
}

// SetDivergence records the per-seed divergence of the last read.
func (p *Propagation) SetDivergence(spread int) {
	if p != nil {
		p.divergence.Set(float64(spread))
	}
}

// ObserveRegisterToVisible records a register-to-visible latency.
func (p *Propagation) ObserveRegisterToVisible(d time.Duration) {
	if p != nil {
		p.registerToVisible.Observe(d.Seconds())
	}
}

// ObserveDeregisterToRemoved records a deregister-to-removed latency.
func (p *Propagation) ObserveDeregisterToRemoved(d time.Duration) {
	if p != nil {
		p.deregisterToRemoved.Observe(d.Seconds())
	}
}

// SetCacheAge records the age of the cache entry just served.
func (p *Propagation) SetCacheAge(d time.Duration) {
	if p != nil {
		p.cacheAge.Set(d.Seconds())
	}
}

// SetClockSkew records the estimated seed clock skew.
func (p *Propagation) SetClockSkew(d time.Duration) {
	if p != nil {
		p.clockSkew.Set(d.Seconds())
	}
}
