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
	}
	reg.MustRegister(p.registerToVisible, p.deregisterToRemoved, p.cacheAge, p.clockSkew)
	return p
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
