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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestPropagationClusterMetricsExposed(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	p := NewPropagation(reg)

	p.SetRegistrySize(5)
	p.AddEvictions(2)
	p.ObserveFanout(2, 3)
	p.IncWaiters()
	p.CountRequest("consul_http")
	p.CountRequest("dns")
	p.SetDivergence(1)
	p.ObserveRegisterToVisible(10 * time.Millisecond)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	present := map[string]bool{}
	for _, mf := range mfs {
		present[mf.GetName()] = true
	}
	for _, name := range []string{
		"yp_registry_services", "yp_registry_ttl_evictions_total", "yp_agent_seed_fanout_total",
		"yp_consul_blocking_query_waiters", "yp_consul_surface_requests_total", "yp_agent_seed_divergence",
		"yp_propagation_register_to_visible_seconds",
	} {
		if !present[name] {
			t.Errorf("metric %q not exposed", name)
		}
	}
}

// TestMetricCardinalityBounded asserts hot-path series carry only low-cardinality
// labels — never a service name (which would explode cardinality).
func TestMetricCardinalityBounded(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	p := NewPropagation(reg)
	p.ObserveFanout(1, 1)
	p.CountRequest("dns")

	mfs, _ := reg.Gather()
	allowed := map[string]map[string]bool{
		"yp_agent_seed_fanout_total":       {"result": true},
		"yp_consul_surface_requests_total": {"surface": true},
	}
	for _, mf := range mfs {
		labels, watched := allowed[mf.GetName()]
		if !watched {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if !labels[l.GetName()] {
					t.Errorf("%s has unexpected label %q (cardinality risk)", mf.GetName(), l.GetName())
				}
			}
		}
	}
}
