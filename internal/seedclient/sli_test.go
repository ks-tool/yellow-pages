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

package seedclient

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/model"
	"github.com/ks-tool/yellow-pages/internal/observability"
	discoveryv1 "github.com/ks-tool/yellow-pages/proto/discovery/v1"
)

func TestPropagationSLIsPopulated(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	prop := observability.NewPropagation(reg)

	addr, _ := startSeed(t, clock.System())
	client := newClient(t, []string{addr}, 2*time.Second)
	client.SetPropagation(prop)
	cache := NewCache(client, CacheOptions{MaxAge: time.Second, Clock: clock.System(), Prop: prop, Log: discardLogger()})
	proxy := NewProxy(ProxyOptions{
		Client: client,
		Node:   model.Node{ID: "agent-1", Datacenter: "dc1"},
		Quorum: 1,
		Cache:  cache,
		Prop:   prop,
		Log:    discardLogger(),
	})

	// Register kicks off the async register-to-visible probe.
	if _, err := proxy.Register(context.Background(), &discoveryv1.RegisterRequest{
		Registration: &discoveryv1.Registration{Services: []*discoveryv1.Service{{Name: "web", TtlSeconds: 30}}, Generation: 1},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// A cache read sets cache-age; a client read sets the clock-skew estimate.
	if _, err := cache.Lookup(context.Background(), model.Query{Name: "web"}); err != nil {
		t.Fatalf("cache Lookup: %v", err)
	}
	if _, err := client.Lookup(context.Background(), model.Query{Name: "web"}); err != nil {
		t.Fatalf("client Lookup: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for histogramSampleCount(t, reg, "yp_propagation_register_to_visible_seconds") == 0 {
		if time.Now().After(deadline) {
			t.Fatal("register_to_visible SLI was not recorded")
		}
		time.Sleep(10 * time.Millisecond)
	}

	for _, name := range []string{"yp_agent_cache_age_seconds", "yp_agent_seed_clock_skew_seconds"} {
		if !metricFamilyExists(t, reg, name) {
			t.Errorf("metric %q is not exposed", name)
		}
	}
}

func histogramSampleCount(t *testing.T, reg *prometheus.Registry, name string) uint64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		var total uint64
		for _, m := range mf.GetMetric() {
			if h := m.GetHistogram(); h != nil {
				total += h.GetSampleCount()
			}
		}
		return total
	}
	return 0
}

func metricFamilyExists(t *testing.T, reg *prometheus.Registry, name string) bool {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			return true
		}
	}
	return false
}
