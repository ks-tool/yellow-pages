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

package e2e

import (
	"testing"

	"github.com/hashicorp/consul/api"

	"github.com/ks-tool/yellow-pages/internal/migrate"
)

// instances is the shared catalog both yp and Consul are seeded with.
var instances = []*api.CatalogRegistration{
	{Node: "node-a", Address: "10.1.0.1", Datacenter: "dc1",
		Service: &api.AgentService{ID: "web-1", Service: "web", Address: "10.1.0.1", Port: 8080, Tags: []string{"v1", "edge"}}},
	{Node: "node-a", Address: "10.1.0.1", Datacenter: "dc1",
		Service: &api.AgentService{ID: "web-2", Service: "web", Address: "10.1.0.2", Port: 8080, Tags: []string{"v2"}}},
	{Node: "node-b", Address: "10.1.0.3", Datacenter: "dc1",
		Service: &api.AgentService{ID: "api-1", Service: "api", Address: "10.1.0.3", Port: 9090}},
}

func mustClient(t *testing.T, addr string) *api.Client {
	t.Helper()
	c, err := api.NewClient(&api.Config{Address: addr})
	if err != nil {
		t.Fatalf("api client for %s: %v", addr, err)
	}
	return c
}

func seedCatalog(t *testing.T, c *api.Client) {
	t.Helper()
	for _, r := range instances {
		if _, err := c.Catalog().Register(r, nil); err != nil {
			t.Fatalf("register %s on %s: %v", r.Service.ID, r.Node, err)
		}
	}
}

// shadowOf maps consul/api health entries to the normalized shadow set used by
// the diff (ignoring index/timestamps/order/LastContact).
func shadowOf(entries []*api.ServiceEntry) []migrate.ShadowEntry {
	out := make([]migrate.ShadowEntry, 0, len(entries))
	for _, e := range entries {
		addr := e.Service.Address
		if addr == "" {
			addr = e.Node.Address
		}
		out = append(out, migrate.ShadowEntry{
			Node: e.Node.Node, ServiceID: e.Service.ID, Address: addr,
			Port: e.Service.Port, Tags: e.Service.Tags, Status: e.Checks.AggregatedStatus(),
		})
	}
	return out
}

// TestHealthServiceConformance proves yp's /v1/health/service is byte-equivalent
// (after normalization) to real Consul for the same catalog, read through the
// real consul/api client against BOTH.
func TestHealthServiceConformance(t *testing.T) {
	consulAddr := startConsul(t)
	yp := startYPSeed(t)

	consul := mustClient(t, consulAddr)
	ypc := mustClient(t, yp.consulHTTP)
	seedCatalog(t, consul)
	seedCatalog(t, ypc)

	for _, svc := range []string{"web", "api"} {
		cEntries, _, err := consul.Health().Service(svc, "", false, nil)
		if err != nil {
			t.Fatalf("consul health %s: %v", svc, err)
		}
		ypEntries, _, err := ypc.Health().Service(svc, "", false, nil)
		if err != nil {
			t.Fatalf("yp health %s: %v", svc, err)
		}
		if diff := migrate.ShadowDiff(shadowOf(cEntries), shadowOf(ypEntries)); !diff.Empty() {
			t.Errorf("health/service/%s diverges from Consul:\n only-consul: %+v\n only-yp:     %+v",
				svc, diff.OnlyLeft, diff.OnlyRight)
		}
	}
}

// TestCatalogServicesConformance compares the service->tags catalog map.
func TestCatalogServicesConformance(t *testing.T) {
	consulAddr := startConsul(t)
	yp := startYPSeed(t)

	consul := mustClient(t, consulAddr)
	ypc := mustClient(t, yp.consulHTTP)
	seedCatalog(t, consul)
	seedCatalog(t, ypc)

	cSvcs, _, err := consul.Catalog().Services(nil)
	if err != nil {
		t.Fatalf("consul catalog services: %v", err)
	}
	ypSvcs, _, err := ypc.Catalog().Services(nil)
	if err != nil {
		t.Fatalf("yp catalog services: %v", err)
	}
	// yp must contain every app service Consul has (Consul also lists its own
	// "consul" service, which yp has no equivalent for — allow that extra).
	for name := range cSvcs {
		if name == "consul" {
			continue
		}
		if _, ok := ypSvcs[name]; !ok {
			t.Errorf("yp catalog missing service %q present in Consul", name)
		}
	}
	for _, want := range []string{"web", "api"} {
		if _, ok := ypSvcs[want]; !ok {
			t.Errorf("yp catalog missing %q", want)
		}
	}
}

// TestBlockingQueryHandover exercises a Consul blocking query (WaitIndex) against
// yp: the first read returns an index, a second blocking read with that index
// blocks until a change bumps it — no busy-loop.
func TestBlockingQueryHandover(t *testing.T) {
	yp := startYPSeed(t)
	ypc := mustClient(t, yp.consulHTTP)
	seedCatalog(t, ypc)

	_, meta, err := ypc.Health().Service("web", "", false, nil)
	if err != nil {
		t.Fatalf("initial read: %v", err)
	}
	if meta.LastIndex == 0 {
		t.Fatal("expected a non-zero X-Consul-Index")
	}

	done := make(chan uint64, 1)
	go func() {
		_, m, berr := ypc.Health().Service("web", "", false,
			&api.QueryOptions{WaitIndex: meta.LastIndex, WaitTime: 5_000_000_000})
		if berr != nil {
			done <- 0
			return
		}
		done <- m.LastIndex
	}()

	// Register a new instance to bump the index and release the blocking read.
	if _, err := ypc.Catalog().Register(&api.CatalogRegistration{
		Node: "node-c", Address: "10.1.0.9", Datacenter: "dc1",
		Service: &api.AgentService{ID: "web-3", Service: "web", Address: "10.1.0.9", Port: 8080},
	}, nil); err != nil {
		t.Fatalf("register to bump index: %v", err)
	}

	newIndex := <-done
	if newIndex <= meta.LastIndex {
		t.Errorf("blocking query did not advance index: got %d, was %d", newIndex, meta.LastIndex)
	}
}
