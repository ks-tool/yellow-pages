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

package store

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/model"
)

var epoch = time.Unix(1_700_000_000, 0).UTC()

func svc(id, name string, port uint16, ttl time.Duration) model.ServiceInstance {
	return model.ServiceInstance{ID: id, Name: name, Address: "10.0.0.1", Port: port, TTL: ttl}
}

func reg(nodeID string, gen uint64, svcs ...model.ServiceInstance) model.Registration {
	return model.Registration{
		Node:       model.Node{ID: nodeID, Name: nodeID, Address: "10.0.0.1", Datacenter: "dc1"},
		Services:   svcs,
		Generation: gen,
	}
}

func newStore(t *testing.T, opts Options) (*Memory, *clock.Fake) {
	t.Helper()
	f := clock.NewFake(epoch)
	opts.Clock = f
	return NewMemory(opts), f
}

func lookupOne(t *testing.T, s *Memory, name string) (model.ServiceEntry, bool) {
	t.Helper()
	res := s.Lookup(model.Query{Name: name})
	if len(res.Entries) == 0 {
		return model.ServiceEntry{}, false
	}
	if len(res.Entries) != 1 {
		t.Fatalf("Lookup(%q): want 1 entry, got %d", name, len(res.Entries))
	}
	return res.Entries[0], true
}

func TestRegisterLookupDeregister(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t, Options{})

	if err := s.Register(reg("n1", 1, svc("web", "web", 8080, 30*time.Second), svc("api", "api", 9090, 30*time.Second))); err != nil {
		t.Fatalf("Register: %v", err)
	}

	web, ok := lookupOne(t, s, "web")
	if !ok {
		t.Fatal("web not found after register")
	}
	if web.Service.Port != 8080 || web.Service.Generation != 1 || web.Health != model.HealthPassing {
		t.Errorf("web = %+v, want port 8080 gen 1 passing", web.Service)
	}
	if web.CreateIndex == 0 || web.ModifyIndex == 0 {
		t.Errorf("web indexes = create %d modify %d, want both >0", web.CreateIndex, web.ModifyIndex)
	}
	if _, ok := lookupOne(t, s, "api"); !ok {
		t.Fatal("api not found after register")
	}

	if err := s.DeregisterService("n1", "web"); err != nil {
		t.Fatalf("DeregisterService: %v", err)
	}
	if _, ok := lookupOne(t, s, "web"); ok {
		t.Error("web still present after DeregisterService")
	}
	if _, ok := lookupOne(t, s, "api"); !ok {
		t.Error("api removed by deregistering web (per-service isolation broken)")
	}

	if err := s.Deregister("n1"); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	if _, ok := lookupOne(t, s, "api"); ok {
		t.Error("api still present after Deregister")
	}
}

func TestServiceIDDefaultsToName(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t, Options{})

	if err := s.Register(reg("n1", 1, model.ServiceInstance{Name: "web", Port: 80, TTL: 30 * time.Second})); err != nil {
		t.Fatalf("Register: %v", err)
	}
	e, ok := lookupOne(t, s, "web")
	if !ok || e.Service.ID != "web" {
		t.Errorf("service id = %q, want defaulted to name %q", e.Service.ID, "web")
	}
}

func TestTTLClamp(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t, Options{MinTTL: 10 * time.Second, MaxTTL: 60 * time.Second, DefaultTTL: 30 * time.Second})

	if err := s.Register(reg("n1", 1,
		svc("low", "low", 1, 2*time.Second),   // below min -> 10s
		svc("high", "high", 2, 5*time.Minute), // above max -> 60s
		svc("zero", "zero", 3, 0),             // unset -> default 30s
	)); err != nil {
		t.Fatalf("Register: %v", err)
	}

	for name, want := range map[string]time.Duration{
		"low":  10 * time.Second,
		"high": 60 * time.Second,
		"zero": 30 * time.Second,
	} {
		e, _ := lookupOne(t, s, name)
		if e.Service.TTL != want {
			t.Errorf("%s TTL = %s, want %s", name, e.Service.TTL, want)
		}
	}
}

func TestRenewKeepsGenerationAndIndex(t *testing.T) {
	t.Parallel()
	s, f := newStore(t, Options{})

	if err := s.Register(reg("n1", 7, svc("web", "web", 8080, 30*time.Second))); err != nil {
		t.Fatalf("Register: %v", err)
	}
	idxAfterRegister := s.Index()

	f.Advance(5 * time.Second)
	if err := s.Renew("n1", nil); err != nil {
		t.Fatalf("Renew: %v", err)
	}

	if got := s.Index(); got != idxAfterRegister {
		t.Errorf("index moved on renew: %d -> %d", idxAfterRegister, got)
	}
	e, _ := lookupOne(t, s, "web")
	if e.Service.Generation != 7 {
		t.Errorf("generation changed on renew: %d", e.Service.Generation)
	}
	if !e.Service.LastSeen.Equal(epoch.Add(5 * time.Second)) {
		t.Errorf("last_seen = %s, want %s (server-stamped)", e.Service.LastSeen, epoch.Add(5*time.Second))
	}
}

func TestReRegisterEndpointChangeBumpsIndex(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t, Options{})

	if err := s.Register(reg("n1", 1, svc("web", "web", 8080, 30*time.Second))); err != nil {
		t.Fatalf("Register: %v", err)
	}
	before, _ := lookupOne(t, s, "web")
	idx1 := s.Index()

	// Endpoint change with a bumped generation must advance ModifyIndex while
	// keeping CreateIndex.
	if err := s.Register(reg("n1", 2, svc("web", "web", 9090, 30*time.Second))); err != nil {
		t.Fatalf("re-Register: %v", err)
	}
	after, _ := lookupOne(t, s, "web")
	if after.Service.Port != 9090 || after.Service.Generation != 2 {
		t.Errorf("after re-register = port %d gen %d, want 9090/2", after.Service.Port, after.Service.Generation)
	}
	if after.ModifyIndex <= before.ModifyIndex {
		t.Errorf("ModifyIndex did not advance on endpoint change: %d -> %d", before.ModifyIndex, after.ModifyIndex)
	}
	if after.CreateIndex != before.CreateIndex {
		t.Errorf("CreateIndex changed on update: %d -> %d", before.CreateIndex, after.CreateIndex)
	}
	if s.Index() <= idx1 {
		t.Errorf("registry index did not advance: %d -> %d", idx1, s.Index())
	}

	// Idempotent re-register with identical data must NOT advance the index.
	idx2 := s.Index()
	if err := s.Register(reg("n1", 2, svc("web", "web", 9090, 30*time.Second))); err != nil {
		t.Fatalf("idempotent re-Register: %v", err)
	}
	if s.Index() != idx2 {
		t.Errorf("idempotent re-register moved the index: %d -> %d", idx2, s.Index())
	}
}

func TestPerServiceExpiryIsolated(t *testing.T) {
	t.Parallel()
	s, f := newStore(t, Options{}) // grace 0

	if err := s.Register(reg("n1", 1,
		svc("web", "web", 8080, 10*time.Second),
		svc("api", "api", 9090, 30*time.Second),
	)); err != nil {
		t.Fatalf("Register: %v", err)
	}

	f.Advance(11 * time.Second) // web expired, api still alive
	if removed := s.GC(); removed != 1 {
		t.Errorf("GC removed %d, want 1", removed)
	}
	if _, ok := lookupOne(t, s, "web"); ok {
		t.Error("expired web still present")
	}
	api, ok := lookupOne(t, s, "api")
	if !ok || api.Health != model.HealthPassing {
		t.Errorf("neighbour api affected by web expiry: ok=%v health=%v", ok, api.Health)
	}
}

func TestGraceWindowKeepsRecentlyCriticalVisible(t *testing.T) {
	t.Parallel()
	s, f := newStore(t, Options{GracePeriod: 20 * time.Second})

	if err := s.Register(reg("n1", 1, svc("web", "web", 8080, 10*time.Second))); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// In the grace window (ttl 10 < age 11 <= ttl+grace 30): visible as critical.
	f.Advance(11 * time.Second)
	if removed := s.GC(); removed != 0 {
		t.Errorf("GC removed %d during grace, want 0", removed)
	}
	web, ok := lookupOne(t, s, "web")
	if !ok || web.Health != model.HealthCritical {
		t.Errorf("recently-critical not visible as critical: ok=%v health=%v", ok, web.Health)
	}

	// Past the grace window (age 36 > 30): reaped.
	f.Advance(25 * time.Second)
	if removed := s.GC(); removed != 1 {
		t.Errorf("GC removed %d past grace, want 1", removed)
	}
	if _, ok := lookupOne(t, s, "web"); ok {
		t.Error("web still present past grace window")
	}
}

func TestRenewRevivesCriticalService(t *testing.T) {
	t.Parallel()
	s, f := newStore(t, Options{GracePeriod: time.Minute})

	if err := s.Register(reg("n1", 1, svc("web", "web", 8080, 10*time.Second))); err != nil {
		t.Fatalf("Register: %v", err)
	}
	f.Advance(11 * time.Second)
	s.GC() // web -> critical (in grace)
	if e, _ := lookupOne(t, s, "web"); e.Health != model.HealthCritical {
		t.Fatalf("web not critical before renew: %v", e.Health)
	}

	if err := s.Renew("n1", []string{"web"}); err != nil {
		t.Fatalf("Renew: %v", err)
	}
	s.GC() // reconciles critical -> passing
	if e, _ := lookupOne(t, s, "web"); e.Health != model.HealthPassing {
		t.Errorf("web not revived to passing after renew: %v", e.Health)
	}
}

func TestModifyIndexTriggers(t *testing.T) {
	t.Parallel()
	s, f := newStore(t, Options{}) // grace 0

	prev := s.Index()
	advanced := func(what string) {
		t.Helper()
		if s.Index() <= prev {
			t.Errorf("%s did not advance the index (%d)", what, s.Index())
		}
		prev = s.Index()
	}
	unchanged := func(what string) {
		t.Helper()
		if s.Index() != prev {
			t.Errorf("%s moved the index: %d -> %d", what, prev, s.Index())
		}
	}

	must(t, s.Register(reg("n1", 1, svc("web", "web", 8080, 10*time.Second))))
	advanced("register")
	if s.Index() < 1 {
		t.Errorf("index must be >= 1 after first mutation, got %d", s.Index())
	}

	f.Advance(2 * time.Second)
	must(t, s.Renew("n1", nil))
	unchanged("renew")

	must(t, s.Register(reg("n1", 2, svc("web", "web", 9090, 10*time.Second))))
	advanced("endpoint-change")

	must(t, s.SetMaintenance("n1", "web", true))
	advanced("maintenance")

	f.Advance(20 * time.Second) // expire (grace 0 -> removed)
	s.GC()
	advanced("expire")

	// Index never decreases.
	if s.Index() < prev {
		t.Errorf("index regressed to %d", s.Index())
	}
}

func TestIndexSurvivesRestart(t *testing.T) {
	t.Parallel()
	s1, _ := newStore(t, Options{})
	must(t, s1.Register(reg("n1", 1, svc("web", "web", 8080, 30*time.Second))))
	must(t, s1.Register(reg("n2", 1, svc("api", "api", 9090, 30*time.Second))))
	high := s1.Index()

	// Simulate a restart resuming from the persisted high-watermark (the epoch).
	s2 := NewMemory(Options{StartIndex: high, Clock: clock.NewFake(epoch)})
	if s2.Index() != high {
		t.Fatalf("resumed index = %d, want %d", s2.Index(), high)
	}
	must(t, s2.Register(reg("n3", 1, svc("db", "db", 5432, 30*time.Second))))
	e, _ := lookupOne(t, s2, "db")
	if e.CreateIndex <= high {
		t.Errorf("post-restart CreateIndex %d did not exceed pre-restart high-watermark %d", e.CreateIndex, high)
	}
}

func TestMaintenanceVisibleAndIdempotent(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t, Options{})
	must(t, s.Register(reg("n1", 1, svc("web", "web", 8080, 30*time.Second))))

	must(t, s.SetMaintenance("n1", "web", true))
	idx := s.Index()
	e, ok := lookupOne(t, s, "web")
	if !ok || !e.Maintenance {
		t.Errorf("maintenance entry not visible/flagged: ok=%v maint=%v", ok, e.Maintenance)
	}

	// Idempotent: enabling again is a no-op.
	must(t, s.SetMaintenance("n1", "web", true))
	if s.Index() != idx {
		t.Errorf("redundant SetMaintenance moved the index: %d -> %d", idx, s.Index())
	}
}

func TestMultipleInstancesSameName(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t, Options{})

	must(t, s.Register(reg("n1", 1, svc("web", "web", 8080, 30*time.Second))))
	must(t, s.Register(reg("n2", 1, svc("web", "web", 8080, 30*time.Second))))
	// Two instances of the same name on one node, distinguished by id.
	must(t, s.Register(model.Registration{
		Node:     model.Node{ID: "n3", Datacenter: "dc1"},
		Services: []model.ServiceInstance{svc("web-a", "web", 8080, 30*time.Second), svc("web-b", "web", 8081, 30*time.Second)},
	}))

	res := s.Lookup(model.Query{Name: "web"})
	if len(res.Entries) != 4 {
		t.Fatalf("want 4 web instances, got %d", len(res.Entries))
	}
	// Sorted by (node, service) deterministically.
	for i := 1; i < len(res.Entries); i++ {
		a, b := res.Entries[i-1], res.Entries[i]
		if a.Node.ID > b.Node.ID || (a.Node.ID == b.Node.ID && a.Service.ID > b.Service.ID) {
			t.Errorf("entries not sorted at %d: %s/%s before %s/%s", i, a.Node.ID, a.Service.ID, b.Node.ID, b.Service.ID)
		}
	}
}

func TestLookupFilters(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t, Options{})
	must(t, s.Register(model.Registration{
		Node:     model.Node{ID: "n1", Datacenter: "dc1"},
		Services: []model.ServiceInstance{{ID: "web", Name: "web", Port: 80, TTL: 30 * time.Second, Tags: []string{"primary", "v2"}}},
	}))
	must(t, s.Register(model.Registration{
		Node:     model.Node{ID: "n2", Datacenter: "dc2"},
		Services: []model.ServiceInstance{{ID: "web", Name: "web", Port: 80, TTL: 30 * time.Second, Tags: []string{"v2"}}},
	}))

	if got := len(s.Lookup(model.Query{Name: "web", Datacenter: "dc1"}).Entries); got != 1 {
		t.Errorf("datacenter filter: got %d, want 1", got)
	}
	if got := len(s.Lookup(model.Query{Name: "web", Tags: []string{"primary"}}).Entries); got != 1 {
		t.Errorf("tag filter: got %d, want 1", got)
	}
	if got := len(s.Lookup(model.Query{Name: "web", Tags: []string{"v2"}}).Entries); got != 2 {
		t.Errorf("shared tag filter: got %d, want 2", got)
	}
	if got := len(s.Lookup(model.Query{Name: "absent"}).Entries); got != 0 {
		t.Errorf("missing service: got %d, want 0", got)
	}
}

func TestErrors(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t, Options{})
	must(t, s.Register(reg("n1", 1, svc("web", "web", 8080, 30*time.Second))))

	cases := []struct {
		name string
		err  error
		want error
	}{
		{"register no node id", s.Register(reg("", 1, svc("web", "web", 1, time.Second))), ErrInvalid},
		{"register service no name", s.Register(reg("n9", 1, svc("x", "", 1, time.Second))), ErrInvalid},
		{"renew unknown node", s.Renew("absent", nil), ErrNotFound},
		{"renew unknown service", s.Renew("n1", []string{"absent"}), ErrNotFound},
		{"deregister unknown node", s.Deregister("absent"), ErrNotFound},
		{"deregister unknown service", s.DeregisterService("n1", "absent"), ErrNotFound},
		{"maintenance unknown", s.SetMaintenance("n1", "absent", true), ErrNotFound},
	}
	for _, tc := range cases {
		if !errors.Is(tc.err, tc.want) {
			t.Errorf("%s: got %v, want %v", tc.name, tc.err, tc.want)
		}
	}
}

func TestOnChangeNotifier(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var got []model.ChangeEvent
	collect := func(events []model.ChangeEvent) {
		mu.Lock()
		got = append(got, events...)
		mu.Unlock()
	}

	f := clock.NewFake(epoch)
	s := NewMemory(Options{Clock: f, OnChange: collect})

	must(t, s.Register(reg("n1", 1, svc("web", "web", 8080, 10*time.Second))))
	f.Advance(20 * time.Second)
	s.GC()

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("want 2 events (put, delete), got %d: %+v", len(got), got)
	}
	if got[0].Type != model.ChangePut || got[1].Type != model.ChangeDelete {
		t.Errorf("event types = %v, %v; want put, delete", got[0].Type, got[1].Type)
	}
}

func TestConcurrentAccess(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t, Options{GracePeriod: time.Second})

	const workers = 8
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(id int) {
			defer wg.Done()
			node := "n" + string(rune('a'+id))
			for i := 0; i < 200; i++ {
				_ = s.Register(reg(node, uint64(i), svc("web", "web", uint16(8000+i), 30*time.Second)))
				_ = s.Renew(node, nil)
				_ = s.Lookup(model.Query{Name: "web"})
				_ = s.GC()
				_ = s.Index()
			}
			_ = s.Deregister(node)
		}(w)
	}
	wg.Wait()
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
