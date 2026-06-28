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

package health

import (
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/ks-tool/yellow-pages/internal/model"
)

func entry(node, svc string, gen uint64, lastSeen time.Time, h model.HealthState, maint bool) model.ServiceEntry {
	return model.ServiceEntry{
		Node:        model.Node{ID: node},
		Service:     model.ServiceInstance{ID: svc, Name: svc, Generation: gen, LastSeen: lastSeen},
		Health:      h,
		Maintenance: maint,
	}
}

func TestVisible(t *testing.T) {
	t.Parallel()

	base := time.Unix(1000, 0)
	cases := []struct {
		name    string
		e       model.ServiceEntry
		opts    FilterOptions
		visible bool
	}{
		{"all visible without OnlyPassing: critical", entry("a", "s", 1, base, model.HealthCritical, false), FilterOptions{}, true},
		{"all visible without OnlyPassing: maintenance", entry("a", "s", 1, base, model.HealthPassing, true), FilterOptions{}, true},
		{"passing kept", entry("a", "s", 1, base, model.HealthPassing, false), FilterOptions{OnlyPassing: true}, true},
		{"warning treated as passing", entry("a", "s", 1, base, model.HealthWarning, false), FilterOptions{OnlyPassing: true}, true},
		{"critical dropped", entry("a", "s", 1, base, model.HealthCritical, false), FilterOptions{OnlyPassing: true}, false},
		{"maintenance dropped even if passing", entry("a", "s", 1, base, model.HealthPassing, true), FilterOptions{OnlyPassing: true}, false},
		{"unspecified dropped", entry("a", "s", 1, base, model.HealthUnspecified, false), FilterOptions{OnlyPassing: true}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := Visible(tc.e, tc.opts); got != tc.visible {
				t.Errorf("Visible() = %v, want %v", got, tc.visible)
			}
		})
	}
}

func TestFilterDoesNotMutateInput(t *testing.T) {
	t.Parallel()

	base := time.Unix(1000, 0)
	in := []model.ServiceEntry{
		entry("a", "s1", 1, base, model.HealthPassing, false),
		entry("a", "s2", 1, base, model.HealthCritical, false),
		entry("a", "s3", 1, base, model.HealthPassing, true),
	}
	snapshot := append([]model.ServiceEntry(nil), in...)

	got := Filter(in, FilterOptions{OnlyPassing: true})
	if len(got) != 1 || got[0].Service.ID != "s1" {
		t.Fatalf("Filter() = %+v, want only s1", got)
	}
	if !reflect.DeepEqual(in, snapshot) {
		t.Errorf("Filter mutated its input")
	}
}

// TestFilterSharedAcrossSurfaces asserts the invariant that the single Filter is
// the only health gate, so native gRPC, Consul HTTP and Consul DNS — three
// independent callers passing the same input — get identical results.
func TestFilterSharedAcrossSurfaces(t *testing.T) {
	t.Parallel()

	base := time.Unix(1000, 0)
	in := []model.ServiceEntry{
		entry("a", "s1", 1, base, model.HealthPassing, false),
		entry("a", "s2", 1, base, model.HealthCritical, false),
		entry("b", "s3", 1, base, model.HealthWarning, false),
		entry("b", "s4", 1, base, model.HealthPassing, true),
	}
	opts := FilterOptions{OnlyPassing: true}

	grpc := Filter(in, opts)
	consulHTTP := Filter(in, opts)
	dns := Filter(in, opts)
	if !reflect.DeepEqual(grpc, consulHTTP) || !reflect.DeepEqual(grpc, dns) {
		t.Fatalf("surfaces diverged:\n grpc=%+v\n http=%+v\n  dns=%+v", grpc, consulHTTP, dns)
	}
}

func TestMergeLWWDedupAndWinner(t *testing.T) {
	t.Parallel()

	base := time.Unix(1000, 0)
	// Same (node,service) twice: higher generation wins; different identities
	// are both kept.
	in := []model.ServiceEntry{
		entry("a", "web", 5, base.Add(2*time.Second), model.HealthPassing, false), // stale data
		entry("a", "web", 6, base, model.HealthPassing, false),                    // fresh data
		entry("b", "api", 1, base, model.HealthPassing, false),
	}

	got := MergeLWW(in)
	if len(got) != 2 {
		t.Fatalf("MergeLWW() len = %d, want 2", len(got))
	}
	if got[0].Node.ID != "a" || got[0].Service.Generation != 6 {
		t.Errorf("winner for a/web = gen %d, want 6", got[0].Service.Generation)
	}
	if got[1].Node.ID != "b" || got[1].Service.ID != "api" {
		t.Errorf("second entry = %s/%s, want b/api", got[1].Node.ID, got[1].Service.ID)
	}
}

// TestMergeLWWGenerationBeatsLastSeen is the divergent-data invariant: a stale
// endpoint on a seed that merely collected more renews (a much later LastSeen)
// must NOT beat the fresher data version (higher Generation).
func TestMergeLWWGenerationBeatsLastSeen(t *testing.T) {
	t.Parallel()

	base := time.Unix(1000, 0)
	stale := entry("a", "web", 5, base.Add(time.Hour), model.HealthPassing, false) // many renews, old endpoint
	fresh := entry("a", "web", 6, base, model.HealthPassing, false)                // new endpoint, fewer renews
	fresh.Service.Address = "10.0.0.2"
	stale.Service.Address = "10.0.0.1"

	for _, order := range [][]model.ServiceEntry{{stale, fresh}, {fresh, stale}} {
		got := MergeLWW(order)
		if len(got) != 1 {
			t.Fatalf("MergeLWW() len = %d, want 1", len(got))
		}
		if got[0].Service.Generation != 6 || got[0].Service.Address != "10.0.0.2" {
			t.Errorf("winner = gen %d addr %s, want gen 6 addr 10.0.0.2",
				got[0].Service.Generation, got[0].Service.Address)
		}
	}
}

// TestMergeLWWDeterministicUnderPermutation shuffles the input many ways and
// asserts the output never changes.
func TestMergeLWWDeterministicUnderPermutation(t *testing.T) {
	t.Parallel()

	base := time.Unix(1000, 0)
	in := []model.ServiceEntry{
		entry("a", "web", 6, base, model.HealthPassing, false),
		entry("a", "web", 5, base.Add(time.Hour), model.HealthPassing, false),
		entry("c", "db", 2, base, model.HealthCritical, false),
		entry("b", "api", 1, base, model.HealthPassing, false),
		entry("b", "api", 1, base.Add(time.Second), model.HealthPassing, false),
	}
	want := MergeLWW(in)

	rng := rand.New(rand.NewSource(42)) //nolint:gosec // deterministic test shuffle
	for i := 0; i < 50; i++ {
		shuffled := append([]model.ServiceEntry(nil), in...)
		rng.Shuffle(len(shuffled), func(a, b int) { shuffled[a], shuffled[b] = shuffled[b], shuffled[a] })
		if got := MergeLWW(shuffled); !reflect.DeepEqual(got, want) {
			t.Fatalf("permutation %d diverged:\n got = %+v\nwant = %+v", i, got, want)
		}
	}
}

func TestMergeLWWEmpty(t *testing.T) {
	t.Parallel()
	if got := MergeLWW(nil); got != nil {
		t.Errorf("MergeLWW(nil) = %+v, want nil", got)
	}
}
