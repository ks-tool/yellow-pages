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

	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/model"
)

func regAt(nodeID, service, address string, gen uint64) model.Registration {
	r := modelReg(nodeID, service)
	r.Generation = gen
	r.Services[0].Address = address
	return r
}

func TestMergeEqualGenerationPicksLatestLastSeen(t *testing.T) {
	t.Parallel()
	addrA, sA := startSeed(t, clock.NewFake(epoch))
	addrB, sB := startSeed(t, clock.NewFake(epoch.Add(10*time.Second)))

	// Same (node, service, generation), different server-stamped last_seen.
	if err := sA.Register(regAt("agent-1", "web", "10.0.0.1", 5)); err != nil {
		t.Fatal(err)
	}
	if err := sB.Register(regAt("agent-1", "web", "10.0.0.2", 5)); err != nil {
		t.Fatal(err)
	}

	client := newClient(t, []string{addrA, addrB}, 2*time.Second)
	lr, err := client.Lookup(context.Background(), model.Query{Name: "web"})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(lr.Entries) != 1 || lr.Entries[0].Service.Address != "10.0.0.2" {
		t.Errorf("equal generation should pick the latest last_seen (10.0.0.2): %+v", lr.Entries)
	}
}

func TestMergeGenerationDominatesClockSkew(t *testing.T) {
	t.Parallel()
	// Seed A's clock is an hour ahead (skew) but holds STALE data (gen 5); seed B
	// is on time with FRESH data (gen 6). Generation must win despite the skew.
	addrA, sA := startSeed(t, clock.NewFake(epoch.Add(time.Hour)))
	addrB, sB := startSeed(t, clock.NewFake(epoch))
	if err := sA.Register(regAt("agent-1", "web", "10.0.0.1", 5)); err != nil {
		t.Fatal(err)
	}
	if err := sB.Register(regAt("agent-1", "web", "10.0.0.2", 6)); err != nil {
		t.Fatal(err)
	}

	client := newClient(t, []string{addrA, addrB}, 2*time.Second)
	lr, err := client.Lookup(context.Background(), model.Query{Name: "web"})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(lr.Entries) != 1 || lr.Entries[0].Service.Generation != 6 || lr.Entries[0].Service.Address != "10.0.0.2" {
		t.Errorf("higher generation must win over a skewed later last_seen: %+v", lr.Entries)
	}
}

func TestPartitionVisibleByOneSeedReturned(t *testing.T) {
	t.Parallel()
	addr, st := startSeed(t, clock.System())
	if err := st.Register(modelReg("agent-1", "web")); err != nil {
		t.Fatal(err)
	}

	// Two seeds are partitioned away (dead); the record visible on the one
	// reachable seed is still returned.
	client := newClient(t, []string{addr, deadAddr, deadAddr}, 500*time.Millisecond)
	lr, err := client.Lookup(context.Background(), model.Query{Name: "web"})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(lr.Entries) != 1 {
		t.Errorf("want the record from the reachable seed, got %d entries", len(lr.Entries))
	}
}

func TestDeregisterTwoOfThreeExpiresAfterTTLOnThird(t *testing.T) {
	t.Parallel()
	cA, cB, cC := clock.NewFake(epoch), clock.NewFake(epoch), clock.NewFake(epoch)
	addrA, sA := startSeed(t, cA)
	addrB, sB := startSeed(t, cB)
	addrC, sC := startSeed(t, cC)

	client := newClient(t, []string{addrA, addrB, addrC}, 2*time.Second)
	if res := client.Register(context.Background(), modelReg("agent-1", "web")); res.Succeeded != 3 {
		t.Fatalf("register want 3-of-3, got %+v", res)
	}

	// A deregister that reaches only two of three seeds leaves a ghost on the third.
	if err := sA.Deregister("agent-1"); err != nil {
		t.Fatal(err)
	}
	if err := sB.Deregister("agent-1"); err != nil {
		t.Fatal(err)
	}
	lr, _ := client.Lookup(context.Background(), model.Query{Name: "web"})
	if len(lr.Entries) != 1 {
		t.Fatalf("ghost should still be visible from the third seed, got %d", len(lr.Entries))
	}

	// Past the TTL on the third seed, its lease expires and the ghost is gone.
	cC.Advance(40 * time.Second)
	_ = sC // its store clock advanced; Lookup now skips the expired lease
	lr, _ = client.Lookup(context.Background(), model.Query{Name: "web"})
	if len(lr.Entries) != 0 {
		t.Errorf("ghost should be gone after TTL on the third seed, got %d", len(lr.Entries))
	}
}
