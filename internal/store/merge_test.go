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
	"testing"
	"time"

	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/model"
)

func entry(node, svc string, gen uint64, port uint16, seen time.Time) model.ServiceEntry {
	return model.ServiceEntry{
		Node: model.Node{ID: node, Name: node, Address: "10.0.0.1", Datacenter: "dc1"},
		Service: model.ServiceInstance{
			ID: svc, Name: svc, Address: "10.0.0.1", Port: port,
			TTL: 30 * time.Second, Generation: gen, LastSeen: seen,
		},
	}
}

func TestMergeLWW(t *testing.T) {
	t.Parallel()
	t0 := time.Unix(1_700_000_000, 0)
	clk := clock.NewFake(t0)
	s := NewMemory(Options{Clock: clk, DefaultTTL: 30 * time.Second})

	// Absent service is added.
	if n := s.Merge([]model.ServiceEntry{entry("n1", "web", 1, 80, t0)}); n != 1 {
		t.Fatalf("initial merge applied %d, want 1", n)
	}

	// A newer generation wins.
	if n := s.Merge([]model.ServiceEntry{entry("n1", "web", 2, 90, t0)}); n != 1 {
		t.Errorf("newer-generation merge applied %d, want 1", n)
	}
	if got := s.Lookup(model.Query{Name: "web"}).Entries[0].Service.Port; got != 90 {
		t.Errorf("port = %d, want 90 (newer gen won)", got)
	}

	// An older generation is rejected (no lost local update).
	if n := s.Merge([]model.ServiceEntry{entry("n1", "web", 1, 70, t0)}); n != 0 {
		t.Errorf("older-generation merge applied %d, want 0", n)
	}
	if got := s.Lookup(model.Query{Name: "web"}).Entries[0].Service.Port; got != 90 {
		t.Errorf("port = %d, want 90 unchanged (older gen rejected)", got)
	}

	// Same generation, newer last_seen wins.
	if n := s.Merge([]model.ServiceEntry{entry("n1", "web", 2, 95, t0.Add(time.Second))}); n != 1 {
		t.Errorf("newer-last_seen merge applied %d, want 1", n)
	}
	if got := s.Lookup(model.Query{Name: "web"}).Entries[0].Service.Port; got != 95 {
		t.Errorf("port = %d, want 95 (newer last_seen won)", got)
	}
}
