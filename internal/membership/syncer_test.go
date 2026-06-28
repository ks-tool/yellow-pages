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

package membership

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/model"
)

type fakePuller struct {
	entries []model.ServiceEntry
	calls   atomic.Int32
}

func (f *fakePuller) Lookup(context.Context, model.Query) (model.LookupResult, error) {
	f.calls.Add(1)
	return model.LookupResult{Entries: f.entries}, nil
}
func (f *fakePuller) Reachable(context.Context) int { return 1 }
func (f *fakePuller) Seeds() []string               { return []string{"peer-1:9900"} }

type fakeMerger struct{ applied []model.ServiceEntry }

func (m *fakeMerger) Merge(e []model.ServiceEntry) int {
	m.applied = append(m.applied, e...)
	return len(e)
}

type fakeGate struct{ ready atomic.Bool }

func (g *fakeGate) SetReady(v bool) { g.ready.Store(v) }

func entry(node, svc string) model.ServiceEntry {
	return model.ServiceEntry{Node: model.Node{ID: node}, Service: model.ServiceInstance{ID: svc, Name: svc}}
}

func TestSyncerSnapshotGatesReadiness(t *testing.T) {
	t.Parallel()
	puller := &fakePuller{entries: []model.ServiceEntry{entry("n1", "web"), entry("n2", "api")}}
	merger := &fakeMerger{}
	gate := &fakeGate{}
	clk := clock.NewFake(time.Unix(0, 0))

	s := New(Options{
		Self: "self:9900", Peers: puller, Store: merger, Interval: time.Minute,
		Clock: clk, Gate: gate, Prop: nil, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	// The gate must NOT be ready before Start runs the snapshot.
	if gate.ready.Load() {
		t.Fatal("gate ready before snapshot")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Start(ctx) }()

	select {
	case <-s.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot never completed")
	}

	// After the snapshot: the registry is populated and the seed is marked ready.
	if !gate.ready.Load() {
		t.Error("gate not ready after snapshot")
	}
	if len(merger.applied) != 2 {
		t.Errorf("merged %d entries, want 2 (the join snapshot)", len(merger.applied))
	}
}

func TestSyncerMembers(t *testing.T) {
	t.Parallel()
	s := New(Options{Self: "self:9900", Peers: &fakePuller{}, Store: &fakeMerger{},
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))})

	members := s.Members(context.Background())
	if len(members) != 2 || members[0].Name != "self:9900" || members[0].Status != "alive" {
		t.Fatalf("members = %+v, want self + 1 peer", members)
	}
	if members[1].Addr != "peer-1:9900" || members[1].Status != "alive" {
		t.Errorf("peer member = %+v, want alive peer-1", members[1])
	}
}
