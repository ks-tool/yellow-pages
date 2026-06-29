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
	"github.com/ks-tool/yellow-pages/internal/server"
	"github.com/ks-tool/yellow-pages/internal/transport"
	"github.com/ks-tool/yellow-pages/internal/watch"
	discoveryv1 "github.com/ks-tool/yellow-pages/proto/discovery/v1"
)

// TestAgentIndexSynthesis verifies the agent synthesises one monotonic index
// (>=1) from cache changes, and that observing the cluster again without a change
// (e.g. after a seed restart) never regresses it.
func TestAgentIndexSynthesis(t *testing.T) {
	t.Parallel()
	addr, st := startSeed(t, clock.System())
	client := newClient(t, []string{addr}, 2*time.Second)

	w := watch.New(0, clock.System())
	cache := NewCache(client, CacheOptions{
		MaxAge:   time.Nanosecond, // always refetch so each Lookup/Refresh observes the cluster
		Clock:    clock.System(),
		OnChange: func(name string) { w.NotifyNames(name) },
		Log:      discardLogger(),
	})
	q := model.Query{Name: "web"}

	if err := st.Register(modelReg("agent-1", "web")); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Lookup(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	idx1 := w.CurrentIndex(q)
	if idx1 < 1 {
		t.Fatalf("agent index = %d, want >= 1", idx1)
	}

	if err := st.Register(modelReg("agent-2", "web")); err != nil {
		t.Fatal(err)
	}
	cache.Refresh(context.Background())
	idx2 := w.CurrentIndex(q)
	if idx2 <= idx1 {
		t.Errorf("agent index not monotonic: %d -> %d", idx1, idx2)
	}

	// Re-observing the same set (as after a seed restart) must not regress it.
	cache.Refresh(context.Background())
	if got := w.CurrentIndex(q); got < idx2 {
		t.Errorf("agent index regressed: %d -> %d", idx2, got)
	}
}

// TestAgentWatchStreamBlocksAndWakes verifies the agent's Watch RPC blocks until
// the set changes, then streams a fresh snapshot.
func TestAgentWatchStreamBlocksAndWakes(t *testing.T) {
	t.Parallel()
	addr, st := startSeed(t, clock.System())
	client := newClient(t, []string{addr}, 2*time.Second)

	w := watch.New(0, clock.System())
	cache := NewCache(client, CacheOptions{
		MaxAge:   time.Hour,
		Clock:    clock.System(),
		OnChange: func(name string) { w.NotifyNames(name) },
		Log:      discardLogger(),
	})
	proxy := NewProxy(ProxyOptions{
		Client:  client,
		Node:    model.Node{ID: "agent-x", Datacenter: "dc1"},
		Quorum:  1,
		Cache:   cache,
		Watcher: w,
		Log:     discardLogger(),
	})

	comp := server.NewComponent(server.Options{
		Addr:      "127.0.0.1:0",
		Service:   proxy,
		Transport: transport.Insecure(),
		Log:       discardLogger(),
	})
	srvAddr := serveComponent(t, comp)
	conn, err := transport.Insecure().Dial(context.Background(), srvAddr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := discoveryv1.NewAgentServiceClient(conn).Watch(ctx, &discoveryv1.WatchRequest{
		Query: &discoveryv1.Query{Name: "web"},
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	puts := make(chan string, 16)
	go func() {
		for {
			r, rerr := stream.Recv()
			if rerr != nil {
				return
			}
			if ev := r.GetEvent(); ev.GetType() == discoveryv1.ChangeType_CHANGE_TYPE_PUT {
				puts <- ev.GetEntry().GetService().GetName()
			}
		}
	}()

	// The watch is blocked (web does not exist yet). Register it, then drive a
	// cache refresh so the agent observes the change and wakes the watch.
	if err := st.Register(modelReg("agent-1", "web")); err != nil {
		t.Fatal(err)
	}
	cache.Refresh(context.Background())

	select {
	case name := <-puts:
		if name != "web" {
			t.Errorf("woke with %q, want web", name)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("agent watch did not wake on the set change")
	}
}
