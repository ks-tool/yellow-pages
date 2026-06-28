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

package server

import (
	"context"
	"testing"
	"time"

	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/model"
	"github.com/ks-tool/yellow-pages/internal/store"
	"github.com/ks-tool/yellow-pages/internal/transport"
	"github.com/ks-tool/yellow-pages/internal/watch"
	discoveryv1 "github.com/ks-tool/yellow-pages/proto/discovery/v1"
)

var watchEpoch = time.Unix(1_700_000_000, 0).UTC()

func webReg(nodeID string) model.Registration {
	return model.Registration{
		Node:       model.Node{ID: nodeID, Datacenter: "dc1"},
		Services:   []model.ServiceInstance{{ID: "web", Name: "web", TTL: 30 * time.Second}},
		Generation: 1,
	}
}

func TestSeedWatchStream(t *testing.T) {
	t.Parallel()
	fake := clock.NewFake(watchEpoch)
	watcher := watch.New(0, fake)
	st := store.NewMemory(store.Options{Clock: fake, DefaultTTL: 30 * time.Second, OnChange: watcher.Notify})

	comp := NewComponent(Options{
		Addr:      "127.0.0.1:0",
		Service:   New(st, testLogger()).SetWatcher(watcher),
		Transport: transport.Insecure(),
		Log:       testLogger(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = comp.Start(ctx) }()
	addr := comp.Addr()
	if addr == nil {
		cancel()
		t.Fatal("server failed to bind")
	}
	conn, err := transport.Insecure().Dial(ctx, addr.String())
	if err != nil {
		cancel()
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		sctx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer scancel()
		_ = comp.Stop(sctx)
		cancel()
	})

	stream, err := discoveryv1.NewAgentServiceClient(conn).Watch(ctx, &discoveryv1.WatchRequest{
		Query: &discoveryv1.Query{Name: "web"},
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	events := make(chan *discoveryv1.WatchResponse, 16)
	go func() {
		for {
			r, rerr := stream.Recv()
			if rerr != nil {
				return
			}
			events <- r
		}
	}()

	waitSnapshotDone(t, events)

	// register -> put
	mustStore(t, st.Register(webReg("agent-1")))
	assertEvent(t, events, discoveryv1.ChangeType_CHANGE_TYPE_PUT)

	// renew -> NO event
	mustStore(t, st.Renew("agent-1", nil))
	assertNoEvent(t, events)

	// deregister -> delete
	mustStore(t, st.Deregister("agent-1"))
	assertEvent(t, events, discoveryv1.ChangeType_CHANGE_TYPE_DELETE)

	// expire (GC after TTL) -> delete
	mustStore(t, st.Register(webReg("agent-2")))
	assertEvent(t, events, discoveryv1.ChangeType_CHANGE_TYPE_PUT)
	fake.Advance(40 * time.Second)
	st.GC()
	assertEvent(t, events, discoveryv1.ChangeType_CHANGE_TYPE_DELETE)
}

// TestSeedWatchUnimplementedWithoutWatcher verifies a watcher-less node reports
// Unimplemented (M3 seeds had no watcher).
func TestSeedWatchUnimplementedWithoutWatcher(t *testing.T) {
	t.Parallel()
	_, conn := dial(t, memStore(t))
	stream, err := discoveryv1.NewAgentServiceClient(conn).Watch(context.Background(), &discoveryv1.WatchRequest{
		Query: &discoveryv1.Query{Name: "web"},
	})
	if err != nil {
		t.Fatalf("Watch open: %v", err)
	}
	if _, err := stream.Recv(); err == nil {
		t.Error("expected Unimplemented without a watcher")
	}
}

func waitSnapshotDone(t *testing.T, events <-chan *discoveryv1.WatchResponse) {
	t.Helper()
	for {
		select {
		case r := <-events:
			if r.GetSnapshotDone() {
				return
			}
		case <-time.After(2 * time.Second):
			t.Fatal("did not receive snapshot_done")
		}
	}
}

func assertEvent(t *testing.T, events <-chan *discoveryv1.WatchResponse, want discoveryv1.ChangeType) {
	t.Helper()
	select {
	case r := <-events:
		if r.GetEvent().GetType() != want {
			t.Errorf("event type = %v, want %v", r.GetEvent().GetType(), want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("did not receive a %v event", want)
	}
}

func assertNoEvent(t *testing.T, events <-chan *discoveryv1.WatchResponse) {
	t.Helper()
	select {
	case r := <-events:
		t.Errorf("unexpected event (renew must not wake watch): %+v", r.GetEvent())
	case <-time.After(250 * time.Millisecond):
	}
}

func mustStore(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("store op: %v", err)
	}
}
