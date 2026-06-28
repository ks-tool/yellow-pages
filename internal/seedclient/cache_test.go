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

func TestCacheBoundedStaleness(t *testing.T) {
	t.Parallel()
	fake := clock.NewFake(epoch)
	addr, st := startSeed(t, clock.System())
	client := newClient(t, []string{addr}, 2*time.Second)
	cache := NewCache(client, CacheOptions{MaxAge: 10 * time.Second, Clock: fake, Log: discardLogger()})

	if err := st.Register(modelReg("agent-1", "web")); err != nil {
		t.Fatal(err)
	}
	if lr, _ := cache.Lookup(context.Background(), model.Query{Name: "web"}); len(lr.Entries) != 1 {
		t.Fatalf("first lookup want 1 entry, got %d", len(lr.Entries))
	}

	// The cluster gains a second instance, but within maxAge reads stay cached.
	if err := st.Register(modelReg("agent-2", "web")); err != nil {
		t.Fatal(err)
	}
	fake.Advance(5 * time.Second)
	if lr, _ := cache.Lookup(context.Background(), model.Query{Name: "web"}); len(lr.Entries) != 1 {
		t.Errorf("within maxAge the read must be served from cache (1 entry), got %d", len(lr.Entries))
	}

	// Past maxAge the read refetches and sees the new instance.
	fake.Advance(10 * time.Second)
	if lr, _ := cache.Lookup(context.Background(), model.Query{Name: "web"}); len(lr.Entries) != 2 {
		t.Errorf("past maxAge the read must refetch (2 entries), got %d", len(lr.Entries))
	}
}

func TestCacheAppliesHealthFilterPostMerge(t *testing.T) {
	t.Parallel()
	addr, st := startSeed(t, clock.System())
	client := newClient(t, []string{addr}, 2*time.Second)
	cache := NewCache(client, CacheOptions{MaxAge: time.Hour, Clock: clock.NewFake(epoch), Log: discardLogger()})

	if err := st.Register(modelReg("agent-1", "web")); err != nil {
		t.Fatal(err)
	}
	if err := st.SetMaintenance("agent-1", "web", true); err != nil {
		t.Fatal(err)
	}

	// Without ?passing the maintenance instance is visible; the cached (unfiltered)
	// set is then filtered fresh on the ?passing read — same cache entry.
	if lr, _ := cache.Lookup(context.Background(), model.Query{Name: "web"}); len(lr.Entries) != 1 {
		t.Errorf("unfiltered read want 1, got %d", len(lr.Entries))
	}
	if lr, _ := cache.Lookup(context.Background(), model.Query{Name: "web", OnlyHealthy: true}); len(lr.Entries) != 0 {
		t.Errorf("?passing must exclude the maintenance instance, got %d", len(lr.Entries))
	}
}

func TestCacheChangeNotifier(t *testing.T) {
	t.Parallel()
	addr, st := startSeed(t, clock.System())
	client := newClient(t, []string{addr}, 2*time.Second)

	var fired []string
	cache := NewCache(client, CacheOptions{
		MaxAge:   time.Nanosecond, // every read refetches
		Clock:    clock.System(),
		OnChange: func(name string) { fired = append(fired, name) },
		Log:      discardLogger(),
	})

	if err := st.Register(modelReg("agent-1", "web")); err != nil {
		t.Fatal(err)
	}
	_, _ = cache.Lookup(context.Background(), model.Query{Name: "web"}) // nil -> 1: change
	cache.Refresh(context.Background())                                 // same: no change
	if err := st.Register(modelReg("agent-2", "web")); err != nil {
		t.Fatal(err)
	}
	cache.Refresh(context.Background()) // 1 -> 2: change

	if len(fired) != 2 {
		t.Errorf("change-notifier fired %d times (%v), want 2", len(fired), fired)
	}
}
