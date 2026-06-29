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
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/health"
	"github.com/ks-tool/yellow-pages/internal/model"
	"github.com/ks-tool/yellow-pages/internal/observability"
)

// Cache is the agent's bounded-staleness read cache. It stores the merged,
// UNFILTERED entry set per query (so ?passing is applied fresh on each read,
// post-merge) and refetches synchronously once an entry is older than maxAge.
// A background RefreshLoop keeps entries warm; a change-notifier fires when a
// refresh changes a set (the basis for the agent-synthesised index in M8).
type Cache struct {
	client   *SeedClient
	maxAge   time.Duration
	clock    clock.Clock
	prop     *observability.Propagation
	onChange func(name string)
	log      *slog.Logger

	mu      sync.Mutex
	entries map[cacheKey]*cacheEntry
}

type cacheKey struct {
	name string
	dc   string
	tags string
}

type cacheEntry struct {
	query     model.Query
	merged    []model.ServiceEntry // full merged set, before the health filter
	index     uint64
	fetchedAt time.Time
}

func keyFor(q model.Query) cacheKey {
	return cacheKey{name: q.Name, dc: q.Datacenter, tags: strings.Join(q.Tags, "\x00")}
}

// CacheOptions configures a Cache.
type CacheOptions struct {
	MaxAge   time.Duration
	Clock    clock.Clock
	Prop     *observability.Propagation
	OnChange func(name string) // optional change-notifier
	Log      *slog.Logger
}

// NewCache builds a read cache over client.
func NewCache(client *SeedClient, opts CacheOptions) *Cache {
	if opts.MaxAge <= 0 {
		opts.MaxAge = 5 * time.Second
	}
	if opts.Clock == nil {
		opts.Clock = clock.System()
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	return &Cache{
		client:   client,
		maxAge:   opts.MaxAge,
		clock:    opts.Clock,
		prop:     opts.Prop,
		onChange: opts.OnChange,
		log:      opts.Log,
		entries:  make(map[cacheKey]*cacheEntry),
	}
}

// Lookup serves q from the cache (bounded staleness), discarding the entry age.
func (c *Cache) Lookup(ctx context.Context, q model.Query) (model.LookupResult, error) {
	lr, _, err := c.LookupWithAge(ctx, q)
	return lr, err
}

// LookupWithAge serves q from the cache when the entry is fresh (within maxAge),
// else refetches, also returning the age of the served entry. The health filter
// (q.OnlyHealthy) is applied AFTER the merge, so an instance healthy on the
// freshest seed is not dropped by a stale seed.
func (c *Cache) LookupWithAge(ctx context.Context, q model.Query) (model.LookupResult, time.Duration, error) {
	key := keyFor(q)

	c.mu.Lock()
	e, ok := c.entries[key]
	fresh := ok && c.clock.Since(e.fetchedAt) < c.maxAge
	c.mu.Unlock()

	if !fresh {
		var err error
		e, err = c.fetch(ctx, q)
		if err != nil {
			if ok {
				c.log.Warn("serving stale cache after refresh failure", "service", q.Name, "error", err)
			} else {
				return model.LookupResult{}, 0, err
			}
		}
	}

	age := c.clock.Since(e.fetchedAt)
	c.prop.SetCacheAge(age)

	entries := e.merged
	if q.OnlyHealthy {
		entries = health.Filter(entries, health.FilterOptions{OnlyPassing: true})
	}
	return model.LookupResult{Entries: entries, Index: e.index}, age, nil
}

// fetch fans out (asking seeds for ALL instances so the merge, not a stale
// seed's filter, decides health) and stores the merged set.
func (c *Cache) fetch(ctx context.Context, q model.Query) (*cacheEntry, error) {
	raw := q
	raw.OnlyHealthy = false
	lr, err := c.client.Lookup(ctx, raw)
	if err != nil {
		return nil, err
	}

	key := keyFor(q)
	entry := &cacheEntry{query: q, merged: lr.Entries, index: lr.Index, fetchedAt: c.clock.Now()}

	c.mu.Lock()
	prev := c.entries[key]
	c.entries[key] = entry
	c.mu.Unlock()

	if c.onChange != nil && changed(prev, entry) {
		c.onChange(q.Name)
	}
	return entry, nil
}

// Refresh refetches every cached query. The RefreshLoop calls it on a tick.
func (c *Cache) Refresh(ctx context.Context) {
	c.mu.Lock()
	queries := make([]model.Query, 0, len(c.entries))
	for _, e := range c.entries {
		queries = append(queries, e.query)
	}
	c.mu.Unlock()

	for _, q := range queries {
		if _, err := c.fetch(ctx, q); err != nil {
			c.log.Warn("cache refresh failed", "service", q.Name, "error", err)
		}
	}
}

// changed reports whether the merged set differs in a way readers care about
// (instance identity, endpoint, generation or health).
func changed(prev, cur *cacheEntry) bool {
	if prev == nil {
		return len(cur.merged) > 0
	}
	if len(prev.merged) != len(cur.merged) {
		return true
	}
	for i := range cur.merged {
		a, b := prev.merged[i], cur.merged[i]
		if a.Node.ID != b.Node.ID || a.Service.ID != b.Service.ID || endpointChanged(a, b) {
			return true
		}
	}
	return false
}

// RefreshLoop is an app.Component that periodically refreshes the cache.
type RefreshLoop struct {
	cache    *Cache
	interval time.Duration
	clock    clock.Clock
	log      *slog.Logger
}

// NewRefreshLoop builds the refresh loop ticking every interval.
func NewRefreshLoop(cache *Cache, interval time.Duration, clk clock.Clock, log *slog.Logger) *RefreshLoop {
	if clk == nil {
		clk = clock.System()
	}
	if log == nil {
		log = slog.Default()
	}
	return &RefreshLoop{cache: cache, interval: interval, clock: clk, log: log}
}

// Name identifies the component.
func (l *RefreshLoop) Name() string { return "cache-refresh" }

// Start refreshes the cache on every tick until ctx is cancelled.
func (l *RefreshLoop) Start(ctx context.Context) error {
	ticker := l.clock.NewTicker(l.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C():
			l.cache.Refresh(ctx)
		}
	}
}

// Stop is a no-op; Start returns when its context is cancelled.
func (l *RefreshLoop) Stop(context.Context) error { return nil }
