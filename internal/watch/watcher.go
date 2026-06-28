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

// Package watch turns the registry change stream into a blocking/streaming
// surface. A Watcher carries its OWN monotonic index (a high-watermark over a
// persisted base, so it survives restarts and never regresses), maps it per
// service name plus a list-key, and lets callers either Subscribe to the live
// ChangeEvents or block in WaitForChange until the index advances. The seed
// feeds it from the Store change-notifier; the agent feeds it from its read
// cache, synthesising one monotonic index over N independent seed index spaces.
package watch

import (
	"context"
	"sync"
	"time"

	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/model"
)

type subscription struct {
	query model.Query
	ch    chan model.ChangeEvent
}

// Watcher is a monotonic, blocking/streaming view over registry changes.
type Watcher struct {
	clock clock.Clock

	mu        sync.Mutex
	index     uint64 // global high-watermark; the table index for list/unseen queries
	nameIndex map[string]uint64
	bump      chan struct{} // closed-and-replaced on every change (broadcast)
	subs      map[int]*subscription
	nextID    int
}

// New builds a Watcher resuming its index from base (the persisted high-watermark
// / epoch). A nil clock defaults to the system clock.
func New(base uint64, clk clock.Clock) *Watcher {
	if clk == nil {
		clk = clock.System()
	}
	return &Watcher{
		clock:     clk,
		index:     base,
		nameIndex: make(map[string]uint64),
		bump:      make(chan struct{}),
		subs:      make(map[int]*subscription),
	}
}

// Index returns the current high-watermark (always >= 1).
func (w *Watcher) Index() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return atLeastOne(w.index)
}

// Notify is the seed path: each event advances the index, updates the per-name
// and list keys, and is delivered to matching subscribers. Renews never reach
// here (the Store emits no change event for them), so they do not wake watchers.
func (w *Watcher) Notify(events []model.ChangeEvent) {
	if len(events) == 0 {
		return
	}
	var deliver []delivery

	w.mu.Lock()
	for _, ev := range events {
		w.index++
		w.nameIndex[ev.Entry.Service.Name] = w.index
		// Deliver with the watcher's own index, not the store's: seed indexes do
		// not leak; the watcher's is the monotonic one exposed downstream.
		out := ev
		out.Index = w.index
		for _, s := range w.subs {
			if matches(s.query, ev.Entry) {
				deliver = append(deliver, delivery{s.ch, out})
			}
		}
	}
	w.signalLocked()
	w.mu.Unlock()

	for _, d := range deliver {
		select {
		case d.ch <- d.event:
		default: // a slow subscriber: drop, it can re-read via WaitForChange/snapshot
		}
	}
}

// NotifyNames is the agent path: it advances the index for each changed service
// name without an event payload (the agent re-reads its merge cache for the new
// snapshot). It is what synthesises one monotonic index over N seeds.
func (w *Watcher) NotifyNames(names ...string) {
	if len(names) == 0 {
		return
	}
	w.mu.Lock()
	for _, name := range names {
		w.index++
		w.nameIndex[name] = w.index
	}
	w.signalLocked()
	w.mu.Unlock()
}

func (w *Watcher) signalLocked() {
	close(w.bump)
	w.bump = make(chan struct{})
}

// Subscribe returns a channel of ChangeEvents matching q and a cancel func that
// removes the subscription. The channel is buffered; a subscriber that falls
// behind drops events and should re-sync via a fresh snapshot.
func (w *Watcher) Subscribe(q model.Query) (<-chan model.ChangeEvent, func()) {
	w.mu.Lock()
	id := w.nextID
	w.nextID++
	s := &subscription{query: q, ch: make(chan model.ChangeEvent, 64)}
	w.subs[id] = s
	w.mu.Unlock()

	return s.ch, func() {
		w.mu.Lock()
		delete(w.subs, id)
		w.mu.Unlock()
	}
}

// CurrentIndex returns the index for q: the per-name key for a named query, else
// the list key. A not-yet-seen service reports the list key, so a watch on it
// wakes when it first appears.
func (w *Watcher) CurrentIndex(q model.Query) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.currentIndexLocked(q)
}

func (w *Watcher) currentIndexLocked(q model.Query) uint64 {
	if q.Name != "" {
		if idx, ok := w.nameIndex[q.Name]; ok {
			return atLeastOne(idx)
		}
	}
	// List queries and not-yet-seen services block on the table index (the global
	// high-watermark), so they wake when the set changes / the service appears.
	return atLeastOne(w.index)
}

// WaitForChange blocks until the index for q exceeds minIndex, or wait elapses,
// or ctx is cancelled, returning the current index. minIndex 0 (or already
// advanced) returns immediately — matching Consul blocking-query semantics,
// including handover where a client's index is greater than ours.
func (w *Watcher) WaitForChange(ctx context.Context, q model.Query, minIndex uint64, wait time.Duration) uint64 {
	timer := w.clock.NewTimer(wait)
	defer timer.Stop()

	for {
		w.mu.Lock()
		cur := w.currentIndexLocked(q)
		ch := w.bump
		w.mu.Unlock()

		// Block only while the index is exactly the client's; any difference is a
		// change (cur > minIndex) or a handover (cur < minIndex, e.g. after a
		// reset) — both return immediately to avoid a busy loop.
		if minIndex == 0 || cur != minIndex {
			return cur
		}
		select {
		case <-ch:
		case <-timer.C():
			return cur
		case <-ctx.Done():
			return cur
		}
	}
}

type delivery struct {
	ch    chan model.ChangeEvent
	event model.ChangeEvent
}

func matches(q model.Query, e model.ServiceEntry) bool {
	if q.Name != "" && q.Name != e.Service.Name {
		return false
	}
	if q.Datacenter != "" && q.Datacenter != e.Node.Datacenter {
		return false
	}
	return e.Service.MatchesTags(q.Tags)
}

func atLeastOne(x uint64) uint64 {
	if x < 1 {
		return 1
	}
	return x
}
