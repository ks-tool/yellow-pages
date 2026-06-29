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

package watch

import (
	"context"
	"testing"
	"time"

	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/model"
)

func putEvent(node, service string) model.ChangeEvent {
	return model.ChangeEvent{
		Type:  model.ChangePut,
		Entry: model.ServiceEntry{Node: model.Node{ID: node, Datacenter: "dc1"}, Service: model.ServiceInstance{ID: service, Name: service}},
	}
}

func TestNotifyBumpsIndexAndDeliversMatching(t *testing.T) {
	t.Parallel()
	w := New(0, clock.System())
	ch, cancel := w.Subscribe(model.Query{Name: "web"})
	defer cancel()

	w.Notify([]model.ChangeEvent{putEvent("a", "web")})
	if w.Index() != 1 {
		t.Fatalf("index = %d, want 1", w.Index())
	}
	select {
	case ev := <-ch:
		if ev.Type != model.ChangePut || ev.Index != 1 {
			t.Errorf("event = %+v, want put index 1", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("did not receive the matching event")
	}

	// An event for another service bumps the index but is not delivered to "web".
	w.Notify([]model.ChangeEvent{putEvent("a", "api")})
	if w.Index() != 2 {
		t.Errorf("index = %d, want 2", w.Index())
	}
	select {
	case ev := <-ch:
		t.Errorf("received an unmatched event: %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestCurrentIndexPerName(t *testing.T) {
	t.Parallel()
	w := New(0, clock.System())
	if got := w.CurrentIndex(model.Query{Name: "web"}); got != 1 {
		t.Errorf("unseen service index = %d, want 1 (>=1, table index)", got)
	}
	w.NotifyNames("web") // index -> 1
	w.NotifyNames("api") // index -> 2
	if got := w.CurrentIndex(model.Query{Name: "web"}); got != 1 {
		t.Errorf("web index = %d, want 1 (per-key, unaffected by api)", got)
	}
	if got := w.CurrentIndex(model.Query{Name: "api"}); got != 2 {
		t.Errorf("api index = %d, want 2", got)
	}
}

func TestWaitForChangeWakesOnChange(t *testing.T) {
	t.Parallel()
	fake := clock.NewFake(time.Unix(0, 0))
	w := New(0, fake)
	w.NotifyNames("web") // web index -> 1

	got := make(chan uint64, 1)
	go func() { got <- w.WaitForChange(context.Background(), model.Query{Name: "web"}, 1, time.Hour) }()

	// Blocked at index 1; a change must wake it.
	select {
	case <-got:
		t.Fatal("WaitForChange returned before any change")
	case <-time.After(50 * time.Millisecond):
	}
	w.NotifyNames("web") // index -> 2
	select {
	case idx := <-got:
		if idx != 2 {
			t.Errorf("woke with index %d, want 2", idx)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitForChange did not wake on change")
	}
}

func TestWaitForChangeImmediateAndTimeout(t *testing.T) {
	t.Parallel()
	fake := clock.NewFake(time.Unix(0, 0))
	w := New(0, fake)
	w.NotifyNames("web") // index 1

	// minIndex 0 returns immediately.
	if got := w.WaitForChange(context.Background(), model.Query{Name: "web"}, 0, time.Hour); got != 1 {
		t.Errorf("minIndex 0 = %d, want 1 immediately", got)
	}
	// Handover: client index ahead of ours returns immediately.
	if got := w.WaitForChange(context.Background(), model.Query{Name: "web"}, 99, time.Hour); got != 1 {
		t.Errorf("handover = %d, want current 1 immediately", got)
	}

	// No change: blocks until the wait elapses (fake clock).
	done := make(chan uint64, 1)
	go func() { done <- w.WaitForChange(context.Background(), model.Query{Name: "web"}, 1, 30*time.Second) }()
	fake.BlockUntil(1)
	fake.Advance(30 * time.Second)
	select {
	case idx := <-done:
		if idx != 1 {
			t.Errorf("timeout returned %d, want 1", idx)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitForChange did not time out")
	}
}

func TestPersistedBaseSurvivesRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if LoadBase(dir) != 0 {
		t.Fatal("fresh dir should load base 0")
	}

	w := New(0, clock.System())
	for i := 0; i < 7; i++ {
		w.NotifyNames("web")
	}
	high := w.Index() // 7
	if err := SaveBase(dir, high); err != nil {
		t.Fatalf("SaveBase: %v", err)
	}

	// "Restart": a new watcher resumes from the persisted base and never regresses.
	w2 := New(LoadBase(dir), clock.System())
	if w2.Index() != high {
		t.Errorf("resumed index = %d, want %d", w2.Index(), high)
	}
	w2.NotifyNames("web")
	if w2.Index() <= high {
		t.Errorf("post-restart index %d did not exceed the base %d", w2.Index(), high)
	}
}
