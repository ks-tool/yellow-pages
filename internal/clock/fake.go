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

package clock

import (
	"sync"
	"time"
)

// Fake is a deterministic Clock whose notion of "now" only moves when Advance
// is called. Timers, tickers and After channels fire when Advance crosses their
// deadline. BlockUntil lets a test wait until a number of waiters have been
// registered, removing the need for sleeps to avoid races.
type Fake struct {
	mu      sync.Mutex
	cond    *sync.Cond
	now     time.Time
	waiters []*waiter
}

type waiter struct {
	until   time.Time
	period  time.Duration // > 0 for tickers
	ch      chan time.Time
	stopped bool
}

// NewFake returns a Fake clock initialised to t.
func NewFake(t time.Time) *Fake {
	f := &Fake{now: t}
	f.cond = sync.NewCond(&f.mu)
	return f
}

// Now returns the fake clock's current time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Since reports the elapsed time since t according to the fake clock.
func (f *Fake) Since(t time.Time) time.Duration { return f.Now().Sub(t) }

// After returns a channel that fires once the fake clock advances past d.
func (f *Fake) After(d time.Duration) <-chan time.Time {
	return f.newWaiter(d, 0).ch
}

// Sleep blocks until the fake clock advances by at least d.
func (f *Fake) Sleep(d time.Duration) { <-f.After(d) }

// NewTimer returns a single-shot Timer driven by the fake clock.
func (f *Fake) NewTimer(d time.Duration) Timer {
	return &fakeTimer{f: f, w: f.newWaiter(d, 0)}
}

// NewTicker returns a Ticker driven by the fake clock. It panics if d <= 0.
func (f *Fake) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		panic("clock: non-positive interval for NewTicker")
	}
	return &fakeTicker{f: f, w: f.newWaiter(d, d)}
}

func (f *Fake) newWaiter(d, period time.Duration) *waiter {
	f.mu.Lock()
	defer f.mu.Unlock()
	w := &waiter{until: f.now.Add(d), period: period, ch: make(chan time.Time, 1)}
	if d <= 0 && period == 0 {
		// A non-positive one-shot fires immediately, like time.After(0).
		w.ch <- f.now
		return w
	}
	f.waiters = append(f.waiters, w)
	f.cond.Broadcast()
	return w
}

// Advance moves the fake clock forward by d, firing any waiters whose deadline
// is now in the past. Tickers fire once per elapsed period.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
	kept := f.waiters[:0]
	for _, w := range f.waiters {
		if w.stopped {
			continue
		}
		switch {
		case w.period > 0:
			for !w.until.After(f.now) {
				send(w.ch, w.until)
				w.until = w.until.Add(w.period)
			}
			kept = append(kept, w)
		case !w.until.After(f.now):
			send(w.ch, w.until)
		default:
			kept = append(kept, w)
		}
	}
	f.waiters = kept
	f.cond.Broadcast()
}

// BlockUntil blocks until at least n active waiters are registered. Use it in
// tests to deterministically wait for code-under-test to arm a timer/After
// before advancing the clock.
func (f *Fake) BlockUntil(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for f.activeLocked() < n {
		f.cond.Wait()
	}
}

func (f *Fake) activeLocked() int {
	n := 0
	for _, w := range f.waiters {
		if !w.stopped {
			n++
		}
	}
	return n
}

func (f *Fake) ensureQueuedLocked(w *waiter) {
	for _, x := range f.waiters {
		if x == w {
			return
		}
	}
	f.waiters = append(f.waiters, w)
}

// send delivers t without blocking, dropping the value if the buffer is full —
// matching the behaviour of *time.Timer/*time.Ticker channels.
func send(ch chan time.Time, t time.Time) {
	select {
	case ch <- t:
	default:
	}
}

type fakeTimer struct {
	f *Fake
	w *waiter
}

func (t *fakeTimer) C() <-chan time.Time { return t.w.ch }

func (t *fakeTimer) Stop() bool {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	active := !t.w.stopped
	t.w.stopped = true
	t.f.cond.Broadcast()
	return active
}

func (t *fakeTimer) Reset(d time.Duration) bool {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	active := !t.w.stopped
	t.w.stopped = false
	t.w.period = 0
	t.w.until = t.f.now.Add(d)
	t.f.ensureQueuedLocked(t.w)
	t.f.cond.Broadcast()
	return active
}

type fakeTicker struct {
	f *Fake
	w *waiter
}

func (t *fakeTicker) C() <-chan time.Time { return t.w.ch }

func (t *fakeTicker) Stop() {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	t.w.stopped = true
	t.f.cond.Broadcast()
}

func (t *fakeTicker) Reset(d time.Duration) {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	if d <= 0 {
		panic("clock: non-positive interval for Ticker.Reset")
	}
	t.w.stopped = false
	t.w.period = d
	t.w.until = t.f.now.Add(d)
	t.f.ensureQueuedLocked(t.w)
	t.f.cond.Broadcast()
}
