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

// Package clock provides a small time seam so that TTL/GC/heartbeat/blocking
// logic can be driven deterministically in tests. Production code depends on
// the Clock interface; real code uses System(), tests use NewFake().
package clock

import "time"

// Clock abstracts the parts of the standard library's time package that the
// service relies on. Keeping this an interface lets tests inject a controllable
// fake clock instead of sleeping on the wall clock.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// Since reports the time elapsed since t (Now().Sub(t)).
	Since(t time.Time) time.Duration
	// Sleep blocks for at least d.
	Sleep(d time.Duration)
	// After returns a channel that receives the current time after d elapses.
	After(d time.Duration) <-chan time.Time
	// NewTimer returns a single-shot Timer that fires after d.
	NewTimer(d time.Duration) Timer
	// NewTicker returns a Ticker that fires every d. It panics if d <= 0.
	NewTicker(d time.Duration) Ticker
}

// Timer mirrors the contract of *time.Timer behind the Clock seam.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
}

// Ticker mirrors the contract of *time.Ticker behind the Clock seam.
type Ticker interface {
	C() <-chan time.Time
	Stop()
	Reset(d time.Duration)
}

// System returns a Clock backed by the standard library time package.
func System() Clock { return systemClock{} }

type systemClock struct{}

func (systemClock) Now() time.Time                         { return time.Now() }
func (systemClock) Since(t time.Time) time.Duration        { return time.Since(t) }
func (systemClock) Sleep(d time.Duration)                  { time.Sleep(d) }
func (systemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (systemClock) NewTimer(d time.Duration) Timer         { return systemTimer{time.NewTimer(d)} }
func (systemClock) NewTicker(d time.Duration) Ticker       { return systemTicker{time.NewTicker(d)} }

type systemTimer struct{ t *time.Timer }

func (s systemTimer) C() <-chan time.Time        { return s.t.C }
func (s systemTimer) Stop() bool                 { return s.t.Stop() }
func (s systemTimer) Reset(d time.Duration) bool { return s.t.Reset(d) }

type systemTicker struct{ t *time.Ticker }

func (s systemTicker) C() <-chan time.Time   { return s.t.C }
func (s systemTicker) Stop()                 { s.t.Stop() }
func (s systemTicker) Reset(d time.Duration) { s.t.Reset(d) }
