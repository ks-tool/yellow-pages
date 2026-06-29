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

// Package ratelimit is the one per-client fixed-window QPS limiter shared by the
// Consul HTTP, Consul DNS and bootstrap surfaces (each had a near-identical copy
// that had drifted on the time source). It is driven by the clock seam so the
// window is deterministically testable with a fake clock.
package ratelimit

import (
	"sync"
	"time"

	"github.com/ks-tool/yellow-pages/internal/clock"
)

// Limiter caps requests per second per key. A nil Limiter or a non-positive
// limit allows everything (the "disabled" state).
type Limiter struct {
	limit int
	clk   clock.Clock

	mu      sync.Mutex
	counts  map[string]int
	resetAt time.Time
}

// New builds a limiter of perSecond requests per key. A nil clock uses System.
func New(perSecond int, clk clock.Clock) *Limiter {
	if clk == nil {
		clk = clock.System()
	}
	return &Limiter{limit: perSecond, clk: clk, counts: map[string]int{}}
}

// Allow reports whether a request from key may proceed this 1s window.
func (l *Limiter) Allow(key string) bool {
	if l == nil || l.limit <= 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.clk.Now()
	if now.After(l.resetAt) {
		l.counts = make(map[string]int, len(l.counts))
		l.resetAt = now.Add(time.Second)
	}
	l.counts[key]++
	return l.counts[key] <= l.limit
}
