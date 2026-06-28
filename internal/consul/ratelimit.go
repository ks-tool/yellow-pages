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

package consul

import (
	"net"
	"net/http"
	"sync"
	"time"
)

// rateLimiter is a per-client fixed-window request limiter for the Consul HTTP
// surface (read + write DoS guard). A zero limit disables it.
type rateLimiter struct {
	limit int

	mu      sync.Mutex
	counts  map[string]int
	resetAt time.Time
}

func newRateLimiter(perSecond int) *rateLimiter {
	return &rateLimiter{limit: perSecond, counts: map[string]int{}}
}

func (r *rateLimiter) allow(client string) bool {
	if r == nil || r.limit <= 0 {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	if now.After(r.resetAt) {
		r.counts = make(map[string]int, len(r.counts))
		r.resetAt = now.Add(time.Second)
	}
	r.counts[client]++
	return r.counts[client] <= r.limit
}

func remoteIP(r *http.Request) string {
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}
