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

package ratelimit

import (
	"testing"
	"time"

	"github.com/ks-tool/yellow-pages/internal/clock"
)

func TestLimiter(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(time.Unix(1000, 0))
	l := New(2, clk)

	if !l.Allow("a") {
		t.Fatal("first request for key a should be allowed")
	}
	if !l.Allow("a") {
		t.Fatal("second request for key a should be allowed")
	}
	if l.Allow("a") {
		t.Error("third for key a within the window should be denied")
	}
	if !l.Allow("b") {
		t.Error("a different key is independent")
	}

	// The window resets on the injected clock (past the 1s boundary).
	clk.Advance(2 * time.Second)
	if !l.Allow("a") {
		t.Error("key a after window advance should be allowed")
	}
}

func TestLimiterDisabled(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(time.Unix(1000, 0))
	for _, l := range []*Limiter{nil, New(0, clk), New(-1, clk)} {
		for i := 0; i < 100; i++ {
			if !l.Allow("x") {
				t.Fatalf("limit<=0 (or nil) must allow everything, denied at %d", i)
			}
		}
	}
}
