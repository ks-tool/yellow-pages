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

package consuldns

import (
	"testing"
	"time"
)

// FuzzParseName ensures the DNS name parser never panics on arbitrary input.
func FuzzParseName(f *testing.F) {
	for _, s := range []string{
		"web.service.consul.", "_x._y.service.dc1.dc.consul.", "n.node.consul.",
		"", ".", "..consul.", "a.b.c.d.e.service.consul.", "тег.web.service.consul.",
	} {
		f.Add(s)
	}
	f.Fuzz(func(_ *testing.T, name string) {
		_ = parseName(name, "consul.")
	})
}

func TestRRLRefusesOverLimit(t *testing.T) {
	t.Parallel()
	r := newRateLimiter(2)
	fixed := time.Unix(1000, 0)
	r.now = func() time.Time { return fixed }

	if !r.allow("1.2.3.4") {
		t.Fatal("first query should be allowed")
	}
	if !r.allow("1.2.3.4") {
		t.Fatal("second query should be allowed")
	}
	if r.allow("1.2.3.4") {
		t.Error("third query in the window should be refused")
	}
	// A different client is independent.
	if !r.allow("5.6.7.8") {
		t.Error("other client should be allowed")
	}
}
