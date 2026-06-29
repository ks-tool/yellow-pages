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

package store

import (
	"errors"
	"testing"
	"time"

	"github.com/ks-tool/yellow-pages/internal/model"
)

func TestRegistryCapacity(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t, Options{MaxServices: 2})

	must(t, s.Register(reg("n1", 1, svc("web", "web", 80, 30*time.Second))))
	must(t, s.Register(reg("n1", 1, svc("api", "api", 81, 30*time.Second))))

	// A third NEW service exceeds the cap.
	if err := s.Register(reg("n1", 1, svc("db", "db", 5432, 30*time.Second))); !errors.Is(err, ErrCapacity) {
		t.Fatalf("over-cap register = %v, want ErrCapacity", err)
	}
	if got := s.Size(); got != 2 {
		t.Errorf("size = %d, want 2 (rejected service not added)", got)
	}

	// Re-registering an existing service (no new instance) is still allowed.
	if err := s.Register(reg("n1", 2, svc("web", "web", 90, 30*time.Second))); err != nil {
		t.Errorf("re-register at cap = %v, want nil", err)
	}
}

func BenchmarkStoreRegisterLookup(b *testing.B) {
	s := NewMemory(Options{DefaultTTL: 30 * time.Second})
	r := reg("n1", 1, svc("web", "web", 80, 30*time.Second))
	q := model.Query{Name: "web"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Register(r)
		_ = s.Lookup(q)
	}
}
