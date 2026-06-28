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
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/model"
)

func TestGCLoopReapsExpired(t *testing.T) {
	t.Parallel()
	f := clock.NewFake(epoch)
	s := NewMemory(Options{Clock: f, DefaultTTL: 10 * time.Second})
	if err := s.Register(reg("n1", 1, svc("web", "web", 8080, 10*time.Second))); err != nil {
		t.Fatalf("Register: %v", err)
	}

	loop := NewGCLoop(s, 5*time.Second, f, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = loop.Start(ctx) }()

	f.BlockUntil(1)             // wait until the GC ticker is armed
	f.Advance(20 * time.Second) // past TTL: the next tick reaps

	deadline := time.Now().Add(2 * time.Second)
	for {
		if len(s.Lookup(model.Query{Name: "web"}).Entries) == 0 {
			return // reaped
		}
		if time.Now().After(deadline) {
			t.Fatal("GC loop did not reap the expired registration")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
