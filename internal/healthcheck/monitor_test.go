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

package healthcheck

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"
)

type chanKeeper struct{ calls chan string }

func (k chanKeeper) KeepAlive(_ context.Context, serviceID string) error {
	select {
	case k.calls <- serviceID:
	default:
	}
	return nil
}

func discardMonitor(t *testing.T, k Keeper) *Monitor {
	t.Helper()
	m := New(Options{Keeper: k, Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	t.Cleanup(func() { _ = m.Stop(context.Background()) })
	return m
}

func TestMonitorGatesOnCheck(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	k := chanKeeper{calls: make(chan string, 16)}
	m := discardMonitor(t, k)
	m.Set("web", []Definition{{Kind: KindTCP, Target: ln.Addr().String(), Interval: 20 * time.Millisecond}})

	if !m.Active("web") {
		t.Fatal("service with a check should be Active (excluded from blanket renew)")
	}

	// Passing check -> the lease is kept alive.
	select {
	case id := <-k.calls:
		if id != "web" {
			t.Errorf("kept alive %q, want web", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no keep-alive while the check passes")
	}

	// Repoint the check at an unreachable port -> it fails -> no further keep-alive.
	// 127.0.0.1:1 stays refused and can't be hijacked by a parallel test, unlike a
	// freed ephemeral port. Settle first so a trailing keep-alive from the old
	// (passing) check lands and gets drained before we assert quiet.
	m.Set("web", []Definition{{Kind: KindTCP, Target: "127.0.0.1:1", Timeout: 100 * time.Millisecond, Interval: 20 * time.Millisecond}})
	time.Sleep(100 * time.Millisecond)
	for len(k.calls) > 0 {
		<-k.calls
	}
	select {
	case <-k.calls:
		t.Error("kept the lease alive after the check started failing")
	case <-time.After(300 * time.Millisecond):
	}

	m.Remove("web")
	if m.Active("web") {
		t.Error("removed service should no longer be Active")
	}
}

func TestMonitorEmptyDefsInactive(t *testing.T) {
	t.Parallel()
	m := discardMonitor(t, chanKeeper{calls: make(chan string, 1)})
	m.Set("web", nil) // a TTL-only / no active check
	if m.Active("web") {
		t.Error("a service with no active checks must not be gated")
	}
}
