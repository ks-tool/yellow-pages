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

package server

import (
	"context"
	"testing"
	"time"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/transport"
)

// TestDrainWindowFlipsReadinessBeforeStopAccept verifies the lame-duck ordering:
// Stop flips readiness NOT_SERVING immediately, then blocks for the drain window
// before stopping accept (GracefulStop). Readiness is therefore off well before
// the server stops — and, in the agent assembly, before the deregister step.
func TestDrainWindowFlipsReadinessBeforeStopAccept(t *testing.T) {
	t.Parallel()
	fake := clock.NewFake(time.Unix(0, 0))
	comp := NewComponent(Options{
		Addr:        "127.0.0.1:0",
		Service:     New(memStore(t), testLogger()),
		Transport:   transport.Insecure(),
		DrainWindow: 5 * time.Second,
		Clock:       fake,
		Log:         testLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = comp.Start(ctx) }()
	addr := comp.Addr()
	if addr == nil {
		t.Fatal("server failed to bind")
	}
	conn, err := transport.Insecure().Dial(ctx, addr.String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	hc := healthpb.NewHealthClient(conn)

	stopped := make(chan error, 1)
	go func() { stopped <- comp.Stop(context.Background()) }()

	// Readiness must go NOT_SERVING promptly (before the drain window elapses).
	waitHealth(t, hc, healthpb.HealthCheckResponse_NOT_SERVING)

	select {
	case <-stopped:
		t.Fatal("Stop returned before the drain window elapsed")
	case <-time.After(100 * time.Millisecond):
	}

	// Release the drain window; Stop then completes.
	fake.BlockUntil(1)
	fake.Advance(5 * time.Second)
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("Stop: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not complete after the drain window")
	}
}

func waitHealth(t *testing.T, hc healthpb.HealthClient, want healthpb.HealthCheckResponse_ServingStatus) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		resp, err := hc.Check(context.Background(), &healthpb.HealthCheckRequest{})
		if err == nil && resp.GetStatus() == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("health did not reach %v in time", want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
