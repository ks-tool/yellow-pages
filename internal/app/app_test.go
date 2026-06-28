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

package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ks-tool/yellow-pages/internal/clock"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// blockingComponent blocks in Start until its context is cancelled, recording
// that it was cancelled. Stop is configurable.
type blockingComponent struct {
	name      string
	started   chan struct{}
	cancelled atomic.Bool
	stopped   atomic.Bool
	stopFn    func(ctx context.Context) error
}

func newBlocking(name string) *blockingComponent {
	return &blockingComponent{name: name, started: make(chan struct{})}
}

func (c *blockingComponent) Name() string { return c.name }

func (c *blockingComponent) Start(ctx context.Context) error {
	close(c.started)
	<-ctx.Done()
	c.cancelled.Store(true)
	return ctx.Err()
}

func (c *blockingComponent) Stop(ctx context.Context) error {
	c.stopped.Store(true)
	if c.stopFn != nil {
		return c.stopFn(ctx)
	}
	return nil
}

func TestRun_FirstFailureCancelsAll(t *testing.T) {
	t.Parallel()

	boom := errors.New("boom")
	blocker := newBlocking("blocker")
	failer := &funcComponent{
		name:  "failer",
		start: func(context.Context) error { return boom },
	}

	a := New(
		WithLogger(quietLogger()),
		WithClock(clock.NewFake(time.Unix(0, 0))),
		WithComponents(blocker, failer),
	)

	err := a.Run(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want it to wrap %v", err, boom)
	}
	if !blocker.cancelled.Load() {
		t.Fatal("blocker was not cancelled by the failing component")
	}
	if !blocker.stopped.Load() {
		t.Fatal("blocker was not stopped during shutdown")
	}
}

func TestRun_GracefulShutdownOnCancel(t *testing.T) {
	t.Parallel()

	c1 := newBlocking("c1")
	c2 := newBlocking("c2")

	a := New(
		WithLogger(quietLogger()),
		WithClock(clock.NewFake(time.Unix(0, 0))),
		WithComponents(c1, c2),
	)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- a.Run(ctx) }()

	<-c1.started
	<-c2.started
	cancel() // mimic SIGTERM via parent cancellation

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run error = %v, want nil on clean shutdown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
	if !c1.stopped.Load() || !c2.stopped.Load() {
		t.Fatal("not all components were stopped")
	}
}

func TestRun_BoundedShutdownTimeout(t *testing.T) {
	t.Parallel()

	fake := clock.NewFake(time.Unix(0, 0))
	release := make(chan struct{})
	hang := newBlocking("hang")
	hang.stopFn = func(context.Context) error {
		<-release // ignore the context; never stops until released
		return nil
	}

	a := New(
		WithLogger(quietLogger()),
		WithClock(fake),
		WithShutdownTimeout(5*time.Second),
		WithComponents(hang),
	)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- a.Run(ctx) }()

	<-hang.started
	cancel() // trigger shutdown; Stop will hang

	// Wait until Run has armed the shutdown-timeout waiter, then trip it.
	fake.BlockUntil(1)
	fake.Advance(5 * time.Second)

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrShutdownTimeout) {
			t.Fatalf("Run error = %v, want ErrShutdownTimeout", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after shutdown timeout")
	}
	close(release)
}

func TestRun_NoComponentsReturnsOnCancel(t *testing.T) {
	t.Parallel()

	a := New(WithLogger(quietLogger()), WithClock(clock.NewFake(time.Unix(0, 0))))

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- a.Run(ctx) }()
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return for empty component set")
	}
}

// funcComponent is a Component backed by closures.
type funcComponent struct {
	name  string
	start func(ctx context.Context) error
	stop  func(ctx context.Context) error
}

func (c *funcComponent) Name() string { return c.name }

func (c *funcComponent) Start(ctx context.Context) error {
	if c.start != nil {
		return c.start(ctx)
	}
	<-ctx.Done()
	return ctx.Err()
}

func (c *funcComponent) Stop(ctx context.Context) error {
	if c.stop != nil {
		return c.stop(ctx)
	}
	return nil
}
