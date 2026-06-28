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

// Package app provides the process lifecycle runner: a small set of Components
// started under one errgroup, with signal handling, first-failure-cancels-all,
// and a bounded graceful shutdown. It deliberately stays minimal — wiring the
// actual served components in starts at M3.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/ks-tool/yellow-pages/internal/clock"
)

// ErrShutdownTimeout is returned when components do not stop within the bounded
// graceful-shutdown window.
var ErrShutdownTimeout = errors.New("graceful shutdown timed out")

// Component is a long-running unit of the process. Start should block for the
// component's lifetime and return when ctx is cancelled (returning ctx.Err() or
// nil on a clean stop). Stop performs graceful teardown within the deadline of
// the context it is given.
type Component interface {
	Name() string
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// App runs Components for the lifetime of the process.
type App struct {
	components      []Component
	log             *slog.Logger
	clk             clock.Clock
	shutdownTimeout time.Duration
	signals         []os.Signal
}

// Option configures an App.
type Option func(*App)

// WithComponents appends components to run.
func WithComponents(cs ...Component) Option {
	return func(a *App) { a.components = append(a.components, cs...) }
}

// WithLogger sets the logger (ignored if nil).
func WithLogger(l *slog.Logger) Option {
	return func(a *App) {
		if l != nil {
			a.log = l
		}
	}
}

// WithClock sets the clock seam (ignored if nil). Tests inject a fake clock to
// drive the shutdown timeout deterministically.
func WithClock(c clock.Clock) Option {
	return func(a *App) {
		if c != nil {
			a.clk = c
		}
	}
}

// WithShutdownTimeout bounds graceful shutdown (ignored if <= 0).
func WithShutdownTimeout(d time.Duration) Option {
	return func(a *App) {
		if d > 0 {
			a.shutdownTimeout = d
		}
	}
}

// WithSignals overrides the signals that trigger shutdown (used in tests).
func WithSignals(sig ...os.Signal) Option {
	return func(a *App) { a.signals = sig }
}

// New constructs an App with sane defaults.
func New(opts ...Option) *App {
	a := &App{
		log:             slog.Default(),
		clk:             clock.System(),
		shutdownTimeout: 15 * time.Second,
		signals:         []os.Signal{os.Interrupt, syscall.SIGTERM},
	}
	for _, o := range opts {
		o(a)
	}
	return a
}

// Run starts all components and blocks until one of: an OS signal arrives, the
// parent ctx is cancelled, or any component's Start returns an error. In every
// case the remaining components are stopped within the bounded shutdown window
// (first-failure-cancels-all). Run returns the first non-nil cause.
func (a *App) Run(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, a.signals...)
	defer stop()

	g, gctx := errgroup.WithContext(ctx)
	for _, c := range a.components {
		g.Go(func() error {
			a.log.Info("component starting", "component", c.Name())
			err := c.Start(gctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				a.log.Error("component failed", "component", c.Name(), "error", err)
				return fmt.Errorf("component %q: %w", c.Name(), err)
			}
			a.log.Info("component stopped", "component", c.Name())
			return nil
		})
	}

	// Block until a shutdown trigger: signal/parent-cancel, or first failure
	// (errgroup cancels gctx with the failing error as its cause).
	<-gctx.Done()
	a.log.Info("shutdown initiated", "cause", context.Cause(gctx))

	stopErr := a.stopComponents()

	runErr := g.Wait()
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		return runErr
	}
	return stopErr
}

// stopComponents stops components in reverse start order within the bounded
// shutdown window. The window is enforced via the clock seam so tests can drive
// it deterministically; components that ignore the cancelled context are
// abandoned (the process is exiting anyway).
func (a *App) stopComponents() error {
	if len(a.components) == 0 {
		return nil
	}

	stopCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		var errs []error
		for i := len(a.components) - 1; i >= 0; i-- {
			c := a.components[i]
			a.log.Info("stopping component", "component", c.Name())
			if err := c.Stop(stopCtx); err != nil && !errors.Is(err, context.Canceled) {
				errs = append(errs, fmt.Errorf("stop %q: %w", c.Name(), err))
			}
		}
		done <- errors.Join(errs...)
	}()

	select {
	case err := <-done:
		return err
	case <-a.clk.After(a.shutdownTimeout):
		cancel() // signal still-running Stops to give up
		a.log.Warn("graceful shutdown timed out", "timeout", a.shutdownTimeout)
		return ErrShutdownTimeout
	}
}
