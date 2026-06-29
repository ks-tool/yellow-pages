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

// Package httpcomp is the one app.Component wrapper for serving an http.Handler
// over the standard library net/http with graceful shutdown — shared by the
// Consul HTTP and Prometheus /metrics surfaces (each had its own identical copy).
package httpcomp

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// Component serves a handler on addr until its context is cancelled, then drains
// within the Stop deadline. It satisfies app.Component (Name/Start/Stop).
type Component struct {
	name string
	addr string
	log  *slog.Logger
	srv  *http.Server
}

// New builds an HTTP component named name, serving handler on addr.
func New(name, addr string, handler http.Handler, log *slog.Logger) *Component {
	if log == nil {
		log = slog.Default()
	}
	return &Component{
		name: name,
		addr: addr,
		log:  log,
		srv: &http.Server{
			Addr:              addr,
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,
		},
	}
}

// Name identifies the component.
func (c *Component) Name() string { return c.name }

// Start binds the listener and serves until ctx is cancelled.
func (c *Component) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", c.addr)
	if err != nil {
		return err
	}
	c.log.Info("http server serving", "component", c.name, "addr", lis.Addr().String())

	errCh := make(chan error, 1)
	go func() {
		if serveErr := c.srv.Serve(lis); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errCh <- serveErr
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}

// Stop gracefully shuts the HTTP server down within ctx's deadline.
func (c *Component) Stop(ctx context.Context) error {
	return c.srv.Shutdown(ctx)
}
