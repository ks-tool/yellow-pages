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
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// Component serves the Consul-compatible HTTP API (default 127.0.0.1:8500) on
// the standard library net/http — no framework. It satisfies app.Component.
type Component struct {
	addr string
	log  *slog.Logger
	srv  *http.Server
}

// NewComponent builds the Consul HTTP listener for handler on addr.
func NewComponent(addr string, handler http.Handler, log *slog.Logger) *Component {
	if log == nil {
		log = slog.Default()
	}
	return &Component{
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
func (c *Component) Name() string { return "consul-http" }

// Start serves until ctx is cancelled.
func (c *Component) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", c.addr)
	if err != nil {
		return err
	}
	c.log.Info("consul HTTP serving", "addr", lis.Addr().String())

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
