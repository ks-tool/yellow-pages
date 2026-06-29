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
	"context"
	"log/slog"
	"net"

	"github.com/miekg/dns"
)

// Component serves the Consul DNS interface on UDP and TCP (default
// 127.0.0.1:8600). It satisfies app.Component.
type Component struct {
	addr    string
	handler dns.Handler
	log     *slog.Logger

	udp, tcp *dns.Server
	ready    chan struct{}
	udpAddr  net.Addr
}

// NewComponent builds the DNS component for handler on addr.
func NewComponent(addr string, handler dns.Handler, log *slog.Logger) *Component {
	if log == nil {
		log = slog.Default()
	}
	return &Component{addr: addr, handler: handler, log: log, ready: make(chan struct{})}
}

// Name identifies the component.
func (c *Component) Name() string { return "consul-dns" }

// Addr blocks until the UDP listener is bound and returns its address (tests).
func (c *Component) Addr() net.Addr {
	<-c.ready
	return c.udpAddr
}

// Start binds UDP+TCP and serves until ctx is cancelled.
func (c *Component) Start(ctx context.Context) error {
	pc, err := net.ListenPacket("udp", c.addr)
	if err != nil {
		close(c.ready)
		return err
	}
	l, err := net.Listen("tcp", c.addr)
	if err != nil {
		_ = pc.Close()
		close(c.ready)
		return err
	}
	c.udp = &dns.Server{PacketConn: pc, Handler: c.handler}
	c.tcp = &dns.Server{Listener: l, Handler: c.handler}
	c.udpAddr = pc.LocalAddr()
	close(c.ready)
	c.log.Info("consul DNS serving", "addr", c.udpAddr.String())

	errCh := make(chan error, 2)
	go func() { errCh <- c.udp.ActivateAndServe() }()
	go func() { errCh <- c.tcp.ActivateAndServe() }()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}

// Stop shuts both servers down within ctx's deadline.
func (c *Component) Stop(ctx context.Context) error {
	if c.udp != nil {
		_ = c.udp.ShutdownContext(ctx)
	}
	if c.tcp != nil {
		_ = c.tcp.ShutdownContext(ctx)
	}
	return nil
}
