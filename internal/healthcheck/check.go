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

// Package healthcheck runs agent-side active health checks (HTTP / TCP / UDP /
// exec) for the services an agent hosts, and gates their liveness: while a
// service's checks pass the agent keeps its lease alive; when they fail the
// agent stops renewing so the registry lets it go critical (and, after grace,
// drops it). This is the Consul "active check" model layered on the per-service
// TTL lease (M13). Exec checks run an arbitrary binary, so they are OFF unless
// explicitly enabled.
package healthcheck

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"path/filepath"
	"time"
)

// Kind is the probe type of a check.
type Kind string

const (
	// KindHTTP issues an HTTP(S) request; a 2xx response passes.
	KindHTTP Kind = "http"
	// KindTCP opens a TCP connection; a successful dial passes.
	KindTCP Kind = "tcp"
	// KindUDP sends a UDP datagram; passes unless the port is unreachable
	// (best-effort — a silent UDP listener also passes).
	KindUDP Kind = "udp"
	// KindScript runs a local binary; exit 0 passes. Requires opt-in.
	KindScript Kind = "script"

	defaultInterval = 10 * time.Second
	defaultTimeout  = 5 * time.Second
)

// Definition is one parsed check. Target is a URL for HTTP and a host:port for
// TCP/UDP; Args is the argv for a script check.
type Definition struct {
	Kind          Kind
	Target        string
	Method        string // HTTP method (default GET)
	Header        map[string][]string
	Args          []string // script argv; Args[0] must be an absolute path
	Interval      time.Duration
	Timeout       time.Duration
	TLSSkipVerify bool
}

func (d Definition) interval() time.Duration {
	if d.Interval > 0 {
		return d.Interval
	}
	return defaultInterval
}

func (d Definition) timeout() time.Duration {
	if d.Timeout > 0 {
		return d.Timeout
	}
	if d.Interval > 0 && d.Interval < defaultTimeout {
		return d.Interval
	}
	return defaultTimeout
}

// probe runs one check within its timeout and returns nil when it passes.
func probe(ctx context.Context, d Definition, enableScript bool) error {
	cctx, cancel := context.WithTimeout(ctx, d.timeout())
	defer cancel()
	switch d.Kind {
	case KindHTTP:
		return probeHTTP(cctx, d)
	case KindTCP:
		return probeTCP(cctx, d.Target)
	case KindUDP:
		return probeUDP(cctx, d.Target)
	case KindScript:
		if !enableScript {
			return errors.New("script checks are disabled (set enable_script_checks)")
		}
		return probeScript(cctx, d.Args)
	default:
		return fmt.Errorf("unknown check kind %q", d.Kind)
	}
}

func probeHTTP(ctx context.Context, d Definition) error {
	method := d.Method
	if method == "" {
		method = http.MethodGet
	}
	req, err := http.NewRequestWithContext(ctx, method, d.Target, nil)
	if err != nil {
		return err
	}
	for k, vs := range d.Header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: d.TLSSkipVerify}, //nolint:gosec // operator opt-in per check
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("http status %d", resp.StatusCode)
	}
	return nil
}

func probeTCP(ctx context.Context, addr string) error {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	return conn.Close()
}

// probeUDP sends a datagram and treats a port-unreachable error as a failure;
// no response within the timeout is treated as PASSING (Consul UDP semantics).
func probeUDP(ctx context.Context, addr string) error {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "udp", addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	if _, err := conn.Write([]byte("yp-health\n")); err != nil {
		return err
	}
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err != nil {
		var nerr net.Error
		if errors.As(err, &nerr) && nerr.Timeout() {
			return nil // no reply within the window — passing (best-effort)
		}
		return err // e.g. connection refused -> port unreachable -> failing
	}
	return nil
}

func probeScript(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("script check has no args")
	}
	if !filepath.IsAbs(args[0]) {
		return fmt.Errorf("script check command must be an absolute path: %q", args[0])
	}
	cmd := exec.CommandContext(ctx, args[0], args[1:]...) //nolint:gosec // absolute path, no shell; gated by enable_script_checks
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%w: %s", err, string(out))
	}
	return nil
}
