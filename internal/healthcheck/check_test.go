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
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestProbeHTTP(t *testing.T) {
	t.Parallel()
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	defer ok.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }))
	defer bad.Close()

	if err := probe(context.Background(), Definition{Kind: KindHTTP, Target: ok.URL}, false); err != nil {
		t.Errorf("200 should pass: %v", err)
	}
	if err := probe(context.Background(), Definition{Kind: KindHTTP, Target: bad.URL}, false); err == nil {
		t.Error("503 should fail")
	}
	if err := probe(context.Background(), Definition{Kind: KindHTTP, Target: "http://127.0.0.1:1/x", Timeout: 200 * time.Millisecond}, false); err == nil {
		t.Error("unreachable should fail")
	}
}

func TestProbeTCP(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := probe(context.Background(), Definition{Kind: KindTCP, Target: addr}, false); err != nil {
		t.Errorf("open port should pass: %v", err)
	}
	_ = ln.Close()
	if err := probe(context.Background(), Definition{Kind: KindTCP, Target: addr, Timeout: 200 * time.Millisecond}, false); err == nil {
		t.Error("closed port should fail")
	}
}

func TestProbeUDPListening(t *testing.T) {
	t.Parallel()
	// A UDP listener that never replies -> no response within the window -> PASS
	// (Consul UDP semantics, best-effort).
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }()
	if err := probe(context.Background(), Definition{Kind: KindUDP, Target: pc.LocalAddr().String(), Timeout: 200 * time.Millisecond}, false); err != nil {
		t.Errorf("silent UDP listener should pass (best-effort): %v", err)
	}
}

func TestProbeScript(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	pass := filepath.Join(dir, "pass.sh")
	fail := filepath.Join(dir, "fail.sh")
	if err := os.WriteFile(pass, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil { //nolint:gosec // test fixture
		t.Fatal(err)
	}
	if err := os.WriteFile(fail, []byte("#!/bin/sh\nexit 3\n"), 0o700); err != nil { //nolint:gosec // test fixture
		t.Fatal(err)
	}

	// Disabled: any script check errors regardless of exit code.
	if err := probe(context.Background(), Definition{Kind: KindScript, Args: []string{pass}}, false); err == nil {
		t.Error("script check must be rejected when disabled")
	}
	// Enabled: exit 0 passes, non-zero fails.
	if err := probe(context.Background(), Definition{Kind: KindScript, Args: []string{pass}}, true); err != nil {
		t.Errorf("exit 0 should pass: %v", err)
	}
	if err := probe(context.Background(), Definition{Kind: KindScript, Args: []string{fail}}, true); err == nil {
		t.Error("exit 3 should fail")
	}
	// Relative path is rejected even when enabled.
	if err := probe(context.Background(), Definition{Kind: KindScript, Args: []string{"./pass.sh"}}, true); err == nil {
		t.Error("relative script path should be rejected")
	}
}
