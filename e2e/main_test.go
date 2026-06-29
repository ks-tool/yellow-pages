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

// Package e2e holds end-to-end tests: a real yp binary plus, for the conformance
// and migration suites, a REAL Consul container (testcontainers). Run with:
//
//	cd e2e && go test ./...      # needs Docker for the Consul-backed tests
//
// Tests that need Docker skip cleanly when it is unavailable.
package e2e

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// ypBinary is the path to the freshly built yp binary (set in TestMain).
var ypBinary string

func TestMain(m *testing.M) {
	bin, cleanup, err := buildYP()
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e: build yp: %v\n", err)
		os.Exit(1)
	}
	ypBinary = bin
	code := m.Run()
	cleanup()
	os.Exit(code)
}

// buildYP compiles the yp binary from the parent module into a temp dir.
func buildYP() (string, func(), error) {
	dir, err := os.MkdirTemp("", "yp-e2e-*")
	if err != nil {
		return "", nil, err
	}
	bin := filepath.Join(dir, "yp")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/yp")
	cmd.Dir = ".." // build inside the main module
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", nil, err
	}
	return bin, func() { _ = os.RemoveAll(dir) }, nil
}

// ypNode is a running yp process and its surface addresses.
type ypNode struct {
	consulHTTP string // http://127.0.0.1:PORT
	grpc       string // 127.0.0.1:PORT
	dnsPort    int
}

// startYPSeed launches a yp seed with the Consul HTTP, gRPC and DNS surfaces on
// free ports, and waits until it answers /v1/status/leader.
func startYPSeed(t *testing.T) ypNode {
	t.Helper()
	grpcPort, httpPort, dnsPort := freePort(t), freePort(t), freePort(t)
	cfg := fmt.Sprintf(`role: seed
node_name: yp-seed
datacenter: dc1
cluster: { name: e2e }
listeners:
  grpc: { port: %d }
  consul_http: { enabled: true, port: %d }
  dns: { enabled: true, port: %d }
  metrics: { enabled: false }
`, grpcPort, httpPort, dnsPort)

	cfgPath := filepath.Join(t.TempDir(), "seed.yaml")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, ypBinary, "--config", cfgPath)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("start yp: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		_ = cmd.Wait()
	})

	node := ypNode{
		consulHTTP: fmt.Sprintf("http://127.0.0.1:%d", httpPort),
		grpc:       fmt.Sprintf("127.0.0.1:%d", grpcPort),
		dnsPort:    dnsPort,
	}
	waitHTTP(t, node.consulHTTP+"/v1/status/leader")
	return node
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

func waitHTTP(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url) //nolint:gosec,noctx // test-local loopback
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("yp did not become ready at %s", url)
}
