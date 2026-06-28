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

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// consulImage is the pinned real-Consul image for conformance/shadow-diff.
const consulImage = "hashicorp/consul:1.20"

// startConsul launches a real Consul agent (-dev) in a container and returns its
// HTTP address. It skips the test when Docker is unavailable.
func startConsul(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        consulImage,
		ExposedPorts: []string{"8500/tcp"},
		Cmd:          []string{"agent", "-dev", "-client", "0.0.0.0"},
		WaitingFor:   wait.ForHTTP("/v1/status/leader").WithPort("8500/tcp").WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("skipping: cannot start Consul container (Docker unavailable?): %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "8500")
	if err != nil {
		t.Fatal(err)
	}
	return "http://" + host + ":" + port.Port()
}
