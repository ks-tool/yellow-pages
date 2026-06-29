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

package migrate

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// consulNode / consulService mirror the Consul health-entry fields we backfill.
type consulNode struct {
	Node, Address, Datacenter string
	Meta                      map[string]string
}

type consulService struct {
	ID, Service, Address string
	Port                 int
	Tags                 []string
	Meta                 map[string]string
}

type healthEntry struct {
	Node    consulNode
	Service consulService
}

// registerBody is the /v1/catalog/register payload (subset).
type registerBody struct {
	Node       string
	Address    string
	Datacenter string
	NodeMeta   map[string]string
	Service    serviceBody
}

type serviceBody struct {
	ID      string
	Name    string `json:"Service"`
	Address string
	Port    int
	Tags    []string
	Meta    map[string]string
}

// Import reads the catalog from a Consul-compatible source (from, e.g.
// http://127.0.0.1:8500) and backfills it into a yellow-pages target (to) via
// /v1/catalog/register. It returns the number of instances written. Existing
// registrations are idempotent. Intended as a one-shot pre-cutover step.
func Import(ctx context.Context, client *http.Client, from, to string) (int, error) {
	from = strings.TrimRight(from, "/")
	to = strings.TrimRight(to, "/")

	var services map[string][]string
	if err := getJSON(ctx, client, from+"/v1/catalog/services", &services); err != nil {
		return 0, fmt.Errorf("migrate: read services: %w", err)
	}

	written := 0
	for name := range services {
		var entries []healthEntry
		if err := getJSON(ctx, client, from+"/v1/health/service/"+name, &entries); err != nil {
			return written, fmt.Errorf("migrate: read %q: %w", name, err)
		}
		for _, e := range entries {
			body := registerBody{
				Node: e.Node.Node, Address: e.Node.Address, Datacenter: e.Node.Datacenter, NodeMeta: e.Node.Meta,
				Service: serviceBody{
					ID: e.Service.ID, Name: e.Service.Service, Address: e.Service.Address,
					Port: e.Service.Port, Tags: e.Service.Tags, Meta: e.Service.Meta,
				},
			}
			if err := putJSON(ctx, client, to+"/v1/catalog/register", body); err != nil {
				return written, fmt.Errorf("migrate: register %s/%s: %w", e.Node.Node, e.Service.ID, err)
			}
			written++
		}
	}
	return written, nil
}

func getJSON(ctx context.Context, client *http.Client, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func putJSON(ctx context.Context, client *http.Client, url string, body any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("PUT %s: status %d: %s", url, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return nil
}
