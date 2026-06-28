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
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ks-tool/yellow-pages/internal/cred"
	"github.com/ks-tool/yellow-pages/internal/model"
)

func indexHeader(t *testing.T, rec *httptest.ResponseRecorder) uint64 {
	t.Helper()
	v, err := strconv.ParseUint(rec.Header().Get("X-Consul-Index"), 10, 64)
	if err != nil {
		t.Fatalf("X-Consul-Index = %q: %v", rec.Header().Get("X-Consul-Index"), err)
	}
	return v
}

func registerOn(t *testing.T, st interface {
	Register(model.Registration) error
}, nodeID, service string) {
	t.Helper()
	err := st.Register(model.Registration{
		Node:       model.Node{ID: nodeID, Datacenter: "dc1"},
		Services:   []model.ServiceInstance{{ID: service, Name: service, TTL: 30 * time.Second}},
		Generation: 1,
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
}

func TestBlockingWakesOnChange(t *testing.T) {
	t.Parallel()
	h, _, st, _ := newHandlerWith(t, Options{})
	registerOn(t, st, "agent-1", "web")

	first := indexHeader(t, do(t, h, http.MethodGet, "/v1/health/service/web?index=0", ""))

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- do(t, h, http.MethodGet, "/v1/health/service/web?index="+strconv.FormatUint(first, 10)+"&wait=2s", "")
	}()
	time.Sleep(50 * time.Millisecond)
	registerOn(t, st, "agent-2", "web") // changes the web set

	select {
	case rec := <-done:
		if got := indexHeader(t, rec); got <= first {
			t.Errorf("blocking index did not advance: %d -> %d", first, got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocking query did not wake on change")
	}
}

func TestBlockingNoChangeReturnsAfterWaitSameIndex(t *testing.T) {
	t.Parallel()
	h, _, st, _ := newHandlerWith(t, Options{})
	registerOn(t, st, "agent-1", "web")
	first := indexHeader(t, do(t, h, http.MethodGet, "/v1/health/service/web?index=0", ""))

	start := time.Now()
	rec := do(t, h, http.MethodGet, "/v1/health/service/web?index="+strconv.FormatUint(first, 10)+"&wait=150ms", "")
	if elapsed := time.Since(start); elapsed < 120*time.Millisecond {
		t.Errorf("returned too early (%s): no busy loop expected", elapsed)
	}
	if got := indexHeader(t, rec); got != first {
		t.Errorf("index changed without a change: %d -> %d", first, got)
	}
}

func TestBlockingWakesOnCreate(t *testing.T) {
	t.Parallel()
	h, _, st, _ := newHandlerWith(t, Options{})
	first := indexHeader(t, do(t, h, http.MethodGet, "/v1/health/service/ghost?index=0", ""))

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- do(t, h, http.MethodGet, "/v1/health/service/ghost?index="+strconv.FormatUint(first, 10)+"&wait=2s", "")
	}()
	time.Sleep(50 * time.Millisecond)
	registerOn(t, st, "agent-1", "ghost") // service first appears

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("blocking on a non-existent service did not wake on creation")
	}
}

func TestConsistencyModes(t *testing.T) {
	t.Parallel()
	h, reg, st, _ := newHandlerWith(t, Options{})
	registerOn(t, st, "agent-1", "web")
	reg.age = 5 * time.Second

	stale := do(t, h, http.MethodGet, "/v1/health/service/web?stale", "")
	if reg.lastMode != model.ConsistencyStale {
		t.Errorf("?stale mode = %v", reg.lastMode)
	}
	if lc := stale.Header().Get("X-Consul-LastContact"); lc != "5000" {
		t.Errorf("?stale LastContact = %q, want 5000", lc)
	}

	cons := do(t, h, http.MethodGet, "/v1/health/service/web?consistent", "")
	if reg.lastMode != model.ConsistencyConsistent {
		t.Errorf("?consistent mode = %v", reg.lastMode)
	}
	if lc := cons.Header().Get("X-Consul-LastContact"); lc != "0" {
		t.Errorf("?consistent LastContact = %q, want 0", lc)
	}
}

func TestTokenAcceptedAndEnforced(t *testing.T) {
	t.Parallel()

	// allow/disabled: a token is accepted, never enforced.
	h, _, _, _ := newHandlerWith(t, Options{})
	if rec := doWith(t, h, http.MethodPut, "/v1/agent/service/register", `{"Name":"web"}`,
		map[string]string{"X-Consul-Token": "anything"}); rec.Code != http.StatusOK {
		t.Errorf("allow mode rejected a token: %d", rec.Code)
	}

	// enforce: token maps to a principal that must own the node (agent-1).
	he, _, _, _ := newHandlerWith(t, Options{
		Authz:    cred.NewAuthorizer(cred.ModeEnforce),
		Identity: cred.NewIdentity(map[string]string{"owner": "agent-1", "other": "intruder"}),
	})
	cases := []struct {
		name  string
		token string
		want  int
	}{
		{"owner token", "owner", http.StatusOK},
		{"non-owner token", "other", http.StatusForbidden},
		{"no token", "", http.StatusForbidden},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hdr := map[string]string{}
			if tc.token != "" {
				hdr["Authorization"] = "Bearer " + tc.token
			}
			rec := doWith(t, he, http.MethodPut, "/v1/agent/service/register", `{"Name":"web"}`, hdr)
			if rec.Code != tc.want {
				t.Errorf("code = %d, want %d", rec.Code, tc.want)
			}
		})
	}
}

func TestWaiterCapReturns429(t *testing.T) {
	t.Parallel()
	h, _, st, _ := newHandlerWith(t, Options{MaxWaiters: 1})
	registerOn(t, st, "agent-1", "web")
	idx := strconv.FormatUint(indexHeader(t, do(t, h, http.MethodGet, "/v1/health/service/web?index=0", "")), 10)

	// First blocking query occupies the only waiter slot.
	blocking := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		blocking <- do(t, h, http.MethodGet, "/v1/health/service/web?index="+idx+"&wait=2s", "")
	}()
	time.Sleep(80 * time.Millisecond)

	// Second blocking query exceeds the cap.
	if rec := do(t, h, http.MethodGet, "/v1/health/service/web?index="+idx+"&wait=2s", ""); rec.Code != http.StatusTooManyRequests {
		t.Errorf("second blocking query = %d, want 429", rec.Code)
	}

	registerOn(t, st, "agent-2", "web") // release the first
	<-blocking
}

func doWith(t *testing.T, h http.Handler, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}
