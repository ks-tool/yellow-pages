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
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/ks-tool/yellow-pages/internal/model"
)

type captureReg struct{ names []string }

func (c *captureReg) RegisterServices(_ context.Context, reg model.Registration) error {
	for _, s := range reg.Services {
		c.names = append(c.names, s.Name)
	}
	return nil
}

func TestLoadServiceDefs(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("web.json", `{"service":{"name":"web","port":80,"check":{"ttl":"30s"}}}`)
	write("group.json", `{"services":[{"name":"api","port":81},{"name":"db","port":5432}]}`)
	write("bad.json", `{ not valid`)
	write("ignored.txt", `{"service":{"name":"nope"}}`)

	reg := &captureReg{}
	n, err := LoadServiceDefs(context.Background(), dir, reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("LoadServiceDefs: %v", err)
	}
	if n != 3 {
		t.Errorf("registered %d, want 3 (web, api, db; bad/txt skipped)", n)
	}
	got := map[string]bool{}
	for _, name := range reg.names {
		got[name] = true
	}
	for _, want := range []string{"web", "api", "db"} {
		if !got[want] {
			t.Errorf("missing %q in %v", want, reg.names)
		}
	}
	if got["nope"] {
		t.Error("loaded a non-.json file")
	}
}
