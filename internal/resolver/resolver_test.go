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

package resolver

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// writeScript writes an executable /bin/sh script and returns its absolute path.
func writeScript(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "plugin.sh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}

// failing is a SeedProvider that always errors.
type failing struct{}

func (failing) Name() string                            { return "failing" }
func (failing) Seeds(context.Context) ([]string, error) { return nil, errors.New("boom") }

func TestStaticCopiesInput(t *testing.T) {
	t.Parallel()
	in := []string{"a:1", "b:1"}
	s := NewStatic(in)
	in[0] = "mutated"
	got, _ := s.Seeds(context.Background())
	if got[0] != "a:1" {
		t.Errorf("Static did not copy its input: %v", got)
	}
}

func TestMergedOrderDedupAndNonDestructive(t *testing.T) {
	t.Parallel()
	m := NewMerged(discardLogger(),
		NewStatic([]string{"a:1", "b:1"}),
		failing{}, // its failure must NOT drop the seeds around it
		NewStatic([]string{"b:1", "c:1", " "}),
	)
	got, err := m.Seeds(context.Background())
	if err != nil {
		t.Fatalf("Seeds: %v", err)
	}
	want := []string{"a:1", "b:1", "c:1"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("merged = %v, want %v", got, want)
	}
}

func TestMergedAllFail(t *testing.T) {
	t.Parallel()
	if _, err := NewMerged(discardLogger(), failing{}).Seeds(context.Background()); err == nil {
		t.Error("expected an error when every provider fails")
	}
}

func TestPluginValidOutput(t *testing.T) {
	t.Parallel()
	path := writeScript(t, `echo '{"seeds":["10.0.0.1:9900","10.0.0.2:9900"]}'`)
	got, err := NewPlugin(PluginConfig{Path: path}).Seeds(context.Background())
	if err != nil {
		t.Fatalf("Seeds: %v", err)
	}
	want := []string{"10.0.0.1:9900", "10.0.0.2:9900"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("plugin seeds = %v, want %v", got, want)
	}
}

func TestPluginOptionsViaEnvOnlyNoArgs(t *testing.T) {
	t.Parallel()
	// The script proves options arrive via the env var and that NO args are
	// passed (args count must be 0).
	path := writeScript(t, `
present=no
printf '%s' "$YP_SEED_PLUGIN_OPTIONS" | grep -q present && present=yes
echo "{\"seeds\":[\"args:$#\",\"opt:$present\"]}"`)

	got, err := NewPlugin(PluginConfig{Path: path, Options: map[string]any{"flag": "present"}}).Seeds(context.Background())
	if err != nil {
		t.Fatalf("Seeds: %v", err)
	}
	want := []string{"args:0", "opt:yes"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("plugin seeds = %v, want %v (options must arrive via env, with no args)", got, want)
	}
}

func TestPluginInvalidOutput(t *testing.T) {
	t.Parallel()
	path := writeScript(t, `echo 'this is not valid { yaml'`)
	if _, err := NewPlugin(PluginConfig{Path: path}).Seeds(context.Background()); err == nil {
		t.Error("expected a parse error on invalid output")
	}
}

func TestPluginTimeout(t *testing.T) {
	t.Parallel()
	path := writeScript(t, `sleep 5; echo '{"seeds":["late:1"]}'`)
	start := time.Now()
	_, err := NewPlugin(PluginConfig{Path: path, Timeout: 200 * time.Millisecond}).Seeds(context.Background())
	if err == nil {
		t.Fatal("expected a timeout error")
	}
	if time.Since(start) > 3*time.Second {
		t.Errorf("plugin was not killed promptly on timeout (took %s)", time.Since(start))
	}
}

func TestPluginNonZeroExit(t *testing.T) {
	t.Parallel()
	path := writeScript(t, `echo '{"seeds":[]}'; exit 1`)
	if _, err := NewPlugin(PluginConfig{Path: path}).Seeds(context.Background()); err == nil {
		t.Error("expected an error on non-zero exit")
	}
}

func TestPluginPathValidation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	nonExec := filepath.Join(dir, "data.txt")
	if err := os.WriteFile(nonExec, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	cases := map[string]string{
		"empty":          "",
		"relative":       "plugin.sh",
		"missing":        filepath.Join(dir, "nope"),
		"directory":      dir,
		"not executable": nonExec,
	}
	for name, path := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewPlugin(PluginConfig{Path: path}).Seeds(context.Background()); err == nil {
				t.Errorf("%s: expected a path-validation error", name)
			}
		})
	}
}

func TestSeedSetCombinations(t *testing.T) {
	t.Parallel()
	pluginPath := writeScript(t, `echo '{"seeds":["b:1","a:1"]}'`) // overlaps + reorders static

	cases := []struct {
		name      string
		providers []SeedProvider
		want      []string
	}{
		{
			name:      "static only",
			providers: []SeedProvider{NewStatic([]string{"a:1", "b:1"})},
			want:      []string{"a:1", "b:1"},
		},
		{
			name:      "plugin plus static (static first, dedup)",
			providers: []SeedProvider{NewStatic([]string{"a:1"}), NewPlugin(PluginConfig{Path: pluginPath})},
			want:      []string{"a:1", "b:1"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := NewMerged(discardLogger(), tc.providers...).Seeds(context.Background())
			if err != nil {
				t.Fatalf("Seeds: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("seed set = %v, want %v", got, tc.want)
			}
		})
	}
}
