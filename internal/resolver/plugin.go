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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

// DefaultOptionsEnv is the environment variable carrying the YAML-encoded plugin
// options. YAML is a JSON superset, so the plugin may parse it with any YAML or
// JSON reader; its own output is read the same way.
const DefaultOptionsEnv = "YP_SEED_PLUGIN_OPTIONS"

// defaultPluginTimeout bounds a single plugin invocation.
const defaultPluginTimeout = 10 * time.Second

// PluginConfig configures a Plugin provider.
type PluginConfig struct {
	// Path is the absolute path to the plugin executable.
	Path string
	// Options are passed to the plugin as YAML in the OptionsEnv variable.
	Options map[string]any
	// Timeout bounds one invocation (default 10s).
	Timeout time.Duration
	// OptionsEnv overrides the options environment variable name.
	OptionsEnv string
}

// Plugin runs an external executable that prints {"seeds":[...]} on stdout. The
// executable is run directly (no shell), with a bounded timeout, its options
// passed only via the environment.
type Plugin struct {
	cfg PluginConfig
}

// NewPlugin builds a Plugin provider, applying defaults.
func NewPlugin(cfg PluginConfig) *Plugin {
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultPluginTimeout
	}
	if cfg.OptionsEnv == "" {
		cfg.OptionsEnv = DefaultOptionsEnv
	}
	return &Plugin{cfg: cfg}
}

// Name identifies the provider.
func (p *Plugin) Name() string { return "plugin:" + filepath.Base(p.cfg.Path) }

// Seeds runs the plugin and parses its {"seeds":[...]} output.
func (p *Plugin) Seeds(ctx context.Context) ([]string, error) {
	if err := validatePath(p.cfg.Path); err != nil {
		return nil, err
	}

	optionsYAML, err := yaml.Marshal(p.cfg.Options)
	if err != nil {
		return nil, fmt.Errorf("resolver: encode plugin options: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, p.cfg.Timeout)
	defer cancel()

	// Run the executable directly — no shell, no arguments — so options can only
	// reach it through the environment and are never interpreted by a shell.
	cmd := exec.CommandContext(ctx, p.cfg.Path) //nolint:gosec // validated absolute path, no shell
	cmd.Env = append(os.Environ(), p.cfg.OptionsEnv+"="+string(optionsYAML))

	out, err := cmd.Output()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("resolver: plugin %q timed out after %s", p.cfg.Path, p.cfg.Timeout)
		}
		return nil, fmt.Errorf("resolver: plugin %q failed: %w", p.cfg.Path, err)
	}

	var parsed struct {
		Seeds []string `yaml:"seeds"`
	}
	if err := yaml.Unmarshal(out, &parsed); err != nil {
		return nil, fmt.Errorf("resolver: parse plugin %q output: %w", p.cfg.Path, err)
	}
	return parsed.Seeds, nil
}

// validatePath rejects anything but an existing, absolute, executable regular
// file — defending against PATH hijack and accidental directory/script targets.
func validatePath(path string) error {
	if path == "" {
		return fmt.Errorf("resolver: plugin path is empty")
	}
	if !filepath.IsAbs(path) {
		return fmt.Errorf("resolver: plugin path %q must be absolute", path)
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("resolver: plugin %q: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("resolver: plugin %q is not a regular file", path)
	}
	if info.Mode().Perm()&0o111 == 0 {
		return fmt.Errorf("resolver: plugin %q is not executable", path)
	}
	return nil
}
