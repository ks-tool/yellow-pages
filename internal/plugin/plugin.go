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

// import "github.com/ks-tool/yellow-pages/internal/plugin"

package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/ks-tool/yellow-pages/plugin"
)

// ExecPlugin runs an external binary and returns the list of seeds.
func ExecPlugin(name string, options map[string]any) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, name)

	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stdout

	if len(options) > 0 {
		optionBytes, err := json.Marshal(options)
		if err != nil {
			return nil, err
		}
		cmd.Env = append(os.Environ(), fmt.Sprintf("%s=%s", plugin.OptionsKey, optionBytes))
	}

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("plugin %s failed: %w\noutput: %s", name, err, stdout.String())
	}

	var seeds plugin.SeedList
	if err := json.Unmarshal(stdout.Bytes(), &seeds); err != nil {
		return nil, fmt.Errorf("failed to parse plugin output: %w", err)
	}
	if len(seeds.Seeds) == 0 {
		return nil, fmt.Errorf("plugin returned empty seed list")
	}
	return seeds.Seeds, nil
}
