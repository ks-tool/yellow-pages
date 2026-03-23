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

package plugin

import (
	"encoding/json"
	"fmt"
	"os"
)

const OptionsKey = "YELLOW_PAGES_PLUGIN_OPTIONS"

type SeedList struct {
	Seeds []string `json:"seeds"`
}

func UnmarshalOptionsFromEnvironmentVariables() (map[string]any, error) {
	opts := os.Getenv(OptionsKey)
	if len(opts) == 0 {
		return nil, nil
	}

	var options map[string]any
	if err := json.Unmarshal([]byte(opts), &options); err != nil {
		return nil, fmt.Errorf("failed to unmarshal plugin options: %w", err)
	}
	return options, nil
}
