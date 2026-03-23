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

// import "github.com/ks-tool/yellow-pages/internal/config"

package config

import "github.com/ks-tool/yellow-pages/internal/plugin"

func (c *Cluster) ResolveSeedsFromPlugin() error {
	if c.DiscoverySeeds == nil {
		return nil
	}

	var err error
	c.Seeds, err = plugin.ExecPlugin(c.DiscoverySeeds.Name, c.DiscoverySeeds.Options)
	return err
}
