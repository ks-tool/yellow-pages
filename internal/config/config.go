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

import (
	"encoding/json/v2"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Cluster  `json:",inline" yaml:",inline"`
	Services []Service `json:"services,omitempty" yaml:"services,omitempty"`
	GRPCPort uint16    `json:"port,omitempty" yaml:"port,omitempty"`

	TTLSeconds           int64 `json:"ttl_seconds,omitempty" yaml:"ttlSeconds,omitempty"`
	HeartbeatIntervalSec int64 `json:"heartbeat_interval_sec,omitempty" yaml:"heartbeatIntervalSec,omitempty"`

	Seed bool `json:"seed,omitempty" yaml:"seed,omitempty"`
	// Clusters list. For seed mode only
	Clusters []Cluster `json:"clusters,omitempty" yaml:"clusters,omitempty"`
}

type Cluster struct {
	Name           string        `json:"cluster_name" yaml:"clusterName"`
	Datacenter     string        `json:"datacenter" yaml:"datacenter"`
	Seeds          []string      `json:"seeds,omitempty" yaml:"seeds,omitempty"`
	DiscoverySeeds *PluginConfig `json:"discovery,omitempty" yaml:"discovery,omitempty"`
}

type PluginConfig struct {
	Name    string         `json:"name" yaml:"name"`
	Options map[string]any `json:"options,omitempty" yaml:"options,omitempty"`
}

type Service struct {
	Name      string            `json:"name" yaml:"name"`
	Endpoints []Endpoint        `json:"endpoints" yaml:"endpoints"`
	Tags      []string          `json:"tags,omitempty" yaml:"tags,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

type Endpoint struct {
	Name     string            `json:"name" yaml:"name"`
	Protocol string            `json:"protocol,omitempty" yaml:"protocol,omitempty"`
	Address  string            `json:"address" yaml:"address"`
	Port     int               `json:"port" yaml:"port"`
	Path     string            `json:"path,omitempty" yaml:"path,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	switch filepath.Ext(path) {
	case ".yaml", ".yml":
		err = yaml.Unmarshal(data, &cfg)
	case ".json":
		err = json.Unmarshal(data, &cfg)
	}
	if err != nil {
		return nil, err
	}

	if cfg.TTLSeconds == 0 {
		cfg.TTLSeconds = 30 // seconds
	}
	if cfg.HeartbeatIntervalSec == 0 {
		cfg.HeartbeatIntervalSec = 10 // seconds
	}
	if cfg.GRPCPort == 0 {
		cfg.GRPCPort = 9900
	}
	return &cfg, nil
}
