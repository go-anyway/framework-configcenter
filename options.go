// Copyright 2025 zampo.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// @contact  zampo3380@gmail.com

package configcenter

import (
	"fmt"
	"time"

	pkgConfig "github.com/go-anyway/framework-config"
)

// Options 统一的配置中心配置选项
type Options struct {
	// 通用选项
	Type        Type
	EnableTrace bool

	// Apollo 选项
	ApolloAppID               string
	ApolloCluster             string
	ApolloNamespaces          []string
	ApolloIP                  string
	ApolloSecretKey           string
	ApolloSyncInterval        time.Duration
	ApolloLongPollTimeout     time.Duration
	ApolloNotificationTimeout time.Duration
	ApolloMaxRetries          int
	ApolloRetryInterval       time.Duration

	// Nacos 选项
	NacosServerAddr    string
	NacosNamespace     string
	NacosGroup         string
	NacosDataIDs       []string
	NacosUsername      string
	NacosPassword      string
	NacosTimeout       time.Duration
	NacosRetryInterval time.Duration
	NacosMaxRetries    int

	// Consul 选项
	ConsulAddress       string
	ConsulDatacenter    string
	ConsulToken         string
	ConsulPrefix        string
	ConsulScheme        string
	ConsulTimeout       time.Duration
	ConsulRetryInterval time.Duration
	ConsulMaxRetries    int
}

// Validate 验证配置选项
func (o *Options) Validate() error {
	if o == nil {
		return fmt.Errorf("options cannot be nil")
	}

	switch o.Type {
	case TypeApollo:
		if o.ApolloAppID == "" {
			return fmt.Errorf("apollo app_id is required")
		}
		if o.ApolloIP == "" {
			return fmt.Errorf("apollo ip is required")
		}
		if len(o.ApolloNamespaces) == 0 {
			return fmt.Errorf("at least one apollo namespace is required")
		}
	case TypeNacos:
		if o.NacosServerAddr == "" {
			return fmt.Errorf("nacos server_addr is required")
		}
		if len(o.NacosDataIDs) == 0 {
			return fmt.Errorf("at least one nacos data_id is required")
		}
	case TypeConsul:
		if o.ConsulAddress == "" {
			return fmt.Errorf("consul address is required")
		}
		if o.ConsulScheme != "" && o.ConsulScheme != "http" && o.ConsulScheme != "https" {
			return fmt.Errorf("consul scheme must be 'http' or 'https'")
		}
	case TypeNone:
		// 不使用配置中心，无需验证
	default:
		return fmt.Errorf("unsupported config center type: %s", o.Type)
	}

	return nil
}

// Config 配置结构体（用于从配置文件创建）
// 对应 config_center.yaml 文件结构
type Config struct {
	Type   string        `yaml:"type" env:"CONFIG_CENTER_TYPE" default:"none"`
	Apollo *ApolloConfig `yaml:"apollo,omitempty"`
	Nacos  *NacosConfig  `yaml:"nacos,omitempty"`
	Consul *ConsulConfig `yaml:"consul,omitempty"`
}

// Validate 验证配置
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("config cannot be nil")
	}

	if Type(c.Type) == TypeNone {
		return nil // 如果类型为 none，不需要验证
	}

	switch Type(c.Type) {
	case TypeApollo:
		if c.Apollo == nil {
			return fmt.Errorf("apollo config is required when type is apollo")
		}
		if !c.Apollo.Enabled {
			return fmt.Errorf("apollo config must be enabled when type is apollo")
		}
		if c.Apollo.AppID == "" {
			return fmt.Errorf("apollo app_id is required")
		}
		if c.Apollo.IP == "" {
			return fmt.Errorf("apollo ip is required")
		}
		if len(c.Apollo.Namespaces) == 0 {
			return fmt.Errorf("at least one apollo namespace is required")
		}
	case TypeNacos:
		if c.Nacos == nil {
			return fmt.Errorf("nacos config is required when type is nacos")
		}
		if !c.Nacos.Enabled {
			return fmt.Errorf("nacos config must be enabled when type is nacos")
		}
		if c.Nacos.ServerAddr == "" {
			return fmt.Errorf("nacos server_addr is required")
		}
		if len(c.Nacos.DataID) == 0 {
			return fmt.Errorf("at least one nacos data_id is required")
		}
	case TypeConsul:
		if c.Consul == nil {
			return fmt.Errorf("consul config is required when type is consul")
		}
		if !c.Consul.Enabled {
			return fmt.Errorf("consul config must be enabled when type is consul")
		}
		if c.Consul.Address == "" {
			return fmt.Errorf("consul address is required")
		}
		if c.Consul.Scheme != "" && c.Consul.Scheme != "http" && c.Consul.Scheme != "https" {
			return fmt.Errorf("consul scheme must be 'http' or 'https'")
		}
	default:
		return fmt.Errorf("unsupported config center type: %s", c.Type)
	}

	return nil
}

// ApolloConfig Apollo 配置
type ApolloConfig struct {
	Enabled             bool                  `yaml:"enabled" env:"APOLLO_ENABLED" default:"false"`
	AppID               string                `yaml:"app_id" env:"APOLLO_APP_ID" required:"true"`
	Cluster             string                `yaml:"cluster" env:"APOLLO_CLUSTER" default:"default"`
	Namespaces          pkgConfig.StringSlice `yaml:"namespaces" env:"APOLLO_NAMESPACE" default:"application"`
	IP                  string                `yaml:"ip" env:"APOLLO_IP" required:"true"`
	SecretKey           string                `yaml:"secret_key" env:"APOLLO_SECRET_KEY"`
	SyncInterval        string                `yaml:"sync_interval" env:"APOLLO_SYNC_INTERVAL" default:"5s"`
	LongPollTimeout     string                `yaml:"long_poll_timeout" env:"APOLLO_LONG_POLL_TIMEOUT" default:"90s"`
	NotificationTimeout string                `yaml:"notification_timeout" env:"APOLLO_NOTIFICATION_TIMEOUT" default:"1s"`
	MaxRetries          int                   `yaml:"max_retries" env:"APOLLO_MAX_RETRIES" default:"3"`
	RetryInterval       string                `yaml:"retry_interval" env:"APOLLO_RETRY_INTERVAL" default:"1s"`
	EnableTrace         bool                  `yaml:"enable_trace" env:"APOLLO_ENABLE_TRACE" default:"true"`
}

// NacosConfig Nacos 配置
type NacosConfig struct {
	Enabled       bool                  `yaml:"enabled" env:"NACOS_ENABLED" default:"false"`
	ServerAddr    string                `yaml:"server_addr" env:"NACOS_SERVER_ADDR" required:"true"`
	Namespace     string                `yaml:"namespace" env:"NACOS_NAMESPACE"`
	Group         string                `yaml:"group" env:"NACOS_GROUP" default:"DEFAULT_GROUP"`
	DataID        pkgConfig.StringSlice `yaml:"data_id" env:"NACOS_DATA_ID" default:"application"`
	Username      string                `yaml:"username" env:"NACOS_USERNAME"`
	Password      string                `yaml:"password" env:"NACOS_PASSWORD"`
	Timeout       string                `yaml:"timeout" env:"NACOS_TIMEOUT" default:"10s"`
	RetryInterval string                `yaml:"retry_interval" env:"NACOS_RETRY_INTERVAL" default:"1s"`
	MaxRetries    int                   `yaml:"max_retries" env:"NACOS_MAX_RETRIES" default:"3"`
	EnableTrace   bool                  `yaml:"enable_trace" env:"NACOS_ENABLE_TRACE" default:"true"`
}

// ConsulConfig Consul 配置
type ConsulConfig struct {
	Enabled       bool   `yaml:"enabled" env:"CONSUL_CONFIG_CENTER_ENABLED" default:"false"`
	Address       string `yaml:"address" env:"CONSUL_CONFIG_CENTER_ADDRESS" default:"localhost:8500"`
	Datacenter    string `yaml:"datacenter" env:"CONSUL_CONFIG_CENTER_DATACENTER"`
	Token         string `yaml:"token" env:"CONSUL_CONFIG_CENTER_TOKEN"`
	Prefix        string `yaml:"prefix" env:"CONSUL_CONFIG_CENTER_PREFIX" default:"config/"`
	Scheme        string `yaml:"scheme" env:"CONSUL_CONFIG_CENTER_SCHEME" default:"http"`
	Timeout       string `yaml:"timeout" env:"CONSUL_CONFIG_CENTER_TIMEOUT" default:"10s"`
	RetryInterval string `yaml:"retry_interval" env:"CONSUL_CONFIG_CENTER_RETRY_INTERVAL" default:"1s"`
	MaxRetries    int    `yaml:"max_retries" env:"CONSUL_CONFIG_CENTER_MAX_RETRIES" default:"3"`
	EnableTrace   bool   `yaml:"enable_trace" env:"CONSUL_CONFIG_CENTER_ENABLE_TRACE" default:"true"`
}

// ToOptions 将 Config 转换为 Options
func (c *Config) ToOptions() (*Options, error) {
	if c == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	opts := &Options{
		Type:        Type(c.Type),
		EnableTrace: true, // 默认启用
	}

	switch Type(c.Type) {
	case TypeApollo:
		if c.Apollo == nil || !c.Apollo.Enabled {
			return nil, fmt.Errorf("apollo config is not enabled")
		}
		opts.ApolloAppID = c.Apollo.AppID
		opts.ApolloCluster = c.Apollo.Cluster
		opts.ApolloNamespaces = c.Apollo.Namespaces.Strings()
		opts.ApolloIP = c.Apollo.IP
		opts.ApolloSecretKey = c.Apollo.SecretKey
		opts.EnableTrace = c.Apollo.EnableTrace

		// 解析时间字段
		if c.Apollo.SyncInterval != "" {
			if dur, err := time.ParseDuration(c.Apollo.SyncInterval); err == nil {
				opts.ApolloSyncInterval = dur
			}
		}
		if c.Apollo.LongPollTimeout != "" {
			if dur, err := time.ParseDuration(c.Apollo.LongPollTimeout); err == nil {
				opts.ApolloLongPollTimeout = dur
			}
		}
		if c.Apollo.NotificationTimeout != "" {
			if dur, err := time.ParseDuration(c.Apollo.NotificationTimeout); err == nil {
				opts.ApolloNotificationTimeout = dur
			}
		}
		if c.Apollo.RetryInterval != "" {
			if dur, err := time.ParseDuration(c.Apollo.RetryInterval); err == nil {
				opts.ApolloRetryInterval = dur
			}
		}
		opts.ApolloMaxRetries = c.Apollo.MaxRetries

	case TypeNacos:
		if c.Nacos == nil || !c.Nacos.Enabled {
			return nil, fmt.Errorf("nacos config is not enabled")
		}
		opts.NacosServerAddr = c.Nacos.ServerAddr
		opts.NacosNamespace = c.Nacos.Namespace
		opts.NacosGroup = c.Nacos.Group
		opts.NacosDataIDs = c.Nacos.DataID.Strings()
		opts.NacosUsername = c.Nacos.Username
		opts.NacosPassword = c.Nacos.Password
		opts.EnableTrace = c.Nacos.EnableTrace

		if c.Nacos.Timeout != "" {
			if dur, err := time.ParseDuration(c.Nacos.Timeout); err == nil {
				opts.NacosTimeout = dur
			}
		}
		if c.Nacos.RetryInterval != "" {
			if dur, err := time.ParseDuration(c.Nacos.RetryInterval); err == nil {
				opts.NacosRetryInterval = dur
			}
		}
		opts.NacosMaxRetries = c.Nacos.MaxRetries

	case TypeConsul:
		if c.Consul == nil || !c.Consul.Enabled {
			return nil, fmt.Errorf("consul config is not enabled")
		}
		opts.ConsulAddress = c.Consul.Address
		opts.ConsulDatacenter = c.Consul.Datacenter
		opts.ConsulToken = c.Consul.Token
		opts.ConsulPrefix = c.Consul.Prefix
		opts.ConsulScheme = c.Consul.Scheme
		opts.EnableTrace = c.Consul.EnableTrace

		if c.Consul.Timeout != "" {
			if dur, err := time.ParseDuration(c.Consul.Timeout); err == nil {
				opts.ConsulTimeout = dur
			}
		}
		if c.Consul.RetryInterval != "" {
			if dur, err := time.ParseDuration(c.Consul.RetryInterval); err == nil {
				opts.ConsulRetryInterval = dur
			}
		}
		opts.ConsulMaxRetries = c.Consul.MaxRetries

	case TypeNone:
		// 不使用配置中心
		return nil, fmt.Errorf("config center type is none")

	default:
		return nil, fmt.Errorf("unsupported config center type: %s", c.Type)
	}

	// 设置默认值
	if opts.ApolloSyncInterval == 0 {
		opts.ApolloSyncInterval = 5 * time.Second
	}
	if opts.ApolloLongPollTimeout == 0 {
		opts.ApolloLongPollTimeout = 90 * time.Second
	}
	if opts.ApolloNotificationTimeout == 0 {
		opts.ApolloNotificationTimeout = 1 * time.Second
	}
	if opts.ApolloRetryInterval == 0 {
		opts.ApolloRetryInterval = 1 * time.Second
	}
	if opts.NacosTimeout == 0 {
		opts.NacosTimeout = 10 * time.Second
	}
	if opts.NacosRetryInterval == 0 {
		opts.NacosRetryInterval = 1 * time.Second
	}
	if opts.ConsulTimeout == 0 {
		opts.ConsulTimeout = 10 * time.Second
	}
	if opts.ConsulRetryInterval == 0 {
		opts.ConsulRetryInterval = 1 * time.Second
	}

	return opts, nil
}
