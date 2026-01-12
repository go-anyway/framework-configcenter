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
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-anyway/framework-log"

	"github.com/hashicorp/consul/api"
	"go.uber.org/zap"
)

// consulCenter Consul 配置中心实现
type consulCenter struct {
	opts     *Options
	client   *api.Client
	kv       *api.KV
	prefix   string
	configs  map[string]string
	mu       sync.RWMutex
	ctx      context.Context
	cancel   context.CancelFunc
	callback func(key, oldValue, newValue string) // 配置变更回调
}

// newConsulCenter 创建 Consul 配置中心
func newConsulCenter(opts *Options) (ConfigCenter, error) {
	if opts.Type != TypeConsul {
		return nil, fmt.Errorf("invalid config center type: expected consul, got %s", opts.Type)
	}

	// 验证配置
	if opts.ConsulAddress == "" {
		return nil, fmt.Errorf("consul address is required")
	}

	// 设置默认值
	if opts.ConsulPrefix == "" {
		opts.ConsulPrefix = "config/"
	}
	// 确保 prefix 以 / 结尾（符合 Consul KV 路径规范）
	if !strings.HasSuffix(opts.ConsulPrefix, "/") {
		opts.ConsulPrefix = opts.ConsulPrefix + "/"
	}
	if opts.ConsulScheme == "" {
		opts.ConsulScheme = "http"
	}
	if opts.ConsulTimeout == 0 {
		opts.ConsulTimeout = 10 * time.Second
	}
	if opts.ConsulRetryInterval == 0 {
		opts.ConsulRetryInterval = 1 * time.Second
	}
	if opts.ConsulMaxRetries == 0 {
		opts.ConsulMaxRetries = 3
	}

	// 创建 Consul 客户端配置
	config := api.DefaultConfig()
	config.Address = opts.ConsulAddress
	if opts.ConsulDatacenter != "" {
		config.Datacenter = opts.ConsulDatacenter
	}
	if opts.ConsulToken != "" {
		config.Token = opts.ConsulToken
	}
	if opts.ConsulScheme != "" {
		config.Scheme = opts.ConsulScheme
	}

	// 创建客户端
	client, err := api.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create consul client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	center := &consulCenter{
		opts:    opts,
		client:  client,
		kv:      client.KV(),
		prefix:  opts.ConsulPrefix,
		configs: make(map[string]string),
		ctx:     ctx,
		cancel:  cancel,
	}

	log.Info("Consul config center created",
		zap.String("address", opts.ConsulAddress),
		zap.String("datacenter", opts.ConsulDatacenter),
		zap.String("prefix", opts.ConsulPrefix),
	)

	// 先尝试同步加载初始配置，确保配置已加载（参考 Nacos 实现）
	// 使用 goroutine + channel 实现超时控制，避免阻塞启动
	done := make(chan bool, 1)
	go func() {
		center.refreshConfigs()
		done <- true
	}()

	// 等待最多 5 秒，如果超时则继续启动（配置会在后台异步加载）
	select {
	case <-done:
		log.Info("Consul initial config loaded successfully")
	case <-time.After(5 * time.Second):
		log.Warn("Consul initial config loading timeout, continuing startup (config will be loaded asynchronously)")
	}

	return center, nil
}

// Name 返回配置中心名称
func (c *consulCenter) Name() string {
	return "consul"
}

// GetConfig 获取配置值
// key: 配置键，使用 . 分隔符格式（如 gateway.features.rate_limit.enabled）
func (c *consulCenter) GetConfig(key string) (string, error) {
	if c == nil || c.kv == nil {
		return "", fmt.Errorf("consul center is not initialized")
	}

	// 将配置键的 . 分隔符转换为 Consul KV 的 / 路径分隔符
	// 例如：gateway.features.rate_limit.enabled -> gateway/features/rate_limit/enabled
	consulKey := strings.ReplaceAll(key, ".", "/")
	fullKey := c.prefix + consulKey
	pair, _, err := c.kv.Get(fullKey, nil)
	if err != nil {
		return "", fmt.Errorf("failed to get config from consul: %w", err)
	}
	if pair == nil {
		return "", fmt.Errorf("config key %s not found", key)
	}
	return string(pair.Value), nil
}

// GetAllConfigs 获取所有配置
func (c *consulCenter) GetAllConfigs() (map[string]string, error) {
	if c == nil || c.kv == nil {
		return nil, fmt.Errorf("consul center is not initialized")
	}

	// 使用 context 和超时机制，避免阻塞
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan map[string]string, 1)
	errChan := make(chan error, 1)

	go func() {
		c.mu.RLock()
		if len(c.configs) > 0 {
			result := make(map[string]string, len(c.configs))
			for k, v := range c.configs {
				result[k] = v
			}
			c.mu.RUnlock()
			done <- result
			return
		}
		c.mu.RUnlock()

		// 如果缓存为空，刷新配置
		c.refreshConfigs()

		c.mu.RLock()
		result := make(map[string]string, len(c.configs))
		for k, v := range c.configs {
			result[k] = v
		}
		c.mu.RUnlock()
		done <- result
	}()

	select {
	case result := <-done:
		return result, nil
	case err := <-errChan:
		return nil, err
	case <-ctx.Done():
		// 超时返回空配置，不阻塞
		log.Debug("GetAllConfigs timeout, returning empty config",
			zap.String("config_center", "consul"))
		return make(map[string]string), nil
	}
}

// WatchChanges 开始监听配置变更
func (c *consulCenter) WatchChanges(callback func(key, oldValue, newValue string)) error {
	if c == nil || c.kv == nil {
		return fmt.Errorf("consul center is not initialized")
	}

	if callback == nil {
		return fmt.Errorf("callback cannot be nil")
	}

	c.mu.Lock()
	c.callback = callback
	c.mu.Unlock()

	log.Info("Consul watch starting", zap.String("prefix", c.prefix))

	// 确保初始配置已加载（如果还没加载，等待一下）
	c.mu.RLock()
	hasInitialConfig := len(c.configs) > 0
	c.mu.RUnlock()

	if !hasInitialConfig {
		log.Debug("Consul initial config not loaded yet, refreshing before starting watch")
		c.refreshConfigs()
	}

	// 启动监听
	go func() {
		ticker := time.NewTicker(5 * time.Second) // 每 5 秒轮询一次
		defer ticker.Stop()

		lastModifyIndex := uint64(0)

		for {
			select {
			case <-c.ctx.Done():
				return
			case <-ticker.C:
				// 使用 blocking query 获取配置变更
				opts := &api.QueryOptions{
					WaitIndex: lastModifyIndex,
					WaitTime:  10 * time.Second,
				}
				if c.opts.ConsulDatacenter != "" {
					opts.Datacenter = c.opts.ConsulDatacenter
				}

				pairs, meta, err := c.kv.List(c.prefix, opts)
				if err != nil {
					log.Warn("Consul watch error",
						zap.String("prefix", c.prefix),
						zap.Error(err),
					)
					continue
				}

				// 检查是否有变更
				if meta.LastIndex > lastModifyIndex {
					lastModifyIndex = meta.LastIndex
					log.Info("Consul config changed",
						zap.String("prefix", c.prefix),
						zap.Uint64("last_index", meta.LastIndex),
					)

					// 构建新配置映射
					newConfigs := make(map[string]string)
					prefixLen := len(c.prefix)
					for _, pair := range pairs {
						if len(pair.Key) > prefixLen {
							key := pair.Key[prefixLen:]
							// 去掉开头的 /（如果 prefix 以 / 结尾，去掉前缀后可能以 / 开头）
							key = strings.TrimPrefix(key, "/")
							// Consul KV 使用 / 作为路径分隔符，转换为配置键的 . 分隔符格式
							// 例如：config/gateway/features/rate_limit/enabled -> gateway.features.rate_limit.enabled
							key = strings.ReplaceAll(key, "/", ".")
							newConfigs[key] = string(pair.Value)
						}
					}

					// 获取旧配置
					c.mu.RLock()
					oldConfigs := make(map[string]string)
					for k, v := range c.configs {
						oldConfigs[k] = v
					}
					callback := c.callback
					c.mu.RUnlock()

					// 更新缓存
					c.mu.Lock()
					c.configs = newConfigs
					c.mu.Unlock()

					// 调用回调
					if callback != nil {
						changeCount := 0
						// 检查变更和新增
						for key, newValue := range newConfigs {
							oldValue := oldConfigs[key]
							if oldValue != newValue {
								changeCount++
								if oldValue == "" {
									log.Info("Consul config key added",
										zap.String("key", key),
										zap.String("value", newValue),
									)
								} else {
									log.Info("Consul config value changed",
										zap.String("key", key),
										zap.String("old_value", oldValue),
										zap.String("new_value", newValue),
									)
								}
								callback(key, oldValue, newValue)
							}
						}
						// 检查删除的配置
						for key, oldValue := range oldConfigs {
							if _, exists := newConfigs[key]; !exists {
								changeCount++
								log.Info("Consul config key deleted",
									zap.String("key", key),
									zap.String("old_value", oldValue),
								)
								callback(key, oldValue, "")
							}
						}
						if changeCount > 0 {
							log.Info("Consul config changes processed",
								zap.Int("change_count", changeCount),
							)
						}
					}
				}
			}
		}
	}()

	return nil
}

// refreshConfigs 刷新配置缓存
func (c *consulCenter) refreshConfigs() {
	pairs, _, err := c.kv.List(c.prefix, nil)
	if err != nil {
		log.Warn("Consul config refresh error",
			zap.String("prefix", c.prefix),
			zap.Error(err),
		)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.configs = make(map[string]string)
	prefixLen := len(c.prefix)
	for _, pair := range pairs {
		if len(pair.Key) > prefixLen {
			key := pair.Key[prefixLen:]
			// Consul KV 使用 / 作为路径分隔符，转换为配置键的 . 分隔符格式
			// 例如：config/gateway/features/rate_limit/enabled -> gateway.features.rate_limit.enabled
			key = strings.ReplaceAll(key, "/", ".")
			c.configs[key] = string(pair.Value)
		}
	}

	if len(c.configs) > 0 {
		log.Info("Consul config refresh completed",
			zap.String("prefix", c.prefix),
			zap.Int("config_count", len(c.configs)),
		)
	}
}

// Close 关闭配置中心连接
func (c *consulCenter) Close() error {
	if c == nil {
		return nil
	}

	if c.cancel != nil {
		c.cancel()
	}

	// Consul API 客户端没有显式的 Close 方法
	return nil
}
