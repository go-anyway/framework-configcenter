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

	"github.com/go-anyway/framework-hotreload"
	"github.com/go-anyway/framework-log"

	"go.uber.org/zap"
)

// NewClient 根据配置创建统一的配置中心客户端
func NewClient(opts *Options) (*Client, error) {
	if opts == nil {
		return nil, fmt.Errorf("options cannot be nil")
	}

	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("invalid options: %w", err)
	}

	if opts.Type == TypeNone {
		return nil, fmt.Errorf("config center type is none")
	}

	var center ConfigCenter
	var err error

	switch opts.Type {
	case TypeApollo:
		center, err = newApolloCenter(opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create apollo center: %w", err)
		}

	case TypeNacos:
		center, err = newNacosCenter(opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create nacos center: %w", err)
		}

	case TypeConsul:
		center, err = newConsulCenter(opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create consul center: %w", err)
		}

	default:
		return nil, fmt.Errorf("unsupported config center type: %s", opts.Type)
	}

	ctx, cancel := context.WithCancel(context.Background())

	client := &Client{
		center:      center,
		Type:        opts.Type,
		enableTrace: opts.EnableTrace,
		watcher:     NewConfigWatcher(),
		configCache: make(map[string]string),
		ctx:         ctx,
		cancel:      cancel,
	}

	log.Info("Config center client created",
		zap.String("type", string(opts.Type)),
		zap.String("name", center.Name()),
		zap.Bool("enable_trace", opts.EnableTrace),
	)

	return client, nil
}

// NewFromConfig 从配置创建客户端
func NewFromConfig(cfg *Config) (*Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	opts, err := cfg.ToOptions()
	if err != nil {
		return nil, fmt.Errorf("failed to convert config to options: %w", err)
	}

	return NewClient(opts)
}

// GetConfig 获取配置值（带 trace）
func (c *Client) GetConfig(key string) (string, error) {
	if c == nil || c.center == nil {
		return "", fmt.Errorf("config center client is not initialized")
	}

	ctx := context.Background()
	return getConfigWithTrace(ctx, string(c.Type), key, func(ctx context.Context) (string, error) {
		return c.center.GetConfig(key)
	}, c.enableTrace)
}

// GetAllConfigs 获取所有配置（带 trace）
func (c *Client) GetAllConfigs() (map[string]string, error) {
	if c == nil || c.center == nil {
		return nil, fmt.Errorf("config center client is not initialized")
	}

	ctx := context.Background()
	return getAllConfigsWithTrace(ctx, string(c.Type), func(ctx context.Context) (map[string]string, error) {
		return c.center.GetAllConfigs()
	}, c.enableTrace)
}

// LoadToTarget 从配置中心加载配置并触发热重载
// 所有配置变更统一通过 watcher.Handle 处理，由 hotreload.Manager 统一管理
// allowedPrefixes: 允许的配置前缀列表（如 ["server.features.", "gateway.features."]）
func (c *Client) LoadToTarget(allowedPrefixes []string) error {
	if c == nil || c.center == nil {
		return fmt.Errorf("config center client is not initialized")
	}

	ctx := context.Background()
	return executeWithTrace(ctx, "load_to_target", string(c.Type), func(ctx context.Context) error {
		// 获取所有配置
		allConfigs, err := c.center.GetAllConfigs()
		if err != nil {
			return fmt.Errorf("failed to get configs from config center: %w", err)
		}

		log.Info("Configs retrieved from config center",
			zap.String("type", string(c.Type)),
			zap.String("name", c.center.Name()),
			zap.Int("total_count", len(allConfigs)),
			zap.Strings("allowed_prefixes", allowedPrefixes),
		)

		if len(allConfigs) == 0 {
			log.Info("No configs found in config center",
				zap.String("type", string(c.Type)),
				zap.String("name", c.center.Name()),
			)
			return nil
		}

		// 记录所有配置键（用于调试）
		allKeys := make([]string, 0, len(allConfigs))
		for key := range allConfigs {
			allKeys = append(allKeys, key)
		}
		log.Debug("All config keys from config center",
			zap.Strings("keys", allKeys),
		)

		// 处理配置：统一通过 watcher.Handle 处理，让 hotreload.Manager 统一处理所有配置变更
		loadedCount := 0
		for key, value := range allConfigs {
			// 检查是否允许的配置键
			if !isAllowedKey(key, allowedPrefixes) {
				log.Debug("Config key skipped (not in allowed prefixes)",
					zap.String("key", key),
					zap.Strings("allowed_prefixes", allowedPrefixes),
				)
				continue
			}

			// 获取旧值（从缓存中）
			c.mu.RLock()
			oldValue := c.configCache[key]
			c.mu.RUnlock()

			// 检查值是否真的变化了，如果没有变化则跳过处理
			if oldValue == value {
				continue
			}

			// 统一通过 watcher.Handle 处理配置变更
			// 这样可以让 hotreload.Manager 统一处理启动时加载和运行时变更
			// hotreload.Manager 会根据配置键匹配相应的处理器（字段设置器或重载器）
			c.watcher.Handle(key, oldValue, value)
			loadedCount++
			log.Debug("Config loaded from config center",
				zap.String("key", key),
				zap.String("value", value),
			)
		}

		if loadedCount > 0 {
			log.Info("Config center config loaded",
				zap.String("type", string(c.Type)),
				zap.String("name", c.center.Name()),
				zap.Int("total_count", len(allConfigs)),
				zap.Int("loaded_count", loadedCount),
			)
		}

		// 更新配置缓存
		c.mu.Lock()
		for k, v := range allConfigs {
			c.configCache[k] = v
		}
		c.mu.Unlock()

		return nil
	}, c.enableTrace)
}

// RegisterChangeHandler 注册配置变更处理器
func (c *Client) RegisterChangeHandler(pattern string, handler ConfigChangeHandler) error {
	if c == nil {
		return fmt.Errorf("config center client is not initialized")
	}
	if handler == nil {
		return fmt.Errorf("handler cannot be nil")
	}

	c.watcher.Register(pattern, handler)
	return nil
}

// ConnectHotReload 连接到热加载管理器
// 当配置中心发生变更时，会自动触发热加载管理器处理
func (c *Client) ConnectHotReload(manager *hotreload.Manager) error {
	if c == nil {
		return fmt.Errorf("config center client is not initialized")
	}
	if manager == nil {
		return fmt.Errorf("hot reload manager cannot be nil")
	}

	// 注册全局处理器，将所有配置变更转发到热加载管理器
	return c.RegisterChangeHandler("*", func(key, oldValue, newValue string) error {
		return manager.HandleChange(key, oldValue, newValue)
	})
}

// SetAllowedPrefixes 设置允许的配置前缀列表
// 用于过滤配置变更，只处理匹配前缀的配置
func (c *Client) SetAllowedPrefixes(prefixes []string) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.allowedPrefixes = make([]string, len(prefixes))
	copy(c.allowedPrefixes, prefixes)
}

// WatchChanges 开始监听配置变更
func (c *Client) WatchChanges() error {
	if c == nil || c.center == nil {
		return fmt.Errorf("config center client is not initialized")
	}

	ctx := context.Background()
	return executeWithTrace(ctx, "watch_changes", string(c.Type), func(ctx context.Context) error {
		// 注册配置变更回调
		return c.center.WatchChanges(func(key, oldValue, newValue string) {
			// 检查配置键是否在允许的前缀列表中
			c.mu.RLock()
			allowedPrefixes := c.allowedPrefixes
			c.mu.RUnlock()

			// 如果设置了允许的前缀列表，进行过滤
			if len(allowedPrefixes) > 0 {
				if !isAllowedKey(key, allowedPrefixes) {
					// 配置键不在允许列表中，跳过处理
					return
				}
			}

			// 从缓存获取旧值（如果新值还没有更新到缓存）
			c.mu.RLock()
			cachedOldValue, hasCache := c.configCache[key]
			c.mu.RUnlock()

			// 如果回调传入的 oldValue 为空，尝试从缓存获取
			if oldValue == "" && hasCache {
				oldValue = cachedOldValue
			}

			// 检查值是否真的变化了，如果没有变化则跳过处理
			if oldValue == newValue {
				return
			}

			log.Debug("Config center change detected",
				zap.String("type", string(c.Type)),
				zap.String("key", key),
				zap.String("old_value", oldValue),
				zap.String("new_value", newValue),
			)

			// 处理配置变更
			c.watcher.Handle(key, oldValue, newValue)

			// 更新配置缓存
			c.mu.Lock()
			if newValue == "" {
				// 配置被删除
				delete(c.configCache, key)
			} else {
				c.configCache[key] = newValue
			}
			c.mu.Unlock()
		})
	}, c.enableTrace)
}

// Name 返回配置中心名称
func (c *Client) Name() string {
	if c == nil || c.center == nil {
		return ""
	}
	return c.center.Name()
}

// Close 关闭客户端
func (c *Client) Close() error {
	if c == nil {
		return nil
	}

	if c.cancel != nil {
		c.cancel()
	}

	if c.center != nil {
		return c.center.Close()
	}

	return nil
}

// isAllowedKey 检查配置键是否在允许的前缀列表中
func isAllowedKey(key string, allowedPrefixes []string) bool {
	if len(allowedPrefixes) == 0 {
		// 如果没有指定允许的前缀，默认允许所有
		return true
	}

	for _, prefix := range allowedPrefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}

	return false
}
