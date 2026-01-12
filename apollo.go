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
	"sync"
	"time"

	"github.com/go-anyway/framework-log"

	"github.com/apolloconfig/agollo/v4"
	"github.com/apolloconfig/agollo/v4/env/config"
	"github.com/apolloconfig/agollo/v4/storage"
	"go.uber.org/zap"
)

// apolloCenter Apollo 配置中心实现
type apolloCenter struct {
	opts        *Options
	client      agollo.Client
	namespaces  []string
	watchers    map[string]context.CancelFunc
	configCache map[string]map[string]string // namespace -> key -> value
	mu          sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc
	callback    func(key, oldValue, newValue string) // 配置变更回调
}

// newApolloCenter 创建 Apollo 配置中心
func newApolloCenter(opts *Options) (ConfigCenter, error) {
	if opts.Type != TypeApollo {
		return nil, fmt.Errorf("invalid config center type: expected apollo, got %s", opts.Type)
	}

	// 验证配置
	if opts.ApolloAppID == "" {
		return nil, fmt.Errorf("apollo app_id is required")
	}
	if opts.ApolloIP == "" {
		return nil, fmt.Errorf("apollo ip is required")
	}
	if len(opts.ApolloNamespaces) == 0 {
		return nil, fmt.Errorf("at least one apollo namespace is required")
	}

	// 设置默认值
	if opts.ApolloCluster == "" {
		opts.ApolloCluster = "default"
	}
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
	if opts.ApolloMaxRetries == 0 {
		opts.ApolloMaxRetries = 3
	}

	// 创建 agollo 客户端配置
	appConfig := &config.AppConfig{
		AppID:          opts.ApolloAppID,
		Cluster:        opts.ApolloCluster,
		IP:             opts.ApolloIP,
		NamespaceName:  opts.ApolloNamespaces[0], // 使用第一个命名空间
		IsBackupConfig: true,                     // 启用本地备份
		Secret:         opts.ApolloSecretKey,
	}

	// 启动 agollo 客户端
	client, err := agollo.StartWithConfig(func() (*config.AppConfig, error) {
		return appConfig, nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start apollo client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	center := &apolloCenter{
		opts:        opts,
		client:      client,
		namespaces:  opts.ApolloNamespaces,
		watchers:    make(map[string]context.CancelFunc),
		configCache: make(map[string]map[string]string),
		ctx:         ctx,
		cancel:      cancel,
	}

	// 初始化配置缓存：为每个命名空间创建缓存映射
	for _, ns := range opts.ApolloNamespaces {
		center.configCache[ns] = make(map[string]string)
	}

	log.Info("Apollo config center created",
		zap.String("app_id", opts.ApolloAppID),
		zap.String("cluster", opts.ApolloCluster),
		zap.String("ip", opts.ApolloIP),
		zap.Strings("namespaces", opts.ApolloNamespaces),
	)

	// 先尝试同步加载初始配置，确保配置已加载（参考 Nacos 实现）
	// 使用 goroutine + channel 实现超时控制，避免阻塞启动
	done := make(chan bool, 1)
	go func() {
		// 通过 GetAllConfigs 触发配置加载
		_, _ = center.GetAllConfigs()
		done <- true
	}()

	// 等待最多 5 秒，如果超时则继续启动（配置会在后台异步加载）
	select {
	case <-done:
		log.Info("Apollo initial config loaded successfully")
	case <-time.After(5 * time.Second):
		log.Warn("Apollo initial config loading timeout, continuing startup (config will be loaded asynchronously)")
	}

	return center, nil
}

// Name 返回配置中心名称
func (c *apolloCenter) Name() string {
	return "apollo"
}

// GetConfig 获取配置值
func (c *apolloCenter) GetConfig(key string) (string, error) {
	if c == nil || c.client == nil {
		return "", fmt.Errorf("apollo center is not initialized")
	}

	// 从所有命名空间查找
	for _, ns := range c.namespaces {
		cache := c.client.GetConfigCache(ns)
		if cache == nil {
			continue
		}

		value, err := cache.Get(key)
		if err == nil {
			if strValue, ok := value.(string); ok {
				return strValue, nil
			}
			return fmt.Sprintf("%v", value), nil
		}
	}

	return "", fmt.Errorf("config key %s not found", key)
}

// GetAllConfigs 获取所有配置
func (c *apolloCenter) GetAllConfigs() (map[string]string, error) {
	if c == nil || c.client == nil {
		return nil, fmt.Errorf("apollo center is not initialized")
	}

	// 使用 context 和超时机制，避免阻塞
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan map[string]string, 1)
	errChan := make(chan error, 1)

	go func() {
		allConfigs := make(map[string]string)

		// 从所有命名空间获取配置
		for _, ns := range c.namespaces {
			cache := c.client.GetConfigCache(ns)
			if cache == nil {
				continue
			}

			// 从 agollo 客户端直接获取
			result := make(map[string]string)
			cache.Range(func(key, value interface{}) bool {
				if keyStr, ok := key.(string); ok {
					if valueStr, ok := value.(string); ok {
						result[keyStr] = valueStr
					} else if value != nil {
						result[keyStr] = fmt.Sprintf("%v", value)
					}
				}
				return true
			})

			// 合并配置（后面的命名空间会覆盖前面的）
			for k, v := range result {
				if _, exists := allConfigs[k]; !exists {
					allConfigs[k] = v
				}
			}

			// 更新缓存
			c.mu.Lock()
			c.configCache[ns] = result
			c.mu.Unlock()
		}

		done <- allConfigs
	}()

	select {
	case result := <-done:
		return result, nil
	case err := <-errChan:
		return nil, err
	case <-ctx.Done():
		// 超时返回空配置，不阻塞
		log.Debug("GetAllConfigs timeout, returning empty config",
			zap.String("config_center", "apollo"))
		return make(map[string]string), nil
	}
}

// WatchChanges 开始监听配置变更
func (c *apolloCenter) WatchChanges(callback func(key, oldValue, newValue string)) error {
	if c == nil || c.client == nil {
		return fmt.Errorf("apollo center is not initialized")
	}

	if callback == nil {
		return fmt.Errorf("callback cannot be nil")
	}

	c.mu.Lock()
	c.callback = callback
	c.mu.Unlock()

	log.Info("Apollo watch starting", zap.Strings("namespaces", c.namespaces))

	// 确保初始配置已加载（如果还没加载，等待一下）
	c.mu.RLock()
	hasInitialConfig := false
	for _, ns := range c.namespaces {
		if configs, ok := c.configCache[ns]; ok && len(configs) > 0 {
			hasInitialConfig = true
			break
		}
	}
	c.mu.RUnlock()

	if !hasInitialConfig {
		log.Debug("Apollo initial config not loaded yet, loading before starting watch")
		_, _ = c.GetAllConfigs()
	}

	// 为每个命名空间启动监听
	for _, namespace := range c.namespaces {
		ns := namespace // 避免闭包问题
		go func() {
			log.Debug("Starting watch for namespace", zap.String("namespace", ns))

			// 创建自定义监听器
			listener := &apolloConfigChangeListener{
				namespace: ns,
				center:    c,
			}

			// 注册监听器
			c.client.AddChangeListener(listener)

			// 启动后台轮询（用于非主命名空间）
			if ns != c.namespaces[0] {
				go c.pollNamespace(c.ctx, ns)
			}
		}()
	}

	return nil
}

// pollNamespace 轮询命名空间配置变更（用于非主命名空间）
func (c *apolloCenter) pollNamespace(ctx context.Context, namespace string) {
	ticker := time.NewTicker(c.opts.ApolloSyncInterval)
	defer ticker.Stop()

	// 记录上次的配置
	lastConfigs := make(map[string]string)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 获取当前配置
			currentConfigs, err := c.getAllConfigsForNamespace(namespace)
			if err != nil {
				log.Warn("Failed to poll namespace config",
					zap.String("namespace", namespace),
					zap.Error(err),
				)
				continue
			}

			// 检查是否有变更
			hasChange := false
			for key, newValue := range currentConfigs {
				oldValue, exists := lastConfigs[key]
				if !exists || oldValue != newValue {
					hasChange = true
					// 调用回调
					c.mu.RLock()
					callback := c.callback
					c.mu.RUnlock()
					if callback != nil {
						callback(key, oldValue, newValue)
					}
				}
			}

			// 检查是否有删除的配置
			for key := range lastConfigs {
				if _, exists := currentConfigs[key]; !exists {
					hasChange = true
					// 调用回调
					c.mu.RLock()
					callback := c.callback
					c.mu.RUnlock()
					if callback != nil {
						callback(key, lastConfigs[key], "")
					}
				}
			}

			if hasChange {
				lastConfigs = currentConfigs
			}
		}
	}
}

// getAllConfigsForNamespace 获取指定命名空间的所有配置
func (c *apolloCenter) getAllConfigsForNamespace(namespace string) (map[string]string, error) {
	cache := c.client.GetConfigCache(namespace)
	if cache == nil {
		return nil, fmt.Errorf("namespace %s not found", namespace)
	}

	result := make(map[string]string)
	cache.Range(func(key, value interface{}) bool {
		if keyStr, ok := key.(string); ok {
			if valueStr, ok := value.(string); ok {
				result[keyStr] = valueStr
			} else if value != nil {
				result[keyStr] = fmt.Sprintf("%v", value)
			}
		}
		return true
	})

	return result, nil
}

// Close 关闭配置中心连接
func (c *apolloCenter) Close() error {
	if c == nil {
		return nil
	}

	if c.cancel != nil {
		c.cancel()
	}

	// agollo SDK 没有显式的 Close 方法，客户端会在进程退出时自动清理
	return nil
}

// apolloConfigChangeListener 配置变更监听器
type apolloConfigChangeListener struct {
	namespace string
	center    *apolloCenter
}

// OnChange 配置变更回调
func (l *apolloConfigChangeListener) OnChange(event *storage.ChangeEvent) {
	select {
	case <-l.center.ctx.Done():
		return
	default:
	}

	// 检查是否是目标命名空间
	if event.Namespace != l.namespace {
		return
	}

	log.Info("Apollo config changed",
		zap.String("namespace", l.namespace),
		zap.Int("change_count", len(event.Changes)),
	)

	// 构建配置映射
	configs := make(map[string]string)
	for key, change := range event.Changes {
		if strValue, ok := change.NewValue.(string); ok {
			configs[key] = strValue
		} else if change.NewValue != nil {
			configs[key] = fmt.Sprintf("%v", change.NewValue)
		} else {
			// 如果 NewValue 为 nil，表示配置被删除
			configs[key] = ""
		}
	}

	// 调用回调
	l.center.mu.RLock()
	callback := l.center.callback
	l.center.mu.RUnlock()

	if callback != nil {
		changeCount := 0
		for key, newValue := range configs {
			// 获取旧值
			var oldValue string
			l.center.mu.RLock()
			if nsCache, ok := l.center.configCache[l.namespace]; ok {
				oldValue = nsCache[key]
			}
			l.center.mu.RUnlock()

			// 记录配置变更
			if oldValue == "" {
				changeCount++
				log.Info("Apollo config key added",
					zap.String("namespace", l.namespace),
					zap.String("key", key),
					zap.String("value", newValue),
				)
			} else if oldValue != newValue {
				changeCount++
				log.Info("Apollo config value changed",
					zap.String("namespace", l.namespace),
					zap.String("key", key),
					zap.String("old_value", oldValue),
					zap.String("new_value", newValue),
				)
			}

			// 调用回调
			callback(key, oldValue, newValue)

			// 更新缓存
			l.center.mu.Lock()
			if l.center.configCache[l.namespace] == nil {
				l.center.configCache[l.namespace] = make(map[string]string)
			}
			if newValue == "" {
				delete(l.center.configCache[l.namespace], key)
			} else {
				l.center.configCache[l.namespace][key] = newValue
			}
			l.center.mu.Unlock()
		}

		if changeCount > 0 {
			log.Info("Apollo config changes processed",
				zap.String("namespace", l.namespace),
				zap.Int("change_count", changeCount),
			)
		}
	}
}

// OnNewestChange 最新配置变更回调
func (l *apolloConfigChangeListener) OnNewestChange(event *storage.FullChangeEvent) {
	select {
	case <-l.center.ctx.Done():
		return
	default:
	}

	// 检查是否是目标命名空间
	if event.Namespace != l.namespace {
		return
	}

	// 构建配置映射（FullChangeEvent 包含所有配置）
	configs := make(map[string]string)
	for key, value := range event.Changes {
		if strValue, ok := value.(string); ok {
			configs[key] = strValue
		} else if value != nil {
			configs[key] = fmt.Sprintf("%v", value)
		}
	}

	// 更新客户端缓存（完整替换）
	l.center.mu.Lock()
	l.center.configCache[l.namespace] = configs
	l.center.mu.Unlock()

	// 调用回调（FullChangeEvent 包含完整的配置列表）
	l.center.mu.RLock()
	callback := l.center.callback
	l.center.mu.RUnlock()

	if callback != nil {
		// 对于 FullChangeEvent，我们只通知新增或变更的配置
		// 因为无法确定哪些配置被删除
		for key, newValue := range configs {
			callback(key, "", newValue)
		}
	}
}
