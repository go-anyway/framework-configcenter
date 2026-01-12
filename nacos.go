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
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-anyway/framework-log"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"go.uber.org/zap"
)

// nacosCenter Nacos 配置中心实现
type nacosCenter struct {
	opts     *Options
	client   config_client.IConfigClient
	dataIDs  []string
	group    string
	configs  map[string]map[string]string // dataID -> key -> value
	mu       sync.RWMutex
	ctx      context.Context
	cancel   context.CancelFunc
	callback func(key, oldValue, newValue string) // 配置变更回调
}

// newNacosCenter 创建 Nacos 配置中心
func newNacosCenter(opts *Options) (ConfigCenter, error) {
	if opts.Type != TypeNacos {
		return nil, fmt.Errorf("invalid config center type: expected nacos, got %s", opts.Type)
	}

	// 验证配置
	if opts.NacosServerAddr == "" {
		return nil, fmt.Errorf("nacos server_addr is required")
	}
	if len(opts.NacosDataIDs) == 0 {
		return nil, fmt.Errorf("at least one nacos data_id is required")
	}

	// 设置默认值
	if opts.NacosGroup == "" {
		opts.NacosGroup = "DEFAULT_GROUP"
	}
	if opts.NacosTimeout == 0 {
		opts.NacosTimeout = 10 * time.Second
	}
	if opts.NacosRetryInterval == 0 {
		opts.NacosRetryInterval = 1 * time.Second
	}
	if opts.NacosMaxRetries == 0 {
		opts.NacosMaxRetries = 3
	}

	// 处理 namespace：当为 "public" 或空字符串时，转换为空字符串
	// Nacos SDK 中，public 命名空间对应空字符串
	namespaceOriginal := opts.NacosNamespace
	namespaceID := namespaceOriginal
	if namespaceID == "public" || namespaceID == "" {
		namespaceID = ""
	}

	// 解析服务器地址（支持多种格式：localhost:8848, http://localhost:8848, http://localhost:8848/nacos）
	ipAddr, port, scheme, err := parseNacosServerAddr(opts.NacosServerAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid nacos server address: %w", err)
	}

	// 创建服务器配置
	sc := []constant.ServerConfig{
		{
			IpAddr:      ipAddr,
			Port:        port,
			Scheme:      scheme,
			ContextPath: "/nacos",
		},
	}

	// 确定缓存目录和日志目录
	// 注意：使用 /tmp 目录在容器重启时会被清空，这是正常的
	// 如果需要持久化缓存，可以通过环境变量或配置项指定其他目录
	// 但首次启动时缓存不存在是正常情况，不应该被当作错误
	cacheDir := "/tmp/nacos/cache"
	logDir := "/tmp/nacos/log"

	// 如果设置了环境变量，使用环境变量指定的目录（可选功能）
	if customCacheDir := os.Getenv("NACOS_CACHE_DIR"); customCacheDir != "" {
		cacheDir = customCacheDir
	}
	if customLogDir := os.Getenv("NACOS_LOG_DIR"); customLogDir != "" {
		logDir = customLogDir
	}

	// 创建缓存目录（包括 config 子目录）
	configCacheDir := filepath.Join(cacheDir, "config")
	if err := os.MkdirAll(configCacheDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create nacos cache directory: %w", err)
	}
	// 创建日志目录
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create nacos log directory: %w", err)
	}

	// 创建客户端配置
	cc := constant.ClientConfig{
		NamespaceId:         namespaceID,
		TimeoutMs:           uint64(opts.NacosTimeout.Milliseconds()),
		NotLoadCacheAtStart: true, // 启动时不加载缓存，避免缓存文件问题
		LogDir:              logDir,
		CacheDir:            cacheDir,
		LogLevel:            "info",
	}

	// 如果有用户名和密码，设置认证
	if opts.NacosUsername != "" {
		cc.Username = opts.NacosUsername
		cc.Password = opts.NacosPassword
	}

	// 创建配置客户端
	configClient, err := clients.CreateConfigClient(map[string]interface{}{
		"serverConfigs": sc,
		"clientConfig":  cc,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create nacos config client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// 更新 opts 中的 namespace 为处理后的值（用于后续使用）
	opts.NacosNamespace = namespaceID

	center := &nacosCenter{
		opts:    opts,
		client:  configClient,
		dataIDs: opts.NacosDataIDs,
		group:   opts.NacosGroup,
		configs: make(map[string]map[string]string),
		ctx:     ctx,
		cancel:  cancel,
	}

	// 初始化配置缓存
	for _, dataID := range opts.NacosDataIDs {
		center.configs[dataID] = make(map[string]string)
	}

	log.Info("Nacos config center created",
		zap.String("server_addr", opts.NacosServerAddr),
		zap.String("ip_addr", ipAddr),
		zap.Uint64("port", port),
		zap.String("scheme", scheme),
		zap.String("namespace", namespaceID),
		zap.String("namespace_original", namespaceOriginal),
		zap.String("group", opts.NacosGroup),
		zap.Strings("data_ids", opts.NacosDataIDs),
		zap.String("cache_dir", cacheDir),
		zap.String("log_dir", logDir),
	)

	// 先尝试同步执行一次 refreshConfigs，确保初始配置已加载
	// 这样可以避免启动时配置变更发生在 refreshConfigs 完成之前
	// 使用 goroutine + channel 实现超时控制，避免阻塞启动
	done := make(chan bool, 1)
	go func() {
		center.refreshConfigs()
		done <- true
	}()

	// 等待最多 5 秒，如果超时则继续启动（配置会在后台异步加载）
	select {
	case <-done:
		log.Info("Initial config loaded successfully")
	case <-time.After(5 * time.Second):
		log.Warn("Initial config loading timeout, continuing startup (config will be loaded asynchronously)")
	}

	// 后续的配置变更会通过 WatchChanges 监听

	return center, nil
}

// Name 返回配置中心名称
func (c *nacosCenter) Name() string {
	return "nacos"
}

// GetConfig 获取配置值
func (c *nacosCenter) GetConfig(key string) (string, error) {
	if c == nil || c.client == nil {
		return "", fmt.Errorf("nacos center is not initialized")
	}

	// 从所有 DataID 查找
	allConfigs, err := c.GetAllConfigs()
	if err == nil {
		if value, ok := allConfigs[key]; ok && value != "" {
			return value, nil
		}
	}

	return "", fmt.Errorf("config key %s not found", key)
}

// GetAllConfigs 获取所有配置
func (c *nacosCenter) GetAllConfigs() (map[string]string, error) {
	if c == nil || c.client == nil {
		return nil, fmt.Errorf("nacos center is not initialized")
	}

	// 先检查缓存（参考 Consul 实现）
	c.mu.RLock()
	hasCache := len(c.configs) > 0
	if hasCache {
		// 检查是否有非空配置
		allConfigs := make(map[string]string)
		for _, configMap := range c.configs {
			if len(configMap) > 0 {
				for k, v := range configMap {
					allConfigs[k] = v
				}
			}
		}
		if len(allConfigs) > 0 {
			result := make(map[string]string, len(allConfigs))
			for k, v := range allConfigs {
				result[k] = v
			}
			c.mu.RUnlock()
			return result, nil
		}
	}
	c.mu.RUnlock()

	// 如果缓存为空，刷新配置
	c.refreshConfigs()

	// 再次从缓存获取
	c.mu.RLock()
	allConfigs := make(map[string]string)
	for _, configMap := range c.configs {
		for k, v := range configMap {
			allConfigs[k] = v
		}
	}
	c.mu.RUnlock()

	return allConfigs, nil
}

// refreshConfigs 刷新配置缓存（参考 Consul 实现）
func (c *nacosCenter) refreshConfigs() {
	allConfigs := make(map[string]string)

	// 从所有 DataID 获取配置
	for _, dataID := range c.dataIDs {
		log.Debug("Fetching config from Nacos",
			zap.String("data_id", dataID),
			zap.String("group", c.group),
			zap.String("namespace", c.opts.NacosNamespace),
		)

		// 构建配置参数
		// 注意：命名空间在客户端配置中已设置（ClientConfig.NamespaceId）
		// public 命名空间对应空字符串，SDK 会自动处理
		content, err := c.client.GetConfig(vo.ConfigParam{
			DataId: dataID,
			Group:  c.group,
		})
		if err != nil {
			// 区分"配置不存在/缓存未生成"和"真实异常"
			if c.isConfigNotFoundError(err) {
				// 配置不存在或缓存未生成（首次启动、缓存被清理等）
				// 这是正常情况，降级为 WARN 级别
				log.Warn("Nacos config not found or cache not ready (this is normal on first startup)",
					zap.String("data_id", dataID),
					zap.String("group", c.group),
					zap.String("namespace", c.opts.NacosNamespace),
					zap.String("reason", "config may not exist in Nacos or cache not generated yet"),
				)
				// 确保即使配置不存在，也初始化一个空 map，用于后续变更检测
				c.mu.Lock()
				if _, exists := c.configs[dataID]; !exists {
					c.configs[dataID] = make(map[string]string)
				}
				c.mu.Unlock()
				// 继续处理其他 dataID，不阻塞流程
				continue
			}

			// 真实异常：网络错误、认证失败、服务器不可用等
			log.Error("Failed to get config from Nacos (real error)",
				zap.String("data_id", dataID),
				zap.String("group", c.group),
				zap.String("namespace", c.opts.NacosNamespace),
				zap.Error(err),
			)
			// 确保即使获取失败，也初始化一个空 map，用于后续变更检测
			c.mu.Lock()
			if _, exists := c.configs[dataID]; !exists {
				c.configs[dataID] = make(map[string]string)
			}
			c.mu.Unlock()
			// 继续处理其他 dataID，不阻塞流程
			continue
		}

		if content == "" {
			log.Warn("Empty config content from Nacos",
				zap.String("data_id", dataID),
				zap.String("group", c.group),
				zap.String("namespace", c.opts.NacosNamespace),
			)
			// 确保即使内容为空，也初始化一个空 map
			c.mu.Lock()
			if _, exists := c.configs[dataID]; !exists {
				c.configs[dataID] = make(map[string]string)
			}
			c.mu.Unlock()
			continue
		}

		// 解析配置内容（优先使用 properties 格式：key=value，每行一个配置项）
		// 支持格式示例：
		//   gateway.features.rate_limit.rate=1.0
		//   gateway.features.rate_limit.burst=5
		parsed := parseProperties(content)
		log.Info("Nacos config loaded",
			zap.String("data_id", dataID),
			zap.String("group", c.group),
			zap.String("namespace", c.opts.NacosNamespace),
			zap.Int("config_count", len(parsed)),
		)

		// 合并配置（后面的 dataID 会覆盖前面的同名键）
		for k, v := range parsed {
			if _, exists := allConfigs[k]; !exists {
				allConfigs[k] = v
			} else {
				log.Debug("Config key already exists, skipping",
					zap.String("key", k),
					zap.String("data_id", dataID),
				)
			}
		}

		// 更新缓存
		c.mu.Lock()
		c.configs[dataID] = parsed
		c.mu.Unlock()
	}

	if len(allConfigs) > 0 {
		log.Info("Nacos config refresh completed",
			zap.Int("total_count", len(allConfigs)),
		)
	}
}

// WatchChanges 开始监听配置变更
func (c *nacosCenter) WatchChanges(callback func(key, oldValue, newValue string)) error {
	if c == nil || c.client == nil {
		return fmt.Errorf("nacos center is not initialized")
	}

	if callback == nil {
		return fmt.Errorf("callback cannot be nil")
	}

	c.mu.Lock()
	c.callback = callback
	c.mu.Unlock()

	log.Info("Nacos watch starting", zap.Strings("data_ids", c.dataIDs))

	// 确保初始配置已加载（如果 refreshConfigs 还没完成，等待一下）
	// 这样可以避免启动时配置变更发生在 refreshConfigs 完成之前
	c.mu.RLock()
	hasInitialConfig := false
	for _, dataID := range c.dataIDs {
		if configs, ok := c.configs[dataID]; ok && len(configs) > 0 {
			hasInitialConfig = true
			break
		}
	}
	c.mu.RUnlock()

	if !hasInitialConfig {
		log.Debug("Initial config not loaded yet, refreshing before starting watch")
		c.refreshConfigs()
	}

	// 为每个 DataID 启动监听
	for _, dataID := range c.dataIDs {
		did := dataID // 避免闭包问题
		go func() {
			log.Debug("Starting watch for dataID", zap.String("data_id", did))
			// 构建监听配置参数
			// 注意：命名空间在客户端配置中已设置（ClientConfig.NamespaceId）
			// public 命名空间对应空字符串，SDK 会自动处理
			err := c.client.ListenConfig(vo.ConfigParam{
				DataId: did,
				Group:  c.group,
				OnChange: func(namespace, group, dataId, data string) {
					// 解析配置内容
					// 注意：如果 data 为空，parseProperties 会返回空 map
					// 这表示配置被清空，应该触发所有旧配置的删除回调
					parsed := parseProperties(data)
					log.Info("Nacos config changed",
						zap.String("data_id", dataId),
						zap.String("group", group),
						zap.String("namespace", namespace),
						zap.Int("config_count", len(parsed)),
					)

					// 获取旧配置和回调函数（必须在更新缓存之前获取）
					c.mu.RLock()
					oldConfigs := make(map[string]string)
					if old, ok := c.configs[dataId]; ok {
						for k, v := range old {
							oldConfigs[k] = v
						}
					}
					callback := c.callback
					c.mu.RUnlock()

					// 更新缓存（即使配置为空也要更新）
					c.mu.Lock()
					c.configs[dataId] = parsed
					c.mu.Unlock()

					// 调用回调
					if callback == nil {
						log.Warn("Nacos config change callback is nil",
							zap.String("data_id", dataId),
						)
						return
					}

					changeCount := 0

					// 检查新增和变更的配置
					for key, newValue := range parsed {
						oldValue, exists := oldConfigs[key]
						if !exists {
							// 新增的配置
							changeCount++
							log.Info("Nacos config key added",
								zap.String("data_id", dataId),
								zap.String("key", key),
								zap.String("new_value", newValue),
							)
							callback(key, "", newValue)
						} else if oldValue != newValue {
							// 变更的配置
							changeCount++
							log.Info("Nacos config value changed",
								zap.String("data_id", dataId),
								zap.String("key", key),
								zap.String("old_value", oldValue),
								zap.String("new_value", newValue),
							)
							callback(key, oldValue, newValue)
						} else {
							// 配置值相同，跳过（不触发回调）
							log.Debug("Nacos config key unchanged",
								zap.String("data_id", dataId),
								zap.String("key", key),
							)
						}
					}

					// 检查删除的配置（存在于旧配置但不在新配置中）
					for key, oldValue := range oldConfigs {
						if _, exists := parsed[key]; !exists {
							changeCount++
							log.Info("Nacos config key deleted",
								zap.String("data_id", dataId),
								zap.String("key", key),
								zap.String("old_value", oldValue),
							)
							callback(key, oldValue, "")
						}
					}

					if changeCount > 0 {
						log.Info("Nacos config changes processed",
							zap.String("data_id", dataId),
							zap.Int("change_count", changeCount),
						)
					}
				},
			})
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Error("Nacos watch error",
					zap.String("data_id", did),
					zap.Error(err),
				)
			} else {
				log.Debug("Nacos watch started successfully", zap.String("data_id", did))
			}
		}()
	}

	return nil
}

// isConfigNotFoundError 判断是否是"配置不存在/缓存未生成"的错误
// 这是 nacos-sdk-go 的设计问题：首次启动时 cache miss 会被记录为 ERROR
// 但实际上这是正常情况，不应该被当作真实异常处理
func (c *nacosCenter) isConfigNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	// 检查是否是缓存文件不存在的错误（首次启动、缓存被清理等正常情况）
	// 这些错误表明：配置可能不存在，或者缓存尚未生成
	cacheMissIndicators := []string{
		"file doesn't exist",
		"file not exist",
		"cache file",
		"Config Encrypted Data Key",
		"read config from both server and cache fail",
	}

	for _, indicator := range cacheMissIndicators {
		if strings.Contains(errStr, indicator) {
			return true
		}
	}

	// 其他错误（网络错误、认证失败、服务器不可用等）视为真实异常
	return false
}

// Close 关闭配置中心连接
func (c *nacosCenter) Close() error {
	if c == nil {
		return nil
	}

	if c.cancel != nil {
		c.cancel()
	}

	// Nacos SDK 没有显式的 Close 方法
	return nil
}

// parseProperties 解析 properties 格式的配置
func parseProperties(content string) map[string]string {
	result := make(map[string]string)
	lines := strings.Split(content, "\n")
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			if key != "" {
				result[key] = value
			} else {
				log.Debug("Skipping empty key in properties",
					zap.Int("line_number", i+1),
					zap.String("line", line),
				)
			}
		} else {
			log.Debug("Skipping invalid line in properties",
				zap.Int("line_number", i+1),
				zap.String("line", line),
			)
		}
	}
	return result
}

// getContentPreview 获取内容预览（前200个字符）
func getContentPreview(content string) string {
	if len(content) <= 200 {
		return content
	}
	return content[:200] + "..."
}

// getKeys 获取 map 的所有键（用于日志）
func getKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// parseNacosServerAddr 解析 Nacos 服务器地址
// 支持格式：
//   - localhost:8848
//   - http://localhost:8848
//   - https://localhost:8848
//   - http://localhost:8848/nacos
//   - 192.168.1.1:8848
//
// 返回：ipAddr, port, scheme, error
func parseNacosServerAddr(addr string) (string, uint64, string, error) {
	if addr == "" {
		return "", 0, "", fmt.Errorf("server address is empty")
	}

	// 默认值
	scheme := "http"
	port := uint64(8848) // Nacos 默认端口

	// 如果包含协议，解析 URL
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		u, err := url.Parse(addr)
		if err != nil {
			return "", 0, "", fmt.Errorf("invalid URL format: %w", err)
		}

		scheme = u.Scheme
		addr = u.Host // 提取 host:port

		// 如果 URL 包含端口，从 Host 中提取
		if u.Port() != "" {
			p, err := strconv.ParseUint(u.Port(), 10, 64)
			if err != nil {
				return "", 0, "", fmt.Errorf("invalid port: %w", err)
			}
			port = p
		}
	}

	// 解析 host:port
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		// 如果没有端口，使用默认端口
		host = addr
	} else {
		// 如果有端口，解析端口号
		p, err := strconv.ParseUint(portStr, 10, 64)
		if err != nil {
			return "", 0, "", fmt.Errorf("invalid port: %w", err)
		}
		port = p
	}

	// 验证 host
	if host == "" {
		return "", 0, "", fmt.Errorf("host is empty")
	}

	// 如果是 IP 地址，直接返回
	if net.ParseIP(host) != nil {
		return host, port, scheme, nil
	}

	// 如果是域名，解析为 IP（可选，也可以直接使用域名）
	// 这里直接返回域名，让 Nacos SDK 处理
	return host, port, scheme, nil
}
