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
	"sync"
)

// Type 配置中心类型
type Type string

const (
	// TypeNone 不使用配置中心
	TypeNone Type = "none"
	// TypeApollo Apollo 配置中心
	TypeApollo Type = "apollo"
	// TypeNacos Nacos 配置中心
	TypeNacos Type = "nacos"
	// TypeConsul Consul 配置中心
	TypeConsul Type = "consul"
)

// ConfigChangeHandler 配置变更处理器
// key: 配置键
// oldValue: 旧值
// newValue: 新值
// 返回错误时，配置变更不会生效，并记录错误日志
type ConfigChangeHandler func(key, oldValue, newValue string) error

// ConfigCenter 配置中心接口（底层实现）
// 抽象不同配置中心的通用操作
type ConfigCenter interface {
	// Name 返回配置中心名称（如 "apollo", "nacos", "consul"）
	Name() string

	// GetConfig 获取配置值
	GetConfig(key string) (string, error)

	// GetAllConfigs 获取所有配置
	GetAllConfigs() (map[string]string, error)

	// WatchChanges 开始监听配置变更
	// callback: 配置变更回调函数，参数为 (key, oldValue, newValue)
	WatchChanges(callback func(key, oldValue, newValue string)) error

	// Close 关闭配置中心连接
	Close() error
}

// ConfigWatcher 配置监听器（管理热更新）
type ConfigWatcher struct {
	handlers map[string][]ConfigChangeHandler // pattern -> handlers
	patterns []string                         // 所有注册的模式
	mu       sync.RWMutex
}

// NewConfigWatcher 创建新的配置监听器
func NewConfigWatcher() *ConfigWatcher {
	return &ConfigWatcher{
		handlers: make(map[string][]ConfigChangeHandler),
		patterns: make([]string, 0),
	}
}

// Register 注册配置变更处理器
func (w *ConfigWatcher) Register(pattern string, handler ConfigChangeHandler) {
	if handler == nil {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.handlers[pattern] == nil {
		w.handlers[pattern] = make([]ConfigChangeHandler, 0)
		// 如果是新模式，添加到模式列表
		exists := false
		for _, p := range w.patterns {
			if p == pattern {
				exists = true
				break
			}
		}
		if !exists {
			w.patterns = append(w.patterns, pattern)
		}
	}
	w.handlers[pattern] = append(w.handlers[pattern], handler)
}

// Handle 处理配置变更
func (w *ConfigWatcher) Handle(key, oldValue, newValue string) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	// 收集所有匹配的处理器
	matchedHandlers := make([]ConfigChangeHandler, 0)

	// 1. 精确匹配
	if handlers, ok := w.handlers[key]; ok {
		matchedHandlers = append(matchedHandlers, handlers...)
	}

	// 2. 全局处理器（空字符串表示匹配所有）
	if handlers, ok := w.handlers[""]; ok {
		matchedHandlers = append(matchedHandlers, handlers...)
	}

	// 3. 通配符匹配
	for pattern, handlers := range w.handlers {
		if pattern == "" || pattern == key {
			continue // 已处理
		}
		// 使用配置中心的模式匹配函数
		if MatchPattern(pattern, key) {
			matchedHandlers = append(matchedHandlers, handlers...)
		}
	}

	// 调用所有匹配的处理器
	for _, handler := range matchedHandlers {
		if err := handler(key, oldValue, newValue); err != nil {
			// 错误已在 handler 中记录，这里不重复记录
			_ = err
		}
	}
}

// Client 统一的配置中心客户端
type Client struct {
	center          ConfigCenter
	Type            Type
	enableTrace     bool
	watcher         *ConfigWatcher
	configCache     map[string]string // 配置缓存（用于获取旧值）
	allowedPrefixes []string          // 允许的配置前缀列表（用于过滤配置变更）
	mu              sync.RWMutex
	ctx             context.Context
	cancel          context.CancelFunc
}
