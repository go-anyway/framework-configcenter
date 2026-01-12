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
	"strings"
)

// MatchPattern 匹配配置键模式
// 支持的模式：
//   - 精确匹配：pattern == key
//   - 前缀匹配：pattern 以 "." 结尾，匹配所有以 pattern 开头的 key
//   - 单级通配符：如 "business.api.*" 匹配 "business.api.timeout"，但不匹配 "business.api.retry.count"
//   - 多级通配符：如 "business.**" 匹配所有以 "business." 开头的配置
func MatchPattern(pattern, key string) bool {
	if pattern == "" {
		return false
	}

	// 精确匹配
	if pattern == key {
		return true
	}

	// 前缀匹配（模式以 "." 结尾）
	if strings.HasSuffix(pattern, ".") {
		return strings.HasPrefix(key, pattern)
	}

	// 多级通配符匹配（**）
	if strings.Contains(pattern, "**") {
		return matchMultiLevel(pattern, key)
	}

	// 单级通配符匹配（*）
	if strings.Contains(pattern, "*") {
		return matchSingleLevel(pattern, key)
	}

	return false
}

// matchSingleLevel 单级通配符匹配
// 例如：business.api.* 匹配 business.api.timeout，但不匹配 business.api.retry.count
func matchSingleLevel(pattern, key string) bool {
	patternParts := strings.Split(pattern, "*")
	if len(patternParts) != 2 {
		return false
	}

	prefix := patternParts[0]
	suffix := patternParts[1]

	if !strings.HasPrefix(key, prefix) {
		return false
	}

	remaining := key[len(prefix):]
	if suffix == "" {
		// 模式以 * 结尾，只要前缀匹配即可
		return true
	}

	if !strings.HasSuffix(remaining, suffix) {
		return false
	}

	// 检查中间部分不包含分隔符（单级通配符）
	middle := remaining[:len(remaining)-len(suffix)]
	return !strings.Contains(middle, ".")
}

// matchMultiLevel 多级通配符匹配
// 例如：business.** 匹配所有以 business. 开头的配置
func matchMultiLevel(pattern, key string) bool {
	patternParts := strings.Split(pattern, "**")
	if len(patternParts) != 2 {
		return false
	}

	prefix := patternParts[0]
	suffix := patternParts[1]

	if !strings.HasPrefix(key, prefix) {
		return false
	}

	if suffix == "" {
		// 模式以 ** 结尾，只要前缀匹配即可
		return true
	}

	remaining := key[len(prefix):]
	return strings.HasSuffix(remaining, suffix)
}
