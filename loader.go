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
	"os"
	"path/filepath"
	"regexp"

	"gopkg.in/yaml.v3"
)

// DetectType 检测配置中心类型
// 优先级：环境变量 > 配置文件
func DetectType(configDir string) Type {
	// 1. 先检查环境变量
	if envType := os.Getenv("CONFIG_CENTER_TYPE"); envType != "" {
		centerType := Type(envType)
		if isValidType(centerType) {
			return centerType
		}
	}

	// 2. 检查配置文件
	configFile := filepath.Join(configDir, "config_center.yaml")
	if data, err := os.ReadFile(configFile); err == nil {
		// 展开环境变量（支持 ${VAR:-default} 格式）
		data = expandEnvVars(data)

		var cfg Config
		if err := yaml.Unmarshal(data, &cfg); err == nil && cfg.Type != "" {
			if isValidType(Type(cfg.Type)) {
				return Type(cfg.Type)
			}
		}
	}

	// 3. 默认不使用配置中心
	return TypeNone
}

// LoadFromFile 从文件加载配置中心配置
func LoadFromFile(configDir string) (*Config, error) {
	configFile := filepath.Join(configDir, "config_center.yaml")
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		// 文件不存在，返回默认配置
		return &Config{
			Type: "none",
		}, nil
	}

	data, err := os.ReadFile(configFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read config center config file: %w", err)
	}

	// 展开环境变量（支持 ${VAR:-default} 格式）
	data = expandEnvVars(data)

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config center config file: %w", err)
	}

	// 如果类型未设置，默认使用 none
	if cfg.Type == "" {
		cfg.Type = "none"
	}

	return &cfg, nil
}

// isValidType 检查配置中心类型是否有效
func isValidType(t Type) bool {
	switch t {
	case TypeNone, TypeApollo, TypeNacos, TypeConsul:
		return true
	default:
		return false
	}
}

// expandEnvVars 展开环境变量引用，支持 ${VAR:-default} 格式
func expandEnvVars(data []byte) []byte {
	str := string(data)

	// 匹配 ${VAR:-default} 或 ${VAR} 格式
	re := regexp.MustCompile(`\$\{([^:}]+)(?::-([^}]*))?\}`)

	result := re.ReplaceAllStringFunc(str, func(match string) string {
		parts := re.FindStringSubmatch(match)
		if len(parts) < 2 {
			return match
		}

		varName := parts[1]
		defaultValue := ""
		if len(parts) > 2 {
			defaultValue = parts[2]
		}

		value := os.Getenv(varName)
		if value == "" {
			return defaultValue
		}
		return value
	})

	return []byte(result)
}
