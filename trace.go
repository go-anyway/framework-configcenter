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
	"time"

	"github.com/go-anyway/framework-log"
	pkgtrace "github.com/go-anyway/framework-trace"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// executeWithTrace 带追踪的执行包装器
func executeWithTrace(
	ctx context.Context,
	operation string,
	configCenterType string,
	handler func(context.Context) error,
	enableTrace bool,
) error {
	startTime := time.Now()

	// 创建追踪 span
	var span trace.Span
	if enableTrace {
		ctx, span = pkgtrace.StartSpan(ctx, "configcenter."+operation,
			trace.WithAttributes(
				attribute.String("configcenter.operation", operation),
				attribute.String("configcenter.type", configCenterType),
			),
		)
		defer span.End()
	}

	// 执行操作
	err := handler(ctx)
	duration := time.Since(startTime)

	// 记录日志
	if err != nil {
		log.FromContext(ctx).Error("Config center operation failed",
			zap.String("operation", operation),
			zap.String("type", configCenterType),
			zap.Duration("duration", duration),
			zap.Error(err),
		)
	} else {
		log.FromContext(ctx).Debug("Config center operation completed",
			zap.String("operation", operation),
			zap.String("type", configCenterType),
			zap.Duration("duration", duration),
		)
	}

	// 更新追踪状态
	if enableTrace && span != nil {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		} else {
			span.SetStatus(codes.Ok, "")
		}
		span.SetAttributes(
			attribute.String("configcenter.duration_ms", duration.String()),
		)
	}

	return err
}

// getConfigWithTrace 带追踪的获取配置
func getConfigWithTrace(
	ctx context.Context,
	configCenterType string,
	key string,
	handler func(context.Context) (string, error),
	enableTrace bool,
) (string, error) {
	startTime := time.Now()

	// 创建追踪 span
	var span trace.Span
	if enableTrace {
		ctx, span = pkgtrace.StartSpan(ctx, "configcenter.get_config",
			trace.WithAttributes(
				attribute.String("configcenter.operation", "get_config"),
				attribute.String("configcenter.type", configCenterType),
				attribute.String("configcenter.key", key),
			),
		)
		defer span.End()
	}

	// 执行操作
	value, err := handler(ctx)
	duration := time.Since(startTime)

	// 记录日志
	if err != nil {
		log.FromContext(ctx).Error("Failed to get config",
			zap.String("type", configCenterType),
			zap.String("key", key),
			zap.Duration("duration", duration),
			zap.Error(err),
		)
	} else {
		log.FromContext(ctx).Debug("Config retrieved",
			zap.String("type", configCenterType),
			zap.String("key", key),
			zap.Duration("duration", duration),
		)
	}

	// 更新追踪状态
	if enableTrace && span != nil {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		} else {
			span.SetStatus(codes.Ok, "")
			span.SetAttributes(
				attribute.String("configcenter.value", value),
			)
		}
		span.SetAttributes(
			attribute.String("configcenter.duration_ms", duration.String()),
		)
	}

	return value, err
}

// getAllConfigsWithTrace 带追踪的获取所有配置
func getAllConfigsWithTrace(
	ctx context.Context,
	configCenterType string,
	handler func(context.Context) (map[string]string, error),
	enableTrace bool,
) (map[string]string, error) {
	startTime := time.Now()

	// 创建追踪 span
	var span trace.Span
	if enableTrace {
		ctx, span = pkgtrace.StartSpan(ctx, "configcenter.get_all_configs",
			trace.WithAttributes(
				attribute.String("configcenter.operation", "get_all_configs"),
				attribute.String("configcenter.type", configCenterType),
			),
		)
		defer span.End()
	}

	// 执行操作
	configs, err := handler(ctx)
	duration := time.Since(startTime)

	// 记录日志
	if err != nil {
		log.FromContext(ctx).Error("Failed to get all configs",
			zap.String("type", configCenterType),
			zap.Duration("duration", duration),
			zap.Error(err),
		)
	} else {
		log.FromContext(ctx).Debug("All configs retrieved",
			zap.String("type", configCenterType),
			zap.Int("count", len(configs)),
			zap.Duration("duration", duration),
		)
	}

	// 更新追踪状态
	if enableTrace && span != nil {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		} else {
			span.SetStatus(codes.Ok, "")
			span.SetAttributes(
				attribute.Int("configcenter.config_count", len(configs)),
			)
		}
		span.SetAttributes(
			attribute.String("configcenter.duration_ms", duration.String()),
		)
	}

	return configs, err
}

// configChangeWithTrace 带追踪的配置变更处理
func configChangeWithTrace(
	ctx context.Context,
	configCenterType string,
	key string,
	oldValue string,
	newValue string,
	handler func(context.Context) error,
	enableTrace bool,
) error {
	startTime := time.Now()

	// 创建追踪 span
	var span trace.Span
	if enableTrace {
		ctx, span = pkgtrace.StartSpan(ctx, "configcenter.config_change",
			trace.WithAttributes(
				attribute.String("configcenter.operation", "config_change"),
				attribute.String("configcenter.type", configCenterType),
				attribute.String("configcenter.key", key),
				attribute.String("configcenter.old_value", oldValue),
				attribute.String("configcenter.new_value", newValue),
			),
		)
		defer span.End()
	}

	// 执行操作
	err := handler(ctx)
	duration := time.Since(startTime)

	// 记录日志
	if err != nil {
		log.FromContext(ctx).Error("Failed to handle config change",
			zap.String("type", configCenterType),
			zap.String("key", key),
			zap.String("old_value", oldValue),
			zap.String("new_value", newValue),
			zap.Duration("duration", duration),
			zap.Error(err),
		)
	} else {
		log.FromContext(ctx).Info("Config change handled",
			zap.String("type", configCenterType),
			zap.String("key", key),
			zap.String("old_value", oldValue),
			zap.String("new_value", newValue),
			zap.Duration("duration", duration),
		)
	}

	// 更新追踪状态
	if enableTrace && span != nil {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		} else {
			span.SetStatus(codes.Ok, "")
		}
		span.SetAttributes(
			attribute.String("configcenter.duration_ms", duration.String()),
		)
	}

	return err
}
