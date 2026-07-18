package grpccache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/murouse/golgi/attr"
	pb "github.com/murouse/grpc-cache/pkg/api/murouse/grpc_cache/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

func (c *GrpcCache) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (
		any, error,
	) {
		fallback := func() (any, error) { return handler(ctx, req) }

		// Получаем политику метода
		policy, hasPolicy := c.getMethodPolicy()[info.FullMethod]
		if !hasPolicy {
			return fallback() // Если политики нет, пропускаем
		}

		// Вычисляем актора
		var actorIdentifier string
		if policy.ActorScoped {
			if c.actorExtractor == nil {
				return nil, fmt.Errorf("actor extractor not set")
			}

			a, ok := c.actorExtractor.IdentifierFromContext(ctx)
			if !ok {
				return nil, fmt.Errorf("cannot extract actor")
			}
			actorIdentifier = a
		}

		// Формируем ключ
		key, err := c.cacheKeyFormatterFunc(c.namespace, c.version, info.FullMethod, actorIdentifier, req)
		if err != nil {
			return nil, fmt.Errorf("cache key format: %w", err)
		}

		// Получаем из кэша
		value, err := c.cache.Get(ctx, key)

		switch {
		case err == nil: // Если нашли в кэше
			res := policy.ResponseType.New().Interface() // Создаем response
			if uErr := proto.Unmarshal([]byte(value), res); uErr != nil {
				// Битая запись в кэше: не отдаём ошибку клиенту, лечим как промах —
				// пересчитаем через хендлер и перезапишем ниже.
				c.logger.ErrorContext(ctx, "proto unmarshal cached response, recomputing", attr.Error(uErr))
			} else {
				return res, nil
			}

		case !errors.Is(err, c.cacheMissError): // Если ошибка кэша (не промах)
			switch c.cacheFailurePolicy {
			case CacheFailurePolicyFallbackToHandler:
				return fallback()
			case CacheFailurePolicyReturnError:
				return nil, fmt.Errorf("cache get: %w", err)
			default:
				return nil, fmt.Errorf("unknown cache failure policy: %d", c.cacheFailurePolicy)
			}
		}

		// Промах (или битая запись): считаем ответ через хендлер.
		// singleflight схлопывает конкурентные запросы с одним ключом в один вызов.
		res, err, _ := c.sfGroup.Do(key, func() (any, error) {
			r, hErr := handler(ctx, req)
			if hErr != nil {
				return r, hErr // ошибку хендлера не кэшируем
			}

			msg, ok := r.(proto.Message)
			if !ok {
				c.logger.ErrorContext(ctx, "response does not implement proto.Message")
				return r, nil
			}

			b, mErr := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
			if mErr != nil {
				c.logger.ErrorContext(ctx, "marshal proto response", attr.Error(mErr))
				return r, nil
			}

			// Отвязываем контекст: отмена/дедлайн клиента не должны срывать запись в кэш.
			if _, sErr := c.cache.Set(context.WithoutCancel(ctx), key, b, policy.TTL); sErr != nil {
				c.logger.ErrorContext(ctx, "cache set", attr.Error(sErr))
			}

			return r, nil
		})

		return res, err
	}
}

func (c *GrpcCache) getMethodPolicy() map[string]Policy {
	c.methodRulesOnce.Do(c.loadMethodPolicies)
	return c.methodPolicies
}

// loadMethodPolicies один раз рефлексией проходимся и сохраняем в память опции методов
func (c *GrpcCache) loadMethodPolicies() {
	files := protoregistry.GlobalFiles
	policiesMap := make(map[string]Policy)

	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		for i := 0; i < fd.Services().Len(); i++ {
			service := fd.Services().Get(i)

			for j := 0; j < service.Methods().Len(); j++ {
				method := service.Methods().Get(j)
				fullMethodName := fmt.Sprintf("/%s/%s", service.FullName(), method.Name())

				options := method.Options().(*descriptorpb.MethodOptions)
				if options == nil || !proto.HasExtension(options, c.policyExtension) {
					continue
				}

				outputMessageType, err := protoregistry.GlobalTypes.FindMessageByName(method.Output().FullName())
				if err != nil {
					c.logger.Error("cannot resolve response type",
						slog.String("method", fullMethodName),
						slog.Any("error", err),
					)
					continue
				}

				extension := proto.GetExtension(options, c.policyExtension)
				if policy, ok := extension.(*pb.Policy); ok {
					policiesMap[fullMethodName] = PolicyToModel(policy, outputMessageType)
				}
			}
		}

		return true
	})

	c.methodPolicies = policiesMap
	c.logger.Debug("loaded methods policies", slog.Int("count", len(policiesMap)))
}
