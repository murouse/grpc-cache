package grpccache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/murouse/golgi/attr"
	pb "github.com/murouse/grpc-cache/pkg/api/murouse/grpc-cache/v1"
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
			if err = proto.Unmarshal([]byte(value), res); err != nil {
				c.logger.ErrorContext(ctx, "proto unmarshal response", attr.Error(err))
				return nil, fmt.Errorf("unmarshal cache response: %w", err)
			}
			return res, nil

		case !errors.Is(err, c.cacheMissError): // Если ошибка кэша
			switch c.cacheFailurePolicy {
			case CacheFailurePolicyFallbackToHandler:
				return fallback()
			case CacheFailurePolicyReturnError:
				return nil, fmt.Errorf("cache get")
			default:
				return nil, fmt.Errorf("unknown cache failure policy: %d", c.cacheFailurePolicy)
			}
		}

		// Если cache miss
		res, err := fallback()
		if err != nil {
			return res, err // при ошибке хендлера возвращаем как есть
		}

		r, ok := res.(proto.Message)
		if !ok {
			c.logger.ErrorContext(ctx, "response does not implement proto.Message")
			return res, nil
		}

		b, err := proto.MarshalOptions{Deterministic: true}.Marshal(r)
		if err != nil {
			c.logger.ErrorContext(ctx, "marshal proto response", attr.Error(err))
			return res, nil
		}

		if _, err = c.cache.Set(ctx, key, b, policy.TTL); err != nil {
			c.logger.ErrorContext(ctx, "cache set", attr.Error(err))
		}

		return res, nil

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
