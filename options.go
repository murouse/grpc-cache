package grpccache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type Option func(cache *GrpcCache)

func WithLogger(logger *slog.Logger) Option {
	return func(c *GrpcCache) {
		c.logger = logger
	}
}

func WithNamespace(namespace string) Option {
	return func(c *GrpcCache) {
		c.namespace = namespace
	}
}

func WithVersion(version string) Option {
	return func(c *GrpcCache) {
		c.version = version
	}
}

func WithPolicyExtension(ext protoreflect.ExtensionType) Option {
	return func(c *GrpcCache) {
		c.policyExtension = ext
	}
}

func WithCacheFailurePolicy(policy CacheFailurePolicy) Option {
	return func(c *GrpcCache) {
		c.cacheFailurePolicy = policy
	}
}

func WithCache(cache Cache, cacheMissError error) Option {
	return func(c *GrpcCache) {
		c.cache = cache
		c.cacheMissError = cacheMissError
	}
}

func WithActorExtractor(extractor ActorExtractor) Option {
	return func(c *GrpcCache) {
		c.actorExtractor = extractor
	}
}

func WithCacheKeyFormatter(cacheKeyFormatterFunc cacheKeyFormatterFunc) Option {
	return func(c *GrpcCache) {
		c.cacheKeyFormatterFunc = cacheKeyFormatterFunc
	}
}

func defaultCacheKeyFormatter(namespace, version, fullMethod, actor string, req any) (string, error) {
	protoMessage, ok := req.(proto.Message)
	if !ok {
		return "", fmt.Errorf("request does not implement proto.Message")
	}

	reqBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(protoMessage)
	if err != nil {
		return "", fmt.Errorf("proto marshal request: %w", err)
	}

	h := sha256.New()
	h.Write(reqBytes)
	sum := hex.EncodeToString(h.Sum(nil))

	const sep = ":"

	return strings.Join([]string{"grpc-cache", namespace, version, fullMethod, actor, sum}, sep), nil
}
