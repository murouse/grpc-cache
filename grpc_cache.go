package grpccache

import (
	"log/slog"
	"sync"

	"github.com/murouse/grpc-cache/internal/cache"
	pb "github.com/murouse/grpc-cache/pkg/api/murouse/grpc-cache/v1"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type GrpcCache struct {
	cache          Cache
	actorExtractor ActorExtractor

	namespace string
	version   string
	logger    *slog.Logger

	policyExtension       protoreflect.ExtensionType
	cacheFailurePolicy    CacheFailurePolicy
	cacheKeyFormatterFunc cacheKeyFormatterFunc
	cacheMissError        error

	methodPolicies  map[string]Policy
	methodRulesOnce sync.Once
}

func New(opts ...Option) *GrpcCache {
	gc := &GrpcCache{
		cache:                 cache.New(),
		actorExtractor:        nil,
		namespace:             "default",
		version:               "v1",
		logger:                slog.New(slog.DiscardHandler),
		policyExtension:       pb.E_Policy,
		cacheFailurePolicy:    CacheFailurePolicyReturnError,
		cacheKeyFormatterFunc: defaultCacheKeyFormatter,
		cacheMissError:        cache.ErrCacheMiss,
	}

	for _, opt := range opts {
		opt(gc)
	}

	return gc
}
