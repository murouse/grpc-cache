package grpccache

import (
	"context"
	"time"
)

type Cache interface {
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) (string, error)
	Get(ctx context.Context, key string) (string, error)
}

type cacheKeyFormatterFunc func(namespace, version, fullMethod, actor string, req any) (string, error)

type ActorExtractor interface {
	IdentifierFromContext(ctx context.Context) (string, bool)
}
