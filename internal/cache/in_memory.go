package cache

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

type item struct {
	value     string
	expiresAt time.Time
}

type MemoryCache struct {
	mu    sync.Mutex
	items map[string]item
}

func New() *MemoryCache {
	return &MemoryCache{
		items: make(map[string]item),
	}
}

func (c *MemoryCache) Set(_ context.Context, key string, value interface{}, expiration time.Duration) (string, error) {
	var v string

	switch val := value.(type) {
	case string:
		v = val
	case []byte:
		v = string(val)
	default:
		a, err := json.Marshal(value)
		if err != nil {
			return "", err
		}
		v = string(a)
	}

	var expiresAt time.Time
	if expiration > 0 {
		expiresAt = time.Now().Add(expiration)
	}

	c.mu.Lock()
	c.items[key] = item{
		value:     v,
		expiresAt: expiresAt,
	}
	c.mu.Unlock()

	return "", nil
}

func (c *MemoryCache) Get(_ context.Context, key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	it, ok := c.items[key]
	if !ok {
		return "", ErrCacheMiss
	}

	if !it.expiresAt.IsZero() && time.Now().After(it.expiresAt) {
		delete(c.items, key)
		return "", ErrCacheMiss
	}

	return it.value, nil
}
