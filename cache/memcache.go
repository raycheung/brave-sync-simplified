package cache

import (
	"context"
	"strconv"
	"sync"
	"time"

	gocache "github.com/patrickmn/go-cache"
)

// memCache implements RedisClient (declared in redis.go) backed by an
// in-process go-cache store,
// replacing the Redis-backed implementation for single-instance deployments.
type memCache struct {
	c  *gocache.Cache
	mu sync.Mutex // guards read-modify-write Incr operations
}

// NewMemCache creates an in-memory RedisClient implementation. Keys default
// to a 10 minute TTL (matching the TTL cache.go already applies to mtime
// entries) unless a caller-supplied ttl overrides it via Set.
func NewMemCache() RedisClient {
	return &memCache{c: gocache.New(10*time.Minute, 10*time.Minute)}
}

func (m *memCache) Set(_ context.Context, key string, val string, ttl time.Duration) error {
	m.c.Set(key, val, ttl)
	return nil
}

func (m *memCache) Get(_ context.Context, key string, deleteAfterGet bool) (string, error) {
	v, found := m.c.Get(key)
	if !found {
		return "", nil
	}
	if deleteAfterGet {
		m.c.Delete(key)
	}
	return v.(string), nil //nolint:forcetypeassert // only this file ever writes into the cache, always as string
}

func (m *memCache) Incr(_ context.Context, key string, subtract bool) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var current int
	if v, found := m.c.Get(key); found {
		s, _ := v.(string)
		current, _ = strconv.Atoi(s)
	}
	if subtract {
		current--
	} else {
		current++
	}
	m.c.Set(key, strconv.Itoa(current), gocache.DefaultExpiration)
	return current, nil
}

func (m *memCache) Del(_ context.Context, keys ...string) error {
	for _, k := range keys {
		m.c.Delete(k)
	}
	return nil
}

func (m *memCache) FlushAll(_ context.Context) error {
	m.c.Flush()
	return nil
}
