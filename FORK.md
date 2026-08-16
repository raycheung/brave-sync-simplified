# This fork

`brave-sync-simplified` is a fork of [`brave/go-sync`](https://github.com/brave/go-sync)
for running a self-hosted Brave Sync server without AWS DynamoDB or Redis. It's meant to
stay in continuous sync with upstream — periodically pulling in upstream's changes — not
a one-time branch-off, so the changes here are kept as small and additive as possible.

## Why

Upstream `go-sync` is built to run at Brave's scale: DynamoDB as the datastore, Redis as a
cache, multi-instance deployment. For a self-hosted, single-instance server serving a
handful of personal devices, that's infrastructure with no payoff — DynamoDB and Redis
each mean another container (or another cloud dependency) to run, patch, and restart,
purely to hold data that fits comfortably in a single file.

This fork keeps 100% of Brave's protocol, auth, and protobuf handling untouched, and adds
an alternative storage backend alongside the existing one.

## What changed

- **Datastore**: added [bbolt](https://github.com/etcd-io/bbolt), an embedded,
  file-backed, pure-Go key-value store, as an alternative to DynamoDB. New file:
  `datastore/bolt.go`, implementing the same `datastore.Datastore` interface
  `datastore/dynamo.go` implements. Configured via `BOLT_PATH` (default
  `/data/brave-sync.db`); the parent directory is created automatically if missing.
- **Cache**: added [go-cache](https://github.com/patrickmn/go-cache), an in-process TTL
  cache, as an alternative to Redis. New file: `cache/memcache.go`, implementing the same
  `cache.RedisClient` interface `cache/redis.go` implements.
- **Wiring**: `server/server.go` now constructs `datastore.NewBolt()` and
  `cache.NewMemCache()` (still wrapped in the existing generic
  `cache.NewRedisClientWithPrometheus` decorator) instead of `datastore.NewDynamo()` and
  `cache.NewRedisClient()`. That's the entire functional diff in `server.go` — a handful
  of token substitutions, nothing restructured.
- **`datastore/dynamo.go` and `cache/redis.go` are kept, not deleted.** They're simply
  unreferenced by `server.go` now. This is deliberate: deleting them would mean every
  future upstream change to those files turns into a merge conflict when catching up with
  `brave/go-sync`; keeping them lets those catch-ups apply as an ordinary 3-way merge.
  Don't "clean up" this apparently-dead code — that's the point of it being there.
- **Added**: `datastore/bolt_test.go`, a unit test suite exercising `Bolt` directly
  (insert/conflict, optimistic-concurrency update, mtime-ordered pagination, folder
  filtering, tag uniqueness, item counts, clear-server-data, disabled-chain marker
  survival).
- `go.mod`: added `go.etcd.io/bbolt` and `github.com/patrickmn/go-cache`. Nothing removed
  — the DynamoDB/Redis dependencies stay, since `dynamo.go`/`redis.go` still need them.

## What didn't change

Everything else: `auth/`, `command/`, `controller/`, `middleware/`, `schema/protobuf/`,
`synccontext/`, the `Datastore`/`Cache` interface definitions themselves
(`datastore/datastore.go`, `cache/cache.go`), the generated Prometheus instrumentation
wrappers (`datastore/instrumented_datastore.go`, `cache/instrumented_redis.go` — both
generic over their interface, so they work unmodified on `Bolt`/`memCache` too), Sentry
error reporting, the `/health-check` endpoint, and the DynamoDB/Redis integration test
suites (they still require live DynamoDB Local + Redis via `make docker-test`, same as
upstream).

Deployment scale for the bbolt/go-cache path is intentionally small (a personal/family
device count, well under the kind of fleet DynamoDB is built for), and the service is
expected to be safe to restart at any time — there is no HA/clustering story for that
path, by design.
