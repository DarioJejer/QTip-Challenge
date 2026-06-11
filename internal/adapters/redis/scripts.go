package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// idempotencyLuaScript is the atomic check-and-set script used by the
// IdempotencyStore to acquire a processing lock without TOCTOU races (ADR-006).
//
// Arguments:
//
//	KEYS[1]  — idempotency key (e.g. "idempotency:{taskID}")
//	ARGV[1]  — workerID (stored for debugging)
//	ARGV[2]  — TTL in seconds
//	ARGV[3]  — ISO-8601 timestamp of acquisition
//
// Returns:
//
//	1 if the lock was acquired (key did not exist or had expired)
//	0 if the key already exists with status "processing" or "completed"
const idempotencyLuaScript = `
local key = KEYS[1]
local workerID = ARGV[1]
local ttl = tonumber(ARGV[2])
local startedAt = ARGV[3]
local existing = redis.call('GET', key)
if existing then
  local data = cjson.decode(existing)
  if data['status'] == 'completed' then return 0 end
  if data['status'] == 'processing' then return 0 end
end
local payload = cjson.encode({status='processing', worker_id=workerID, started_at=startedAt})
redis.call('SET', key, payload, 'EX', ttl)
return 1
`

// reclaimStaleLuaScript atomically re-acquires a processing lock that was set
// longer than maxAgeSeconds ago. It is used by TryReclaimStale to recover
// tasks whose original worker crashed without calling ClearProcessing.
//
// Arguments:
//
//	KEYS[1]  — idempotency key
//	ARGV[1]  — new workerID
//	ARGV[2]  — TTL in seconds
//	ARGV[3]  — new started_at Unix timestamp (string)
//	ARGV[4]  — maxAgeSeconds threshold
//	ARGV[5]  — current Unix timestamp (nowUnix)
//
// Returns:
//
//	1 if the stale lock was reclaimed
//	0 if the lock is absent, non-processing, or still within maxAge
const reclaimStaleLuaScript = `
local key = KEYS[1]
local existing = redis.call('GET', key)
if not existing then return 0 end
local data = cjson.decode(existing)
if data['status'] ~= 'processing' then return 0 end
local existingTime = tonumber(data['started_at'])
if not existingTime then return 0 end
local nowUnix = tonumber(ARGV[5])
local maxAge = tonumber(ARGV[4])
if (nowUnix - existingTime) < maxAge then return 0 end
local ttl = tonumber(ARGV[2])
local payload = cjson.encode({status='processing', worker_id=ARGV[1], started_at=ARGV[3]})
redis.call('SET', key, payload, 'EX', ttl)
return 1
`

// ScriptRegistry holds the SHA1 digests of Lua scripts pre-loaded into Redis
// via SCRIPT LOAD. Using SHAs with EVALSHA instead of inline EVAL reduces
// bandwidth and avoids re-sending the script body on every call.
type ScriptRegistry struct {
	// IdempotencySHA is the SHA1 of the idempotency check-and-set Lua script.
	IdempotencySHA string
	// ReclaimStaleSHA is the SHA1 of the stale-lock reclaim Lua script.
	ReclaimStaleSHA string
}

// LoadScripts loads all Lua scripts into Redis and stores their SHAs in the
// returned ScriptRegistry. Call this once during the bootstrap sequence
// (main.go step 4), before starting workers.
//
// SCRIPT LOAD is idempotent — reloading the same script returns the same SHA.
func LoadScripts(ctx context.Context, client *redis.Client) (*ScriptRegistry, error) {
	idempotencySHA, err := client.ScriptLoad(ctx, idempotencyLuaScript).Result()
	if err != nil {
		return nil, fmt.Errorf("redis: load idempotency script: %w", err)
	}

	reclaimSHA, err := client.ScriptLoad(ctx, reclaimStaleLuaScript).Result()
	if err != nil {
		return nil, fmt.Errorf("redis: load reclaim-stale script: %w", err)
	}

	log.Info().
		Str("component", "redis").
		Str("idempotency_sha", idempotencySHA).
		Str("reclaim_stale_sha", reclaimSHA).
		Msg("redis: Lua scripts loaded")

	return &ScriptRegistry{
		IdempotencySHA:  idempotencySHA,
		ReclaimStaleSHA: reclaimSHA,
	}, nil
}
