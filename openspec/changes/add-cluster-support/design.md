## Context

The queue key layout already uses a literal `{queue_alias}` hash tag, so each
queue's record, lists, and sorted sets map to one Redis Cluster slot. The
current provider nevertheless creates `redis.asyncio.Redis`, which cannot
discover Cluster nodes or route redirects. The bundled Function library must
exist on every primary that may receive an FCALL.

## Goals / Non-Goals

**Goals:**

- Provide an explicit, documented Cluster backend family with the same public
  queue semantics as the standalone Redis family.
- Route operations through a Cluster-aware asyncio client while retaining the
  existing single-slot Function contract.
- Deploy and verify the Function library across current Cluster primaries.

**Non-Goals:**

- Automatically detect Cluster topology for ordinary Redis backends.
- Add Sentinel, multi-primary independent Redis, proxy-specific, or cross-slot
  queue support.
- Preserve or migrate a queue to a different hash tag or Redis database.

## Decisions

### Use separate backend classes

Expose `RedisClusterAsyncQueue`, `RedisClusterAsyncQueueJson`,
`RedisClusterAsyncStack`, `RedisClusterAsyncStackJson`,
`RedisClusterAsyncPriorityQueue`, `RedisClusterAsyncPriorityQueueJson`, and
`RedisClusterEventQueue`. They select Cluster behaviour in `QUEUES` directly.
Ordinary Redis classes remain standalone and do no probe for Cluster mode.

This makes topology, database restrictions, support expectations, and
deployment behaviour visible in configuration. A `cluster=True` option was
rejected because it makes a backend's transport semantics less apparent; URL
auto-detection was rejected because it adds an ACL-sensitive discovery call and
can be ambiguous behind managed-service endpoints or proxies.

### Use one seed URL and require database zero

`LOCATION` stays one Redis URL and acts as a Cluster seed. `RedisCluster`
discovers the remaining primaries and slot map from it. Cluster backends reject
a non-zero URL database because Redis Cluster supports only database zero.

Multiple seed URLs are intentionally deferred. A single reachable seed is
sufficient for discovery, keeps `LOCATION` semantics stable, and avoids
overloading it with a topology-specific list format.

### Share provider operations through `QueueProviderRedisCluster`

`QueueProviderRedisCluster` subclasses `QueueProviderRedis`. It constructs
`redis.asyncio.cluster.RedisCluster` from the seed URL, rejects a non-zero
database, FCALLs `django_queue_info` with `{alias}`, and closes a Cluster
client. Queue operations stay on the base class.

The application provider does not enumerate primaries. `get_primaries()` is
only for management commands and tests, using the **sync**
`redis.cluster.RedisCluster` client after `initialize()`.
`redis_lua_compat` uses that same primary walk so an application credential
can FCALL `django_queue_info` on every current primary without `FUNCTION LIST`.

Pub/Sub stays `publish` / `subscribe` on the Cluster client. `aobserve` today
opens a second standalone `from_url` client; the Cluster subclass must open a
Cluster client instead. Do not adopt sharded Pub/Sub unless a live multi-primary
test shows ordinary Pub/Sub failing.

### Live Cluster fixture: one Redis 8 image, three processes

A single `redis-server` cannot prove Cluster routing. The integration fixture
uses a custom image `FROM redis:8` that starts three `redis-server` processes
on distinct ports, each with its own data directory, `cluster-enabled yes`, and
its own `cluster-config-file`. An entrypoint waits until all three PING, then
`redis-cli --cluster create … --cluster-replicas 0 --cluster-yes`.

Gossip uses each node's cluster bus port inside the container (default
`port + 10000`); those need not be published. Clients use one published seed
port. For Compose on a Docker network, set `cluster-announce-ip` to the service
name so `CLUSTER SLOTS` returns addresses other containers can dial. For
pytest-on-host via published ports, remap `CLUSTER SLOTS` addresses with
redis-py `address_remap` (or equivalent) from container `host:port` to
`get_container_host_ip():published_port`. Do not expect unmapped `127.0.0.1:7000`
inside the container to be reachable from the Mac/Windows Docker host.

Testcontainers can build that Dockerfile (`DockerImage`) and run it as a
`DockerContainer` that publishes the three client ports. Do not reuse
`RedisContainer`, which assumes one `redis-server` on 6379 and a standalone
client. Wait until cluster create has finished (image `HEALTHCHECK` or an
equivalent wait strategy), not merely until one process PINGs.

### Deploy Function libraries per current primary

For a configured Cluster backend, `redis_lua_lib --deploy` obtains the current
primary-node set from the Cluster client and runs the existing explicit,
lease-protected library deployment procedure against each primary. It reports
the node that failed and succeeds only after all current primaries contain the
bundled library. The application provider uses the Cluster client; its
normal FCALL compatibility gate reports a missing or incompatible library
on the node that serves the queue's slot. `redis_lua_compat` additionally walks
every current primary with a direct 0-key `django_queue_info` FCALL so a
missing library on a primary that does not own a configured queue is still
detected. It stays a separate command from `redis_lua_lib` because managed Redis
typically splits Function-management ACLs from application FCALL.

`--redis-url` is a standalone-only override: it is the sole target and Cluster
aliases are not visited. `--redis-cluster-url` is a Cluster-only override: it
is the sole seed and standalone aliases are not visited. The two options are
mutually exclusive. Without either flag, commands walk `QUEUES` and handle each
unique `LOCATION` according to that alias's backend topology.

Two distinct Cluster seed URLs are two deploy targets, even if they belong to
the same Redis Cluster. That is a user configuration error; the commands do not
merge them. Application providers never compare topology with each other.

If a management command opens more than one Cluster client in one run, it MAY
warn by comparing Redis Cluster node ids from `CLUSTER NODES` (not
`ClusterNode.name`, which is only `host:port`). Matching id sets mean the seeds
discovered the same cluster. Double `FUNCTION LOAD` on the same primaries is
idempotent when the bundled revision already matches.

Redis Functions are replicated to replicas, so deployment targets primaries.
After a topology change that introduces a new primary, operators rerun
`redis_lua_lib --deploy` before application work is routed there. Provider code
does not load libraries automatically.

### Retain the existing hash-tag schema and Function surface

No queue-key migration, new Function naming convention, or API-version bump is
needed solely for Cluster support. Existing queue-owned keys already share the
same resolved alias hash tag, so each Function's explicit keys are single-slot.
The same stable Function library is loaded on each primary.

### Route `django_queue_info` with an optional tagged key

`django_queue_info` ignores `keys` and `args`. A 0-key FCALL is valid, but on
Cluster redis-py sends that call to a random slot, so a compatibility check can
pass on a loaded primary and the next real FCALL fail on the primary that owns
the queue. Cluster providers therefore pass one queue-owned hash-tagged key
(`{alias}` is enough) so introspection runs on that queue's slot owner. Cluster
`redis_lua_compat` instead opens a direct client to each current primary and
FCALLs `django_queue_info` with no keys, so every primary is checked. Lua does
not read the key. `api_version` stays unchanged.

The registered Function description documents this optional routing key. That is
metadata only: behaviour and the 0-key call remain valid.

## Risks / Trade-offs

- [A seed endpoint is unavailable during startup] → RedisCluster reports an
  actionable connection/discovery failure; configure a reachable Cluster
  endpoint and retry deployment.
- [A failover or scale-out introduces a new primary without the library] → rerun
  the explicit deployment command; the provider fails clearly rather than
  falling back to scripts.
- [A provider operation accidentally touches a differently tagged key] → add
  live Cluster integration coverage for every operation family and retain
  strict alias validation.
- [Cluster client behaviour differs in Pub/Sub, scanning, or blocking calls] →
  cover async queue, priority, lifecycle, and event delivery against a real
  multi-primary Cluster before documenting support. Start Pub/Sub with ordinary
  `PUBLISH`/`SUBSCRIBE` on `RedisCluster` and only change protocol if that test
  fails.
- [Two seed URLs name one Cluster] → treat as two targets; optional warning from
  matching node-id sets after discovery. Do not merge.

## Migration Plan

1. Release the new Cluster backend classes without changing ordinary Redis
   backend behaviour.
2. Configure Cluster aliases with database `0` and a Cluster seed URL.
3. Run `python manage.py redis_lua_lib --deploy` with deployment credentials to
   install the library on current primaries.
4. Run `python manage.py redis_lua_compat` with the application credential,
   then start applications and workers using the Cluster backends.
5. Roll back by selecting the standalone backend only when the target is a
   standalone Redis deployment; rollback between Cluster application releases
   continues to use the compatible Function-library deployment procedure.
