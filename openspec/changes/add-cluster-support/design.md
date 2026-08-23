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

### Share provider operations through a Cluster client factory

Refactor the Redis provider's client construction behind a small overridable
factory or provider type. The Cluster provider constructs
`redis.asyncio.cluster.RedisCluster`; it retains the same per-event-loop
lifecycle and provider operation implementations. The Cluster client has the
async command surface needed by the provider, but is not a subclass of the
standalone async Redis client, so internal annotations and construction must
not depend on concrete standalone-client identity.

### Deploy Function libraries per current primary

For a configured Cluster backend, `redis_lua_lib --deploy` obtains the current
primary-node set from the Cluster client and runs the existing explicit,
lease-protected library deployment procedure against each primary. It reports
the node that failed and succeeds only after all current primaries contain the
bundled library. The application command and providers use the Cluster client;
their normal FCALL compatibility gate reports a missing or incompatible library
on the node that serves the queue's slot.

Redis Functions are replicated to replicas, so deployment targets primaries.
After a topology change that introduces a new primary, operators rerun
`redis_lua_lib --deploy` before application work is routed there. Provider code
does not load libraries automatically.

### Retain the existing hash-tag schema and Function surface

No queue-key migration, new Function naming convention, or API-version bump is
needed solely for Cluster support. Existing queue-owned keys already share the
same resolved alias hash tag, so each Function's explicit keys are single-slot.
The same stable Function library is loaded on each primary.

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
  multi-primary Cluster before documenting support.

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
