## Why

Redis queue keys are already co-located with a queue-specific hash tag, but the
current backend creates a standalone Redis client and therefore cannot discover
or route to Redis Cluster nodes. Applications that deliberately use a Redis
Cluster need an explicit, supported backend rather than relying on a proxy or
an accidental single-node connection.

## What Changes

- Add an explicit Redis Cluster backend family for async FIFO, stack, priority,
  JSON, and event queues. Ordinary Redis backends remain standalone-client
  backends and do not infer Cluster topology.
- Use `redis.asyncio.cluster.RedisCluster` for the Cluster backend family and
  require database `0`.
- Use the configured `LOCATION` as a Cluster seed URL; the client discovers the
  remaining topology.
- Extend library deployment and compatibility diagnostics to recognise Cluster
  targets. Deployment loads the bundled `django_queues` Function library on
  every current Cluster primary; application compatibility validates that the
  selected Cluster client can invoke it.
- Document Cluster-specific configuration, required deployment permissions,
  primary-node deployment, and topology limitations.

## Capabilities

### New Capabilities

- `redis-cluster-backends`: Explicit Redis Cluster queue backends, topology
  configuration, client routing, and database restrictions.

### Modified Capabilities

- `redis-function-library`: Deploy and validate the Function library across
  the primaries of an explicitly configured Redis Cluster.

## Impact

- Redis backend construction, provider client lifecycle, management-command
  target resolution, command diagnostics, package exports, documentation, and
  integration test infrastructure.
- Uses the existing `redis-py>=5.2.1` dependency's asyncio Cluster client; no
  dependency upgrade is required.
- Cluster deployments require the library on every primary. Queue-owned keys
  retain their existing `{queue_alias}` hash tag so multi-key Functions remain
  single-slot operations.
