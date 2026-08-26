## 1. Cluster backend foundation

- [x] 1.1 Add and export explicit Redis Cluster async FIFO, stack, priority, JSON, and event backend classes; verify each resolves from a `QUEUES` backend path.
- [x] 1.2 Add `QueueProviderRedisCluster` as a subclass of `QueueProviderRedis`; verify Cluster variants construct and close an asyncio Cluster client without changing standalone client behaviour.
- [x] 1.3 Validate Cluster backend `LOCATION` as a seed URL and reject a non-zero database; verify configured aliases accept database `0` and fail clearly for another database.

## 2. Cluster queue operations

- [x] 2.1 Route all existing Redis provider operations through the Cluster client boundary; verify the existing standalone Redis suite remains green without API or lifecycle regressions.
- [x] 2.2 Add a real multi-primary Redis Cluster integration fixture: custom `FROM redis:8` image running three cluster-enabled processes on distinct ports and data dirs, then `redis-cli --cluster create`. Build and run it with testcontainers `DockerImage` + `DockerContainer` (not `RedisContainer`). Verify a seed discovers the topology and queue operations are redirected to the hash-slot owner. Remap announced addresses for host-side clients.
- [x] 2.3 Exercise FIFO, stack, priority, scheduling, lifecycle/claim recovery, retained-entry lookup, raw values, and event delivery on Cluster variants; verify their observable results match standalone backends and no operation produces a cross-slot error. Prove lifecycle Pub/Sub with ordinary `PUBLISH`/`SUBSCRIBE` on the Cluster client before considering sharded Pub/Sub.

## 3. Function-library deployment and diagnostics

- [x] 3.1 Extend `redis_lua_lib --deploy` to identify configured Cluster targets and deploy the bundled library to every current primary via the sync Cluster client and `get_primaries()`; verify each primary reports the expected library and API versions. `--redis-url` visits only standalone targets; `--redis-cluster-url` visits only Cluster seeds; the flags are mutually exclusive. Distinct seed URLs remain distinct targets.
- [x] 3.2 Verify Cluster application compatibility and failure diagnostics when a primary lacks the library or FCALL permission; verify providers do not auto-deploy or use an EVALSHA fallback. Cluster providers FCALL `django_queue_info` with one hash-tagged queue key. Cluster `redis_lua_compat` FCALLs `django_queue_info` on every current primary. The Function description documents that optional routing key without changing behaviour or `api_version`.
- [x] 3.3 Cover Cluster topology changes in command and integration tests; verify rerunning explicit deployment installs the library on a newly introduced primary.

## 4. Documentation and validation

- [x] 4.1 Document Cluster backend selection, database-zero restriction, seed URL configuration, all-primary Function deployment, and post-topology-change deployment procedure; verify examples use explicit Cluster backend paths.
- [x] 4.2 Run the full test suite, Ruff, Ty, strict OpenSpec validation, and `git diff --check`; verify the new Cluster specs and implementation pass all checks.
