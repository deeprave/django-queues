## 1. Cluster backend foundation

- [ ] 1.1 Add and export explicit Redis Cluster async FIFO, stack, priority, JSON, and event backend classes; verify each resolves from a `QUEUES` backend path.
- [ ] 1.2 Refactor Redis provider client construction behind a loop-affine overridable boundary; verify Cluster variants construct and close an asyncio Cluster client without changing standalone client behaviour.
- [ ] 1.3 Validate Cluster backend `LOCATION` as a seed URL and reject a non-zero database; verify configured aliases accept database `0` and fail clearly for another database.

## 2. Cluster queue operations

- [ ] 2.1 Route all existing Redis provider operations through the Cluster client boundary; verify the existing standalone Redis suite remains green without API or lifecycle regressions.
- [ ] 2.2 Add a real multi-primary Redis Cluster integration fixture; verify a seed node discovers the topology and queue operations are redirected to the owner of the configured queue hash slot.
- [ ] 2.3 Exercise FIFO, stack, priority, scheduling, lifecycle/claim recovery, retained-entry lookup, raw values, and event delivery on Cluster variants; verify their observable results match standalone backends and no operation produces a cross-slot error.

## 3. Function-library deployment and diagnostics

- [ ] 3.1 Extend `redis_lua_lib --deploy` to identify configured Cluster targets and deploy the bundled library to every current primary; verify each primary reports the expected library and API versions.
- [ ] 3.2 Verify Cluster application compatibility and failure diagnostics when a primary lacks the library or FCALL permission; verify providers do not auto-deploy or use an EVALSHA fallback.
- [ ] 3.3 Cover Cluster topology changes in command and integration tests; verify rerunning explicit deployment installs the library on a newly introduced primary.

## 4. Documentation and validation

- [ ] 4.1 Document Cluster backend selection, database-zero restriction, seed URL configuration, all-primary Function deployment, and post-topology-change deployment procedure; verify examples use explicit Cluster backend paths.
- [ ] 4.2 Run the full test suite, Ruff, Ty, strict OpenSpec validation, and `git diff --check`; verify the new Cluster specs and implementation pass all checks.
