# Changelog

## v1.2.0 - 2026-08-23

- ⚠️ **BREAKING CHANGE:** Redis-backed queues now require the bundled `django_queues` Redis Function library to be deployed before applications start. Deploy it with `python manage.py redis_lua_lib --deploy` using a credential with Redis Function-management permission; applications fail clearly when the library is absent, incompatible, or FCALL is denied. Redis 7 or later is now required for Redis-backed queues.
- ⚠️ **BREAKING CHANGE:** Redis queue keys now use a Cluster hash-tagged alias. Existing Redis queue state is not compatible with this key layout.
- Replaced the Redis provider's per-client EVALSHA scripts with a shared, versioned Redis Function library. Atomic queue operations now reuse common server-side scheduling and priority helpers.
- Added `redis_lua_lib` for deployment-role library checks and explicit deployment, and `redis_lua_compat` for application-role FCALL compatibility checks. `--redis-url` is available as an exceptional single-target override; otherwise both commands use configured Redis queue locations.
- Redis queue keys now share a queue-alias Cluster hash tag. Queue aliases must contain only ASCII letters, digits, `_`, or `-`.

## v1.1.0 - 2026-08-21

- Added scheduled availability for identified async queues: pass an absolute `ClockTime` as `available_at` (for example, from an upstream `run_after` value) to keep work queued without reserving a worker until it is due. Past and omitted instants remain immediately eligible.
- Redis queues durably schedule and promote work atomically; memory queues provide matching behaviour. Scheduled work is released by availability time, then priority within the same availability group, before entering ordinary dispatch.
- Event queues accept and ignore `available_at` for API compatibility. Added scheduled-work lifecycle cleanup and compatibility coverage, including an experimental Python 3.15 development CI lane.

## v1.0.3 - 2026-08-20

- Added priority-aware dispatch for tracked entries on memory and Redis priority queues. `enqueue(..., priority=0)` now records an entry priority; higher priorities dispatch first and equal priorities retain arrival order.

## v1.0.2 — 2026-08-19

- `queue_observer` now supports decorator syntax (`@queue_observer("alias")`) alongside the existing direct call, deferring backend activation until the shared runtime starts.
- Fixed a bug where two threads touching the same configured `AsyncQueue` alias for the first time could each get their own lifecycle-observer receiver and Redis connection, instead of sharing one. Event workers and observer receivers for all configured queues now run on a single shared background thread, started once at process startup. No API changes; not a breaking change.

## v1.0.1 — 2026-08-17

- Fixed the Redis queue observer to run fully asynchronously, matching the rest of the Redis backend. No API changes; not a breaking change.

## v1.0.0 — 2026-08-17

- Initial release
