## Why

Redis provider operations currently use many isolated EVALSHA scripts. Shared
logic, especially scheduled-entry promotion, must be copied into each script,
which permits claim and direct-dequeue paths to diverge. Redis 7 has supported
persistent Function libraries for years and Redis 8 is the deployment target.

## What Changes

- Replace the provider's registered EVALSHA scripts with a versioned Redis
  Function library invoked through FCALL.
- Put reusable Lua helpers, including scheduled promotion, record/index
  cleanup, and priority-score handling, inside that library.
- Add a management command as the sole owner of library installation,
  replacement, upgrade, and rollback. Providers validate a loaded library's
  compatibility but never deploy it.
- Provide separate deployment-role and application-role diagnostics: the former
  checks Function-library support and performs explicit deployment, while the
  latter confirms that an application credential can invoke the installed
  library.
- Give the library a `library_version` for every bundled revision and a stable
  `api_version` compatibility field, so
  application releases can require a minimum compatible surface without
  blocking a rollback to an older application against a newer compatible
  library.
- Keep each public provider operation a single atomic FCALL; helpers are local
  calls within that function, never a chain of client-side FCALLs.
- Correct Redis path divergence so equivalent claim and direct-dequeue
  operations preserve the same eligibility, uniqueness, and cleanup rules.

## Capabilities

### New Capabilities

- `redis-function-library`: Redis Function library deployment, invocation, and
  compatibility requirements for the Redis provider.

### Modified Capabilities

- `redis-entry-claims`: preserve atomic single-entry dispatch semantics across
  Redis claim and direct-dequeue paths.

## Impact

- `django_queue.backends.redis.provider` and its Redis integration tests.
- Redis deployment requires version 7 or later. The management command needs
  FUNCTION LOAD permission; applications need FCALL permission.
- Existing EVALSHA script cache entries are unaffected during migration.
