## 1. Function library foundation

- [x] 1.1 Define a namespaced, versioned Redis Function library source and its
  shared Lua helper conventions, including a stable introspection entry point,
  `library_version`, and `api_version` compatibility field.
- [x] 1.2 Add `redis_lua_lib` for deployment-role support and permission
  checks, with `--deploy` as the only explicit install/replace mode for upgrade
  and rollback.
- [x] 1.3 Add `redis_lua_compat` for application-role FCALL compatibility
  checks against the stable introspection entry point.
- [x] 1.4 Add provider support to validate and cache library compatibility per
  async client/loop before FCALL, without deploying the library, and to report
  operation-specific runtime permission failures.
- [x] 1.5 Add test fixtures for management-command deployment, application
  compatibility, absent or incompatible libraries, and loading failures.

## 2. Provider migration

- [x] 2.1 Port shared scheduling promotion and priority-score helpers into the
  library and reuse them from claim and direct-dequeue functions.
- [x] 2.2 Port tracked-entry enqueue, claim, lease, release, recovery, delete,
  and lifecycle functions from EVALSHA to FCALL.
- [x] 2.3 Port raw-value and event-queue scripts, then remove obsolete script
  registration and cache handling.

## 3. Validation and deployment safety

- [x] 3.1 Add parity tests for every migrated provider operation, including
  concurrent promotion and failure/cleanup paths.
- [x] 3.2 Verify Redis Function persistence, explicit upgrade and rollback,
  API compatibility, key declaration, deployment/application ACL failure
  behaviour, and absence of an EVALSHA fallback.
- [x] 3.3 Run the focused Redis suite, full lint/format/type/test suite, and
  strict OpenSpec validation.
