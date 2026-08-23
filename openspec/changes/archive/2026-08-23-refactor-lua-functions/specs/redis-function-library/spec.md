## Purpose

Define the deployment and invocation contract for Redis-backed queue server
operations implemented through a durable Redis Function library.

## ADDED Requirements

### Requirement: Deploy the Redis Function library explicitly
The Redis queue backend SHALL provide `redis_lua_lib` as the sole owner
of installing or replacing its Function library. It SHALL run with a deployment
credential, check Redis/Valkey Function support and library-management
permissions, and provide `--deploy` as the only explicit mode that installs or
replaces the bundled library. That mode SHALL require Redis 7 or later, serialise replacement
so concurrent invocations do not duplicate an update, and support a deliberate
upgrade or rollback to the bundled library revision.

The bundled Function-library source SHALL be loaded through a package-resource
API. By default, the command SHALL derive and deduplicate Redis targets from
Django `QUEUES` configuration. It SHALL accept `--redis-url` as a discouraged
override for exceptional use; when supplied, that URL SHALL be the sole target.

#### Scenario: Concurrent deployment requests
- **WHEN** two management-command invocations target the same Redis library
- **THEN** one invocation performs the replacement and the other rechecks the
  installed revision before deciding whether further work is needed

### Requirement: Validate application compatibility without deployment
Each application release SHALL declare a minimum compatible `api_version`.
Before its first FCALL for a provider client/loop, the Redis backend SHALL
confirm that the installed library exists and reports an `api_version` at
least that minimum, then cache the successful result for that client/loop. It
SHALL NOT install or replace a library from an application provider.

#### Scenario: Redis Functions are unavailable
- **WHEN** a Redis queue needs its Function library and the library is absent,
  incompatible, or unavailable because of server version or application ACL
  configuration
- **THEN** the backend raises an error identifying the prerequisite and the
  deployment command

### Requirement: Diagnose application-role compatibility separately
The Redis queue backend SHALL provide `redis_lua_compat` for use with an
application credential. It SHALL invoke the library's stable introspection
entry point and fail clearly when FCALL or that entry point is unavailable. It
SHALL NOT install or replace the library, and it SHALL NOT exhaustively probe
each nested Redis command or queue-key permission.

#### Scenario: Application credential cannot invoke Functions
- **WHEN** `redis_lua_compat` runs with a credential denied FCALL
- **THEN** it fails with an actionable application-permission diagnostic

#### Scenario: A nested operation lacks permission at runtime
- **WHEN** an invoked Function is denied an underlying Redis command or queue
  key permission
- **THEN** the provider reports a diagnostic naming the provider operation and
  Redis permission failure

### Requirement: Preserve Function API compatibility across normal releases
The Function library SHALL expose stable registered entry points and a stable
introspection entry point reporting `library_version` and `api_version`.
`library_version` SHALL change for every bundled-library revision. Normal
releases SHALL retain existing entry-point names and backward-compatible
argument and return behaviour. The `api_version` SHALL increase only for a deliberate
incompatible change.

#### Scenario: Blue-green application deployment
- **WHEN** old and new compatible application releases use the same Redis
  library
- **THEN** both can invoke their established Function entry points

### Requirement: Do not fall back to EVALSHA
When a Redis queue requires the Function library, the backend SHALL NOT fall
back to EVALSHA scripts if Function support or permission is unavailable.

#### Scenario: Functions are unavailable
- **WHEN** the Function library cannot be used because of server capability or
  permission
- **THEN** the backend reports the configuration error without invoking an
  EVALSHA fallback

### Requirement: Preserve atomic provider operations
Each Redis provider operation that changes queue state SHALL execute as one
atomic server-side function invocation. Shared server-side logic SHALL not
require the client to compose multiple invocations.

#### Scenario: Invoke a multi-index mutation
- **WHEN** a queue operation changes an entry record and one or more queue
  indexes
- **THEN** observers cannot see a partially applied mutation

### Requirement: Co-locate queue-owned Redis keys
The Redis backend SHALL construct every queue-owned Redis key with a literal
Cluster hash tag containing the resolved queue alias. Queue aliases SHALL be
non-empty ASCII strings containing only letters, digits, `_`, or `-`.

#### Scenario: A Function discovers an entry record from a queue index
- **WHEN** a claim or promotion Function removes an entry ID from a queue index
- **THEN** the record key it derives for that ID maps to the same Redis Cluster
  slot as the queue index
