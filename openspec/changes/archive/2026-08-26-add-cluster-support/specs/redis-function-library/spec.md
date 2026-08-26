## MODIFIED Requirements

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
Django `QUEUES` configuration according to each alias's backend topology.
`--redis-url` SHALL be a discouraged standalone-only override: when supplied it
is the sole target and Cluster aliases SHALL NOT be visited. `--redis-cluster-url`
SHALL be a discouraged Cluster-only override: when supplied it is the sole seed
and standalone aliases SHALL NOT be visited. The two options SHALL be mutually
exclusive. Distinct Cluster seed URLs SHALL remain distinct targets.

For an explicitly configured Redis Cluster backend, deployment SHALL discover
the current Cluster primaries and install or replace the bundled library on
every primary before reporting success.

#### Scenario: Concurrent deployment requests
- **WHEN** two management-command invocations target the same Redis library
- **THEN** one invocation performs the replacement and the other fails
  immediately because a deployment is already in progress

#### Scenario: Standalone URL override ignores Cluster aliases
- **WHEN** `redis_lua_lib` or `redis_lua_compat` is given `--redis-url`
- **THEN** it uses that URL as a standalone Redis target and does not visit
  Cluster backends from `QUEUES`

#### Scenario: Cluster URL override ignores standalone aliases
- **WHEN** `redis_lua_lib` or `redis_lua_compat` is given `--redis-cluster-url`
- **THEN** it uses that URL as a Cluster seed and does not visit standalone
  Redis backends from `QUEUES`

#### Scenario: Deploy to a Redis Cluster
- **WHEN** `redis_lua_lib --deploy` targets an explicitly configured Redis
  Cluster backend
- **THEN** the bundled Function library is installed or replaced on every
  current Cluster primary

### Requirement: Route Function introspection with an optional queue key
The stable introspection entry point SHALL ignore keys and arguments. A
zero-key FCALL SHALL remain valid. Cluster callers SHALL pass one queue-owned
hash-tagged key so the FCALL executes on the primary that owns that queue's
slot. The registered Function description SHALL document that optional routing
key. Documenting or passing the key SHALL NOT increment `api_version`.

#### Scenario: Cluster provider compatibility check
- **WHEN** a Cluster provider validates the library before its first operation
  FCALL
- **THEN** it invokes `django_queue_info` with one key in the configured
  queue's hash tag and uses the returned `api_version`

#### Scenario: Cluster compatibility command checks every primary
- **WHEN** `redis_lua_compat` targets a Redis Cluster seed
- **THEN** it invokes `django_queue_info` on every current primary without
  requiring `FUNCTION LIST`
