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
Django `QUEUES` configuration. It SHALL accept `--redis-url` as a discouraged
override for exceptional use; when supplied, that URL SHALL be the sole target.
For an explicitly configured Redis Cluster backend, deployment SHALL discover
the current Cluster primaries and install or replace the bundled library on
every primary before reporting success.

#### Scenario: Concurrent deployment requests
- **WHEN** two management-command invocations target the same Redis library
- **THEN** one invocation performs the replacement and the other rechecks the
  installed revision before deciding whether further work is needed

#### Scenario: Deploy to a Redis Cluster
- **WHEN** `redis_lua_lib --deploy` targets an explicitly configured Redis
  Cluster backend
- **THEN** the bundled Function library is installed or replaced on every
  current Cluster primary
