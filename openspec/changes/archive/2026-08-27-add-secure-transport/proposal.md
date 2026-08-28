## Why

Production Redis is routinely reached over TLS (`tls-port`). Django Queue
currently documents and tests only `redis://`. Application clients inherit TLS
only when redis-py parses `rediss://`; Cluster Function deployment opens
per-primary connections that can drop those settings. Operators cannot treat
encrypted transport as a supported contract for standalone or Cluster.

## What Changes

- Treat `rediss://` as the supported LOCATION scheme for encrypted Redis
  transport on standalone and Cluster backends.
- Accept Redis TLS verification settings as backend OPTIONS and apply them to
  every client the package opens: queue providers, observers, and
  Function-library management commands, including Cluster discovery and
  per-primary deploy connections.
- Cover native Redis TLS for standalone and Cluster in live tests.

## Capabilities

### New Capabilities

- `redis-secure-transport`: Encrypted Redis transport for standalone and
  Cluster backends, including `rediss://`, TLS OPTIONS, and uniform application
  of those settings to every connection the package opens.

### Modified Capabilities

- `redis-function-library`: Function-library deploy and compatibility commands
  SHALL use the same encrypted-transport settings as the configured queue
  LOCATION when opening Redis or Cluster connections.

## Impact

- Redis provider client construction, Cluster seed and per-primary clients,
  `redis_lua_lib` / `redis_lua_compat` target connections, README LOCATION and
  OPTIONS documentation, and TLS-enabled Redis integration fixtures.
- No new runtime dependency: redis-py already implements TLS.
- Cluster TLS live tests use the explicit Cluster backend family.
