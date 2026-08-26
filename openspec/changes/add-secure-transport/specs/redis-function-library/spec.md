## ADDED Requirements

### Requirement: Honour encrypted transport for Function-library connections
`redis_lua_lib` and `redis_lua_compat` SHALL open Redis connections using the
same LOCATION scheme, credentials, and TLS verification options as the
configured queue target they are acting on. For a Cluster target they SHALL
apply those settings to the seed client and to every per-primary connection.
They SHALL NOT install or replace a library over a weaker transport than the
target is configured to use.

#### Scenario: Compatibility check against a TLS Cluster seed
- **WHEN** `redis_lua_compat` runs against a Cluster alias whose LOCATION is
  `rediss://` with TLS verification options
- **THEN** it opens encrypted connections with those options and reports
  compatibility without falling back to plaintext

#### Scenario: Deploy to TLS-protected primaries
- **WHEN** `redis_lua_lib --deploy` runs against a Cluster alias whose LOCATION
  is `rediss://` with credentials and TLS verification options
- **THEN** each primary connection uses those credentials and TLS settings and
  the command does not attempt an unauthenticated or plaintext primary
  connection
