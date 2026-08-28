# Redis Secure Transport

## Purpose

Define encrypted Redis transport for standalone and Cluster queue backends,
including native TLS and uniform use of those settings on every connection the
package opens.

## Requirements

### Requirement: Select encrypted transport through the Redis URL scheme
A Redis queue backend SHALL treat a `rediss://` LOCATION as encrypted transport
and a `redis://` LOCATION as plaintext transport. Encrypted transport SHALL
apply to standalone backends and to Redis Cluster backends that use the
LOCATION as a Cluster seed. The backend SHALL reject TLS verification options
when the LOCATION scheme is `redis://`.

#### Scenario: Open a standalone queue over TLS
- **WHEN** a `QUEUES` alias selects a standalone Redis backend with a
  `rediss://` LOCATION and valid TLS verification options
- **THEN** queue operations complete over an encrypted connection to that Redis

#### Scenario: Open a Cluster queue over TLS
- **WHEN** a `QUEUES` alias selects a Redis Cluster backend with a `rediss://`
  database-`0` seed and valid TLS verification options
- **THEN** topology discovery and queue operations use encrypted connections to
  the discovered nodes

#### Scenario: Reject TLS options on a plaintext URL
- **WHEN** a Redis backend is configured with a `redis://` LOCATION and TLS
  verification options
- **THEN** configuration raises an actionable error identifying `rediss://` as
  the scheme required for encrypted transport

### Requirement: Verify TLS with backend options
A Redis queue backend SHALL accept TLS verification settings as backend
options, including a CA bundle and optional client certificate and key. When
LOCATION uses `rediss://`, the backend SHALL verify the server certificate
unless an option explicitly disables that verification. Hostname verification
SHALL remain enabled unless an option explicitly disables it.

#### Scenario: Verify the server with a CA bundle
- **WHEN** a Redis backend uses `rediss://` and a CA bundle option pointing at
  the issuer of the server certificate
- **THEN** the client accepts the connection and queue operations succeed

#### Scenario: Reject an untrusted server certificate
- **WHEN** a Redis backend uses `rediss://` and a CA bundle that did not issue
  the server certificate
- **THEN** the client fails to establish the connection with an error that
  identifies a TLS verification failure

### Requirement: Apply transport settings to every Redis connection
Every Redis client the package opens for a configured alias or Function-library
target SHALL use that alias's LOCATION scheme, credentials, and TLS
verification options. That includes queue providers, observers, Cluster
topology discovery, and per-primary Function-library connections. A Cluster
deploy connection SHALL NOT drop TLS or credentials that the seed URL and
options supplied.

#### Scenario: Deploy the Function library over TLS
- **WHEN** an operator runs the Function-library deploy command against a
  `rediss://` standalone or Cluster target with TLS verification options
- **THEN** every Redis connection the command opens uses those encrypted
  transport settings and the deploy completes

#### Scenario: Observe a queue over TLS
- **WHEN** a worker or listener observes a Redis queue whose LOCATION is
  `rediss://`
- **THEN** the observer connection uses the same encrypted transport settings
  as the queue's provider client

#### Scenario: Reject Cluster discovery that cannot use the configured TLS
- **WHEN** a Cluster seed uses `rediss://` but discovered node addresses cannot
  be dialed with those encrypted transport settings
- **THEN** topology use fails with an actionable error rather than silently
  opening plaintext connections to those nodes
