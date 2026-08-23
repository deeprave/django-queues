## Context

`QueueProviderRedis` currently registers a collection of EVALSHA scripts per
async Redis client and stores their handles in `_Scripts`. This works but makes
shared atomic behaviour expensive to maintain: each script has an isolated Lua
context, so scheduled promotion and index cleanup must be copied into every
claim and dequeue path.

## Goals / Non-Goals

**Goals:**

- Use one versioned Redis Function library with local reusable Lua helpers.
- Preserve existing provider operation names, queue-facing behaviour, atomicity,
  and Redis key contracts.
- Deploy the library through one explicit management command and validate its
  compatibility once per provider client/loop before the first FCALL.
- Make Redis 7 the supported minimum; Redis 8 is the deployment target.

**Non-Goals:**

- Change queue lifecycle, priority, scheduling, or raw-value semantics.
- Retain an EVALSHA fallback for pre-Redis-7 servers.
- Expose the function library as a public extension API.
- Let application providers install, upgrade, or downgrade the library.

## Decisions

### Ship one namespaced library with timestamp build metadata

The provider owns a stable library name such as `django_queues`, with stable,
registered entry points named `django_queue_<operation>`. Its source carries a `YYMMDD_HHMMSS`
`library_version` for inspection and deployment diagnostics. The
management command uses `FUNCTION LOAD REPLACE` against the stable library
name for either an upgrade or a deliberate rollback. Function names are global
across libraries, so timestamping library names would create collisions unless
every caller and function name also migrated.

Alternative: one library per queue. Rejected because libraries are server
deployment objects, while queue names are application data and can be dynamic.

The library source is a bundled package resource and is loaded through the
package-resource API, never by assuming an installed filesystem path.

### Keep one public operation within one FCALL

Python invokes one registered function for each provider mutation. Helpers are
ordinary local Lua functions within the library; Python must not compose
multiple FCALLs to form one provider operation. This preserves the current
atomic operation boundary.

### Deploy through a management command; validate in providers

One management command is the only code allowed to install or replace the
bundled library. It serialises deployment with a Redis lease, rechecks the
installed build after acquiring that lease, and can target either a newer or
an older bundled build. Application providers never load or replace a library.

Two commands separate the required credentials. `redis_lua_lib` runs
with a deployment identity, checks Redis/Valkey Function support and library
management permissions, and is the only command with an explicit `--deploy`
mode that loads or replaces the bundled library. `redis_lua_compat` runs
with an application identity and invokes only the library's stable
introspection entry point to verify FCALL availability. The latter deliberately
does not attempt to exhaustively probe every command or queue-key permission;
an actual provider operation reports a diagnostic naming the failed Function
operation and the Redis permission error.

Both commands derive Redis targets from Django's `QUEUES` configuration,
deduplicate equivalent URLs, and operate on every discovered target. They also
offer a discouraged `--redis-url` override for exceptional use; when supplied,
it is the sole target rather than an addition to configuration-derived targets.
The default avoids exposing credentials in shell history.

The library exposes one stable introspection entry point returning a
`library_version` and `api_version` compatibility field. `library_version`
changes for every bundled-library revision. Each application release
declares the minimum `api_version` it requires. Before its first FCALL, each
provider client/loop checks that the library exists and meets that minimum,
then caches the successful result locally. It fails clearly when the library
is absent, too old, or unavailable through the configured ACL.

The `api_version` is distinct from `library_version` and changes only for a
deliberate incompatible Function API change. Every normal release retains
established entry-point names and backward-compatible argument and return
behaviour, so old and new pods can share one library during blue/green
deployment.
Known queue indexes and records are passed to Functions explicitly. Claim and
promotion operations necessarily discover an entry ID while executing, then
derive that entry's record key atomically. To retain record-per-entry storage,
every Redis key owned by one queue uses a literal Cluster hash tag containing
the resolved queue alias, so discovered record keys and their supplied indexes
share one Cluster slot. Queue aliases are consequently a single key-schema
segment: non-empty ASCII letters, digits, `_`, or `-` only.

This deliberately accepts Redis Functions' stricter general guidance against
programmatically derived keys for those discovery operations; the tagged
queue namespace removes the concrete Cluster cross-slot risk without changing
the atomic record/index operation.

### Validate parity before removing scripts

Port one operation at a time behind provider-level tests, including failure and
concurrency cases. Remove script registration only after every current entry
point uses the function library.

## Risks / Trade-offs

- [Library deployment requires ACL changes] → Document FUNCTION LOAD for the
  management command and FCALL for applications; fail with actionable backend
  configuration errors.
- [A replacement is incompatible with live callers] → Keep the complete
  Function API surface backward compatible and validate it against a checked-in
  compatibility manifest before deployment.
- [Independent pods race to replace a shared library] → Providers never
  replace it; the management command serialises the explicit deployment.
- [Managed service blocks Functions or an application ACL blocks a nested Redis
  command] → Separate deployment and application diagnostics fail early for
  support/FCALL failures; provider runtime diagnostics report operation-specific
  command or key failures. No EVALSHA fallback is provided.
- [Function execution blocks Redis] → Retain short, bounded operations and
  existing key/argument discipline.

## Migration Plan

1. Add the function library, its deployment management command, and provider
   compatibility/invocation support.
2. Port and test all current scripts while preserving their public operations.
3. Deploy to Redis 7+ with the management command's FUNCTION LOAD permission
   and applications' FCALL permission.
4. Roll back by running the management command from the chosen library build;
   providers continue to validate only their minimum compatible API version.
