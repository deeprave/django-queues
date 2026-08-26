## Context

See proposal.md for motivation. Standalone and Cluster providers already
construct clients with redis-py `from_url(LOCATION)`. That parses `rediss://`
into SSL flags for the seed client, but the package does not document TLS,
does not accept CA or client-certificate OPTIONS, and Cluster Function
deployment currently constructs per-primary `Redis(host, port)` clients that
drop URL credentials and SSL. Stunnel is an operator process, not a library.

Cluster backends and their live fixture are introduced by `add-cluster-support`.
This change applies encrypted transport to both topologies; Cluster TLS live
tests require that backend family.

## Goals / Non-Goals

**Goals:**

- Make `rediss://` plus TLS OPTIONS a supported contract for every Redis TCP
  connection the package opens.
- Treat stunnel and other TLS terminators as the same client contract when
  advertised endpoints are the TLS listeners.
- Prove native TLS on standalone and Cluster, and stunnel in front of
  standalone Redis, in live tests.

**Non-Goals:**

- Spawning, configuring, or bundling stunnel (or any terminator).
- A three-node Cluster-plus-stunnel live fixture; Cluster native TLS plus
  standalone stunnel cover the client and terminator contracts.
- Auto-detecting TLS from a `redis://` URL, Sentinel, or mixed plaintext/TLS
  node sets.
- Changing Function `api_version` or queue semantics.

## Decisions

### Use the URL scheme as the TLS switch

`rediss://` means every subsequent Redis connection for that target is TLS.
`redis://` means plaintext. TLS OPTIONS on a `redis://` LOCATION are a
configuration error, not a silent upgrade. URL auto-upgrade was rejected
because a mistyped scheme would encrypt unexpectedly or, worse, a `rediss`
typo to `redis` would look like success on an open port.

redis-py already maps `rediss://` to `ssl=True`. The package will pass that
through rather than implementing a TLS stack.

### Pass TLS files as backend OPTIONS

LOCATION stays a URL. CA bundle, client certificate, client key,
`ssl_cert_reqs`, and `ssl_check_hostname` are backend OPTIONS using redis-py
connection keyword names. They are merged into every client constructor.
When both a URL query parameter and an OPTION set the same SSL key, OPTIONS
win so Django settings remain the operator-facing place for filesystem paths.

Username and password remain URL userinfo (and any redis-py query equivalents).
TLS and AUTH are independent.

Default verification follows redis-py for `rediss://` (certificate required,
hostname checked). Tests supply a generated CA via `ssl_ca_certs` rather than
disabling verification.

### One connection-settings helper for every client

A shared helper derives constructor kwargs from LOCATION plus OPTIONS
(credentials, SSL, and Cluster `address_remap` when present). Standalone
`from_url`, Cluster `from_url`, observer clients, management-command seed
clients, and per-primary `Redis()` clients all use it. Per-primary clients
override only host and port after remap; they MUST copy the rest of the
cluster manager's connection kwargs.

This is the product fix for Cluster deploy dropping AUTH/TLS, not a
kwargs-only unit test bolted onto Cluster support.

### Stunnel is a terminator, not a feature flag

The package never starts stunnel. Operators run stunnel (or an equivalent)
so the address in LOCATION is a TLS listener. The client uses `rediss://`
exactly as for native Redis `tls-port`.

For Cluster, `CLUSTER SLOTS` / announce MUST return those TLS listener
addresses. Wrapping only the seed leaves discovered plaintext `host:port`
values; the client must fail rather than open plaintext node connections.
Native Redis Cluster TLS (`tls-port` and `tls-cluster`) is the live Cluster
proof; stunnel-on-Cluster is documented with that announce rule.

### Live fixtures

- Standalone native TLS: Redis 8 with `tls-port`, generated test CA and server
  cert, plaintext port disabled or unused.
- Cluster native TLS: the three-process Redis 8 Cluster image with TLS on each
  client port and `tls-cluster yes`, plus existing `address_remap` for
  published ports.
- Standalone stunnel: plaintext Redis plus a stunnel listener; LOCATION is
  `rediss://` to the stunnel port with the test CA.

Do not add a stunnel Debian package to the Redis Cluster image.

### Alternatives considered

- **URL query-only SSL paths.** Works with redis-py but is a poor Django
  settings shape for certificate files. OPTIONS are the documented path;
  query params still parse if present.
- **In-process TLS terminator.** Out of scope; stunnel stays an external
  process.
- **CERT_NONE by default in tests and docs.** Convenient and unsafe. Tests
  generate a CA.

## Risks / Trade-offs

- [Cluster announce mismatch] → Fail closed when a discovered node cannot be
  dialed with the configured TLS settings; document announce/stunnel port
  alignment.
- [Fixture complexity] → Reuse the Cluster image pattern; add TLS via config
  and certs mounted or generated at entrypoint, rather than a second Cluster
  topology.
- [Self-signed production certs] → Operators must set `ssl_ca_certs`; do not
  document disabling verification as the normal path.
- [Depends on Cluster backends] → Implement standalone TLS first if Cluster
  is not yet merged; Cluster TLS tests wait on that family.

## Migration Plan

Existing `redis://` aliases are unchanged. Operators move LOCATION to
`rediss://`, add CA OPTIONS, and point Cluster announce or stunnel at TLS
listeners. Rollback is reverting LOCATION to `redis://` and removing TLS
OPTIONS. Function library contents are unaffected.

## Open Questions

None. Client certificates are optional OPTIONS (`ssl_certfile` / `ssl_keyfile`)
and do not change the requirements.
