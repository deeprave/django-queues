## Context

See proposal.md for motivation. Standalone and Cluster providers already
construct clients with redis-py `from_url(LOCATION)`. That parses `rediss://`
into SSL flags for the seed client, but the package does not document TLS,
does not accept CA or client-certificate OPTIONS, and Cluster Function
deployment currently constructs per-primary `Redis(host, port)` clients that
can drop URL credentials and SSL.

Cluster backends and their live fixture are already on main. This change
applies encrypted transport to both topologies.

## Goals / Non-Goals

**Goals:**

- Make `rediss://` plus TLS OPTIONS a supported contract for every Redis TCP
  connection the package opens.
- Prove native Redis TLS on standalone and Cluster in live tests.

**Non-Goals:**

- Explicit support, fixtures, or documentation for stunnel or other TLS
  terminators. A terminator that presents a TLS Redis listener is reachable
  with the same `rediss://` client as native `tls-port`; the package does not
  treat that as a separate deployment.
- Spawning, configuring, or bundling any TLS terminator.
- Auto-detecting TLS from a `redis://` URL, Sentinel, or mixed plaintext/TLS
  node sets.
- Changing Function `api_version` or queue semantics.

## Decisions

### Use the URL scheme as the TLS switch

`rediss://` means every subsequent Redis connection for that target is TLS.
`redis://` means plaintext. TLS OPTIONS on a `redis://` LOCATION are a
configuration error, not a silent upgrade. OPTIONS may override URL query SSL
keys on a `rediss://` LOCATION; they do not change the scheme.

redis-py already maps `rediss://` to `ssl=True`. The package will pass that
through rather than implementing a TLS stack.

### Pass TLS files as backend OPTIONS

LOCATION stays a URL. CA bundle, client certificate, client key,
`ssl_cert_reqs`, and `ssl_check_hostname` are backend OPTIONS using redis-py
connection keyword names. They are merged into every client constructor.
When both a URL query parameter and an OPTION set the same SSL key, OPTIONS
win so Django settings remain the operator-facing place for filesystem paths.
TLS query keys are copied into constructor kwargs and stripped from the URL
passed to `from_url`, because redis-py otherwise lets the query string win.

Username and password remain URL userinfo (and any redis-py query equivalents).
TLS and AUTH are independent.

Default verification follows redis-py for `rediss://` (certificate required,
hostname checked). redis-py builds the SSL context with
`ssl.create_default_context()`, so a server cert issued by a CA already in
the system trust store needs no `ssl_ca_certs`. A private or generated CA
is passed as `ssl_ca_certs` (or `ssl_ca_path` / `ssl_ca_data`). Disabling
verification (`ssl_cert_reqs` none) remains an explicit OPTION, not the
default. Tests use a generated CA via `ssl_ca_certs` rather than disabling
verification or installing that CA into the system store.

### One connection-settings helper for every client

A shared helper derives TLS constructor kwargs from LOCATION plus OPTIONS
(scheme and SSL files, cert requirements, hostname check). Standalone
`from_url`, Cluster `from_url`, observer clients, management-command seed
clients, and per-primary `Redis()` clients all use it. Cluster
`address_remap` remains a Cluster-only overlay applied by the Cluster
provider and Function-library Cluster constructors, not by the TLS helper.
Per-primary clients override only host and port after remap; they MUST copy
the rest of the cluster manager's connection kwargs.

### Live fixtures

Live TLS tests generate a short-lived test CA and a server certificate at
fixture or image-entrypoint time. Those PEMs are not committed. Redis is
configured with `tls-cert-file` / `tls-key-file` / `tls-ca-cert-file`; pytest
clients pass the CA path as `ssl_ca_certs`. A second generated CA that did
not issue the server cert covers the untrusted-certificate scenario. Client
certificates are not part of the live fixtures. Existing plaintext Redis and
Cluster fixtures stay as they are.

- Standalone native TLS: Redis 8 with `tls-port`, generated CA and server
  cert, plaintext port disabled or unused.
- Cluster native TLS: the three-process Redis 8 Cluster image with the same
  CA on every node and TLS on each client `tls-port`. The single-container
  fixture keeps the cluster bus in plaintext (`tls-cluster no`) so `CLUSTER
  MEET` can join; clients still use `rediss://`. Existing `address_remap`
  covers published ports. Hostname verification uses the host string pytest
  dials (`get_container_host_ip()`, remapped for Cluster). The generated
  server cert SAN includes the likely testcontainers identities:
  DNS `localhost` and `host.docker.internal`, IP `127.0.0.1`, `::1`, and
  `172.17.0.1`. Python treats `localhost` as a DNS name and `127.0.0.1` as
  an IP, so both are required. Success criterion is local Docker Desktop and
  GitHub `ubuntu-latest` CI. Exotic hosts (`TESTCONTAINERS_HOST_OVERRIDE`,
  non-default bridge IPs) are out of scope for the fixture rather than a
  reason to disable `ssl_check_hostname`.

### Alternatives considered

- **URL query-only SSL paths.** Works with redis-py but is a poor Django
  settings shape for certificate files. OPTIONS are the documented path;
  query params still parse if present.
- **First-class stunnel support.** Unnecessary once Redis native TLS exists.
  Any TLS listener, including a terminator, already works as `rediss://`.
- **CERT_NONE by default in tests and docs.** Convenient and unsafe. Tests
  generate a CA.

## Risks / Trade-offs

- [Cluster announce mismatch] → Fail closed when a discovered node cannot be
  dialed with the configured TLS settings.
- [Fixture complexity] → Reuse the Cluster image pattern; add TLS via config
  and certs mounted or generated at entrypoint, rather than a second Cluster
  topology.
- [Self-signed production certs] → Operators must set `ssl_ca_certs`; do not
  document disabling verification as the normal path.

## Migration Plan

Existing `redis://` aliases are unchanged. Operators move LOCATION to
`rediss://` and add CA OPTIONS. Rollback is reverting LOCATION to `redis://`
and removing TLS OPTIONS. Function library contents are unaffected.

## Open Questions

None. Client certificates are optional OPTIONS (`ssl_certfile` / `ssl_keyfile`)
and do not change the requirements.
