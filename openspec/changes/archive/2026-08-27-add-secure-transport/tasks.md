## 1. Connection settings

- [x] 1.1 Add a shared helper that derives Redis client kwargs from LOCATION and TLS OPTIONS (scheme, credentials, SSL files, cert requirements, hostname check). Verify unit tests: `rediss://` yields TLS flags; `redis://` with TLS OPTIONS raises; OPTIONS override URL query SSL keys.
- [x] 1.2 Apply the helper to standalone and Cluster provider clients, including observers. Verify a standalone `rediss://` queue constructs a TLS client and a Cluster seed does likewise without changing plaintext `redis://` behaviour.

## 2. Function-library commands

- [x] 2.1 Thread LOCATION credentials and TLS OPTIONS through `redis_lua_lib` and `redis_lua_compat` seed clients, and through every Cluster per-primary client (host/port override only). Verify command tests capture those kwargs on a `rediss://` Cluster seed, including password and `ssl=True`.
- [x] 2.2 Fail clearly when Cluster discovery yields node addresses that cannot be dialed with the configured encrypted transport. Verify a test where advertised plaintext ports plus a `rediss://` seed surfaces an actionable error rather than a plaintext fallback.

## 3. Live TLS coverage

- [x] 3.1 Add a standalone Redis 8 TLS fixture (generated CA and server cert, `tls-port`) and verify enqueue, claim/recovery, and Function deploy/compat succeed over `rediss://` with `ssl_ca_certs`, and fail with an untrusted CA.
- [x] 3.2 Extend the multi-primary Cluster fixture with native TLS (`tls-port` on client ports) and verify a Cluster backend plus `redis_lua_lib --deploy` succeed over `rediss://` with the test CA, including a queue whose slot is not owned by the seed.

## 4. Documentation and validation

- [x] 4.1 Document `rediss://`, TLS OPTIONS, and that every advertised Cluster endpoint MUST be reachable with those TLS settings. Verify README examples cover standalone and Cluster.
- [x] 4.2 Run the full test suite, Ruff, Ty, strict OpenSpec validation, and `git diff --check`; verify the new specs and implementation pass all checks.
