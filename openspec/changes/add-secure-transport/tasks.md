## 1. Connection settings

- [ ] 1.1 Add a shared helper that derives Redis client kwargs from LOCATION and TLS OPTIONS (scheme, credentials, SSL files, cert requirements, hostname check). Verify unit tests: `rediss://` yields TLS flags; `redis://` with TLS OPTIONS raises; OPTIONS override URL query SSL keys.
- [ ] 1.2 Apply the helper to standalone and Cluster provider clients, including observers. Verify a standalone `rediss://` queue constructs a TLS client and a Cluster seed does likewise without changing plaintext `redis://` behaviour.

## 2. Function-library commands

- [ ] 2.1 Thread LOCATION credentials and TLS OPTIONS through `redis_lua_lib` and `redis_lua_compat` seed clients, and through every Cluster per-primary client (host/port override only). Verify command tests capture those kwargs on a `rediss://` Cluster seed, including password and `ssl=True`.
- [ ] 2.2 Fail clearly when Cluster discovery yields node addresses that cannot be dialed with the configured encrypted transport. Verify a test where advertised plaintext ports plus a `rediss://` seed surfaces an actionable error rather than a plaintext fallback.

## 3. Live TLS coverage

- [ ] 3.1 Add a standalone Redis 8 TLS fixture (generated CA and server cert, `tls-port`) and verify enqueue, claim/recovery, and Function deploy/compat succeed over `rediss://` with `ssl_ca_certs`, and fail with an untrusted CA.
- [ ] 3.2 Extend the multi-primary Cluster fixture with native TLS (`tls-port`, `tls-cluster`) and verify a Cluster backend plus `redis_lua_lib --deploy` succeed over `rediss://` with the test CA, including a queue whose slot is not owned by the seed.
- [ ] 3.3 Add a standalone stunnel (or equivalent terminator) in front of plaintext Redis and verify a `rediss://` LOCATION to the terminator port succeeds with the test CA.

## 4. Documentation and validation

- [ ] 4.1 Document `rediss://`, TLS OPTIONS, stunnel as a terminator, and the Cluster announce rule (every advertised endpoint MUST be the TLS listener). Verify README examples cover standalone and Cluster.
- [ ] 4.2 Run the full test suite, Ruff, Ty, strict OpenSpec validation, and `git diff --check`; verify the new specs and implementation pass all checks.
