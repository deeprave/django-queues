## 1. Facades and memory backend

- [x] 1.1 Add `NotificationQueue` beside `EventQueue` on `BaseQueue` and export `MemoryNotificationQueue`. Verify it ignores `priority`/`available_at` like EventQueue, has no `list`/`prune`/`HANDLER`, and unit tests for enqueue + in-process seeing pass.
- [x] 1.2 Implement `MemoryNotificationQueueWorker` that invokes every eligible local listener (filters skip; exceptions logged; returns ignored for lifetime and do not consume-remove). Verify two-listener and filter-skip tests pass without consume-on-`True`.
- [x] 1.3 Expire memory payloads by sender-set `timeout_seconds` / queue `TIMEOUT` in the notification worker. Memory MAY omit a removal lease because expire is atomic under the provider lock. Redis SHALL `SET PX` then delete (see 2.4). Verify a test that waits past lifetime no longer finds the payload.

## 2. Redis and Cluster delivery

- [x] 2.1 Add `RedisNotificationQueue` that publishes so every connected receiver sees the payload, stores it for `afind` until worker expiry, and never uses EventQueue claim/release/remove. Verify a two-process (or two-receiver) test both dispatch and neither owns the payload.
- [x] 2.2 Add `RedisClusterNotificationQueue` with the same see-but-do-not-own contract. Verify Cluster seed/`db=0` rules match other Cluster backends and a receiver on the cluster alias dispatches.
- [x] 2.3 Document in code comments that a process without an active receiver at publish is not required to catch up and that this is not a rewindable stream. Verify a test that starts a receiver after enqueue does not assert delivery.
- [x] 2.4 Add `django_queue_notification_store`, lease-free `django_queue_notification_get` (GET lease then GET entry), and `django_queue_notification_expire` (SET lease then delete). Do not reuse `django_queue_expire`. Bump `library_version` only. Verify notification reads do not SET, existing claim/expire Functions are unchanged, and `redis_lua_compat` still reports `api_version` 1.

## 3. Listeners, observers, and runtime

- [x] 3.1 Allow `@queue_listener` on EventQueue **or** NotificationQueue; keep EventQueue claim/rotate/consume tests green. Verify AsyncQueue still raises; NotificationQueue registers; EventQueue consume-or-pass is unchanged.
- [x] 3.2 Reject `queue_observer` on NotificationQueue with the same AsyncQueue-only error path used for EventQueue. Verify the rejection test.
- [x] 3.3 Host one notification receiver per notification alias on `QueueRuntime` (start with Django when `QUEUES` is set; no `runqueues`). The loop waits on Pub/Sub with a timeout tick and expires due IDs from the deadline ZSET using Redis `TIME`. Verify idle expiry without a later publish, and EventQueue worker + AsyncQueue observer + NotificationQueue receiver on one loop.

## 4. Composition and docs

- [x] 4.1 Wire `QUEUES` backends, provider composition, and public exports (`NotificationQueue`, Redis/Cluster classes). Verify Django settings can resolve a notification alias and `openspec validate add-notification-queue --strict` still passes.
- [x] 4.2 Add a third README table row for NotificationQueue: see-but-do-not-own, `@queue_listener`, sender-set lifetime, worker expiry, not rewindable. Describe EventQueue as distributed processing (claim then own). Remove EventQueue “fan-out” language. Verify the choose-a-queue section names all three types.
