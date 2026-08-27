## 1. Facades and memory backend

- [ ] 1.1 Add `NotificationQueue` beside `EventQueue` on `BaseQueue` and export `MemoryNotificationQueue`. Verify it ignores `priority`/`available_at` like EventQueue, has no `list`/`prune`/`HANDLER`, and unit tests for enqueue + in-process delivery pass.
- [ ] 1.2 Implement `MemoryNotificationQueueWorker` that invokes every eligible local listener (filters skip; exceptions logged; returns ignored for lifetime). Verify two-listener and filter-skip tests pass without consume-on-`True`.
- [ ] 1.3 Expire memory payloads by `timeout_seconds` / queue `TIMEOUT` without a claim. Verify a test that waits past TTL no longer finds the payload.

## 2. Redis and Cluster delivery

- [ ] 2.1 Add `RedisNotificationQueue` that publishes the payload to connected receivers (Pub/Sub or equivalent) and optionally stores it with TTL for `afind`. Verify delivery does not call claim/lease/recover Redis Functions and a two-process (or two-receiver) test both dispatch.
- [ ] 2.2 Add `RedisClusterNotificationQueue` with the same connected-only contract. Verify Cluster seed/`db=0` rules match other Cluster backends and a receiver on the cluster alias dispatches.
- [ ] 2.3 Document in code comments that a process without an active receiver at publish is not required to catch up. Verify a test that starts a receiver after enqueue does not assert delivery.

## 3. Listeners, observers, and runtime

- [ ] 3.1 Allow `@queue_listener` on EventQueue **or** NotificationQueue; keep EventQueue claim/rotate/consume tests green. Verify AsyncQueue still raises; NotificationQueue registers; EventQueue consume-or-pass is unchanged.
- [ ] 3.2 Reject `queue_observer` on NotificationQueue with the same AsyncQueue-only error path used for EventQueue. Verify the rejection test.
- [ ] 3.3 Host one notification receiver per notification alias on `QueueRuntime` (start with Django when `QUEUES` is set; no `runqueues`). Verify runtime tests cover EventQueue worker + AsyncQueue observer + NotificationQueue receiver on one loop.

## 4. Composition and docs

- [ ] 4.1 Wire `QUEUES` backends, provider composition, and public exports (`NotificationQueue`, Redis/Cluster classes). Verify Django settings can resolve a notification alias and `openspec validate add-notification-queue --strict` still passes.
- [ ] 4.2 Add a third README table row for NotificationQueue: connected-only broadcast, `@queue_listener`, TTL, no durable result. Keep EventQueue as claimed stream (no fan-out). Verify the choose-a-queue section names all three types.
