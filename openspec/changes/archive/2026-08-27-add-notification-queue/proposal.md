## Why

Applications need owner-less notification: every Django process that sees a
payload at publish time may handle it, and none of them owns it. Event queues
are distributed processing: a worker that claims an event thereafter owns it
and other workers do not treat that event as theirs. Docs previously called
EventQueue “fan-out”; that is wrong. True multi-receiver seeing is a separate
semantic type, not a change to EventQueue.

## What Changes

- Add `NotificationQueue` as a third semantic facade beside `AsyncQueue` and
  `EventQueue`, with memory, Redis, and Redis Cluster backends.
- Each connected receiver processes every notification it **sees**. There is
  no claim, release, or consume-remove. Multiple receivers see the same
  message; none owns it.
- Document EventQueue as distributed event processing (claim then own), not
  fan-out. Do not turn EventQueue into multi-receiver seeing.
- Expire payloads by a lifetime the **sender** sets (`TIMEOUT` /
  `timeout_seconds`). Redis expiry **sets** a short-lived removal lease
  (`SET PX`) before delete; stored reads only GET that lease, then GET the
  entry if it is absent. Memory expiry is atomic under the provider lock and
  MAY omit a lease. Readers never `SET`. The lease is not an ownership claim
  and is not renewed as a processing budget.
- Reuse `@queue_listener` (filter, sync/async) on notification aliases.
  Every eligible local listener on a process that saw the message is invoked;
  listener return values do not consume the notification for other processes.
- Not a rewindable durable stream: a process that was not connected does not
  catch up.
- **Not breaking** for existing EventQueue / AsyncQueue behaviour.

## Capabilities

### New Capabilities

- `notification-queue`: Owner-less, best-effort notification delivery to
  processes that see the payload, sender-set lifetime, worker expiry (Redis
  removal-side lease; memory MAY omit), GET-only stored reads, and local
  listener dispatch without ownership claims.

### Modified Capabilities

- `queue-entries`: EventQueue SHALL NOT claim to deliver each event to every
  listener. EventQueue is distributed processing: claim then own.
  NotificationQueue is see-but-do-not-own.
- `provider-composition`: Compose a `NotificationQueue` facade and
  notification-aware workers; Redis notification delivery SHALL NOT use
  EventQueue claim/release/remove. A short-lived lease MAY be set only on
  worker expiry; stored reads SHALL be GET-only.
- `async-queue-backends`: Distinguish three semantic types under
  `BaseQueue` (`AsyncQueue`, `EventQueue`, `NotificationQueue`).
- `event-queue-listeners`: `@queue_listener` MAY register on a
  NotificationQueue; EventQueue dispatch (claim, rotation, consume on
  True/False) is unchanged.
- `completion-notifications`: `queue_observer` SHALL reject a
  NotificationQueue the same way it rejects an EventQueue.

## Impact

- New classes: `NotificationQueue`, `MemoryNotificationQueue`,
  `RedisNotificationQueue`, `RedisClusterNotificationQueue`, plus matching
  workers and Redis publish/subscribe (or equivalent) plumbing.
- `queue_listener` accepts notification aliases; EventQueue claim path stays.
- `QUEUES` gains notification backends; README choose-a-queue table gains a
  third row. Shared `QueueRuntime` hosts notification receivers.
- Redis Function library: add `django_queue_notification_store`,
  lease-free `django_queue_notification_get`,
  `django_queue_notification_expire`, and `django_queue_notification_clear`.
  Do not reuse `django_queue_expire`. `library_version` bump; `api_version`
  stays 1. Redeploy with `redis_lua_lib --deploy`.
