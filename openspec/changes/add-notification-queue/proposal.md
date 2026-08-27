## Why

Applications need owner-less broadcast: every Django process connected at
publish time should see the same short-lived fact, independently, with no
claim winner. Event queues are a claimed stream (one process consumes each
record). Docs previously called that “fan-out”; it is not. True fan-out is a
separate, EventQueue-like semantic type, not a change to EventQueue.

## What Changes

- Add `NotificationQueue` as a third semantic facade beside `AsyncQueue` and
  `EventQueue`, with memory, Redis, and Redis Cluster backends.
- Deliver each notification to every **currently connected** node. Delivery
  is best-effort: no Kafka-style consumer groups, no durable replay, no
  guarantee that a process that was down will see the event.
- Reuse `@queue_listener` (filter, sync/async) on notification aliases.
  Every eligible local listener on a connected node is invoked; listener
  return values do not consume the notification for other nodes.
- Expire payloads by TTL (queue `TIMEOUT` / entry `timeout_seconds`). No
  claim/lease on the happy path.
- Document EventQueue as claimed consume-or-pass. Do not turn EventQueue
  into broadcast.
- **Not breaking** for existing EventQueue / AsyncQueue behaviour.

## Capabilities

### New Capabilities

- `notification-queue`: Owner-less, best-effort notification delivery to
  connected nodes, TTL expiry, and local listener dispatch without claims.

### Modified Capabilities

- `queue-entries`: EventQueue SHALL NOT claim to deliver each event to every
  listener; that contract belongs to NotificationQueue. EventQueue remains a
  claimed stream.
- `provider-composition`: Compose a `NotificationQueue` facade and
  notification-aware workers; Redis notification delivery SHALL NOT use
  claim/lease/recovery.
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
- No new Python or Redis version requirements. Redis Function claims are not
  used for notification delivery.
