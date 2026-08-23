## Why

Applications need to enqueue durable work that must not be dispatched before a
specified instant. Implementing this in each consumer would either hold worker
claims while waiting or create a race between normal enqueue and scheduling.

## What Changes

- Add an optional queue-facing `available_at` instant to identified
  `AsyncQueue.enqueue()` and `AsyncQueue.aenqueue()` operations.
- Make Redis-backed identified queues atomically store future entries in a
  scheduled sorted-set index rather than their normal pending index.
- Promote due scheduled entries into the ordinary pending or priority index
  during the Redis claim operation, then use the existing claim lifecycle.
- Count scheduled entries as pending work so configured workers remain active
  for queues that contain only future work.
- Ensure deletion and queued-to-terminal cleanup remove scheduled membership
  atomically. Non-AsyncQueue variants accept and ignore `available_at` where
  delayed dispatch does not apply.

## Capabilities

### New Capabilities

- `delayed-entry-dispatch`: durable, Redis-backed eligibility scheduling for
  identified AsyncQueue entries.

### Modified Capabilities

- `async-queue-backends`: identified AsyncQueue enqueue operations accept an
  optional availability instant and report scheduled work as pending.

## Impact

- Affects `AsyncQueue` enqueue APIs, Redis and memory backend providers,
  Redis Lua claim/enqueue/cleanup scripts, worker readiness checks, and queue
  documentation.
- Adds a Redis scheduled-entry ZSET distinct from the existing delayed-release
  ZSET. No new dependency or QueueEntry durable field is introduced.
