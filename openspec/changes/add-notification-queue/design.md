## Context

See `proposal.md` for motivation. `EventQueue` is a claimed stream:
`RedisEventQueueWorker` claims, rotates `@queue_listener`s, and removes on
`True`/`False`. Async-queue `queue_observer` is already owner-less Pub/Sub of
lifecycle snapshots, but it is bound to `AsyncQueue` and is not a place to
publish application facts. Notification queues reuse listener registration
and the process-wide `QueueRuntime`, not EventQueue claims.

## Goals / Non-Goals

**Goals:**

- Third semantic facade with memory / Redis / Redis Cluster backends.
- Best-effort broadcast to processes that have an active receiver at publish.
- TTL expiry without a delivery claim.
- Same `@queue_listener` + `filter` surface; all eligible local listeners run.

**Non-Goals:**

- Changing EventQueue into fan-out.
- Kafka/consumer-group replay or “you will get events from while you were down.”
- `queue_observer` on notification aliases.
- Durable terminal results, `list`/`prune`, `HANDLER` / `runqueues` work.
- Cross-process “last listener deletes.”

## Decisions

### Separate `NotificationQueue`, do not subclass EventQueue for delivery

Keep claim/consume on `EventQueue`. Notification workers never call claim,
renew, release, or recover. Shared pieces: `QueueEntry` shape, JSON
validation, `timeout_seconds` as lifetime, `queue_listener` registry,
`QueueRuntime` thread/loop.

**Alternative:** a flag on EventQueue. Rejected: claim vs broadcast cannot
share the same worker without lying in the API.

### Redis: Pub/Sub (or Cluster equivalent) plus optional TTL key

Publish the payload (and id) on a channel derived from the queue name so
connected receivers dispatch without a GET. Optionally `SETEX` the same
payload so `afind` works until TTL; delivery MUST NOT require that GET.
Missed Pub/Sub is accepted.

**Alternative:** Redis Stream without consumer groups (`XREAD` per process).
Gives a short catch-up window at the cost of per-process last-id and more
machinery. Defer unless Pub/Sub proves insufficient.

**Alternative:** reuse async-queue `aobserve`. Rejected: observers are
lifecycle snapshots of claimed work, not application notifications.

### Local dispatch invokes every eligible listener

No rotation cursor, no consume-on-`True`. Filters still skip. Exceptions are
logged per listener; siblings still run. Return values are ignored for
cross-process lifetime.

**Alternative:** one mandatory “terminator” listener. Rejected: that is
EventQueue ownership. Apps that want a handler list still wrap it in one
listener.

### Runtime

One notification receiver task per configured notification alias on the
existing process-wide runtime, analogous to Redis observer receivers: start
with Django, no `runqueues`. Memory: in-process publish to local listeners
only.

## Risks / Trade-offs

- [A process that is down misses the event] → Document as the product
  contract; use EventQueue or AsyncQueue when work must run once later.
- [Pub/Sub payload size / lost subscriber] → Keep payloads small; optional
  TTL key does not fix a missed publish.
- [Slow local listener vs TTL] → TTL does not wait for handlers; handlers
  must tolerate the payload vanishing from `afind` while they run.
- [Confusion with EventQueue docs] → README table is three rows; EventQueue
  text already states claimed stream.

## Migration Plan

1. Add backends and workers; EventQueue behaviour unchanged.
2. Document the third queue type; operators opt in via `QUEUES`.
3. Roll back by removing notification aliases; no EventQueue migration.

## Open Questions

None that block the spec. Pub/Sub vs a later optional Stream catch-up window
can be chosen at implementation without changing connected-only delivery.
