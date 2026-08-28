## Context

See `proposal.md` for motivation. `EventQueue` is distributed processing:
`RedisEventQueueWorker` claims, rotates `@queue_listener`s, and removes on
`True`/`False`. The claiming worker owns that event; other workers do not.
Async-queue `queue_observer` is already owner-less Pub/Sub of lifecycle
snapshots, but it is bound to `AsyncQueue` and is not a place to publish
application facts. Notification queues reuse listener registration and the
process-wide `QueueRuntime`. They have no claim/release/remove: a process
handles each notification it sees, and does not own it.

## Goals / Non-Goals

**Goals:**

- Third semantic facade with memory / Redis / Redis Cluster backends.
- Every connected receiver that sees a notification may process it; none owns
  it.
- Sender sets lifetime; the notification worker expires stored entries.
- Short-lived **removal** lease so stored readers can see expiry in progress;
  readers only GET.
- Same `@queue_listener` + `filter` surface; all eligible local listeners run.
- Replace EventQueue “fan-out” language with distributed processing
  (claim then own).

**Non-Goals:**

- Changing EventQueue into multi-receiver seeing.
- A durable, rewindable stream or catch-up for processes that were down.
- `queue_observer` on notification aliases.
- Durable terminal results, `list`/`prune`, `HANDLER` / `runqueues` work.
- EventQueue-style claim, release, or consume-remove on notifications.
- Renewing the removal lease as a processing budget, or taking it on read.

## Decisions

### Separate `NotificationQueue`, do not subclass EventQueue for delivery

Keep claim/consume on `EventQueue`. Notification workers never claim an
entry in order to process it, never release ownership, and never remove it
because a listener returned `True` or `False`. Shared pieces: `QueueEntry`
shape, JSON validation, `timeout_seconds` as sender-set lifetime,
`queue_listener` registry, `QueueRuntime` thread/loop.

**Alternative:** a flag on EventQueue. Rejected: claim-and-own vs
see-but-do-not-own cannot share the same worker without lying in the API.

### Redis: publish so every connected receiver sees the payload

Publish the payload (and id) on a channel derived from the queue name so
connected receivers dispatch without taking a claim. Store the same payload
so `afind` works until the sender-set lifetime elapses. Delivery MUST NOT
require winning a claim. A process without an active receiver at publish is
not required to catch up; this is not a rewindable stream.

**Alternative:** Redis Stream without consumer groups (`XREAD` per process).
Gives a short catch-up window and invites pretending the stream can be
rewound. Rejected as product behaviour; Pub/Sub (or equivalent) matches
connected-only seeing.

**Alternative:** reuse async-queue `aobserve`. Rejected: observers are
lifecycle snapshots of claimed work, not application notifications.

### Redis Function library: new notification entry points, not claim reuse

Seeing can use `PUBLISH` the same way lifecycle observers do. Mutations of
stored notification state — write payload and expiry index, set the removal
lease, delete when the sender lifetime has elapsed — are queue-state changes
and MUST be one server-side Function each. Stored **read** SHALL NOT mutate:
GET the lease key, then GET the entry if the lease is absent.

Existing Functions (`django_queue_claim`, `django_queue_expire`,
`django_queue_store_event_and_push`, and the rest) encode EventQueue pending
lists and ownership claims. Notification delivery MUST NOT call them.

Add notification-specific entry points. Do not extend or call
`django_queue_expire` (that Function is EventQueue claim/pending/unclaimed
indexes). **Approved 2026-08-27:** clean additions only; bump
`library_version`; keep `api_version` at 1.

- `django_queue_notification_store` — on enqueue only: `SET` payload,
  `ZADD` the entry ID onto the deadline ZSET (score = Redis `TIME` + sender
  lifetime), then `PUBLISH`. That `ZADD` is how the expiry list is
  populated. Mutation.
- `django_queue_notification_get` — lease-free fetch: `GET` lease, then
  `GET` entry only if the lease is absent. Returns the payload or nil.
  Read-only; no `SET`.
- `django_queue_notification_expire` — worker: look at the earliest
  deadline member; if its score is due, `SET` the removal lease (`PX`),
  delete the payload, and `ZREM` it from the index (pop-if-due). If not
  due, leave it. Mutation. Idempotent if another worker already popped it.
  One call expires at most one member.
- `django_queue_notification_clear` — administration: delete every indexed
  notification, its removal lease, and the deadline ZSET in one execution.
  Mutation.

Operators redeploy with `redis_lua_lib --deploy`.

**Alternative:** client `SET NX` plus `GET`/`DEL` without a Function.
Rejected: provider mutations must be one Function; expiry uses Redis `TIME`
inside that Function, matching EventQueue.

### Worker expires under a removal lease; readers only GET

The sender sets remaining lifetime (`timeout_seconds` or queue `TIMEOUT`).
The notification worker removes the stored entry when Redis `TIME` says that
lifetime has elapsed. Redis key TTL is not the protocol.

A single Redis `GET` versus `DEL` of one string is already atomic; that is
not the collision. The collision is a stored read overlapping expiry of the
record and its deadline index, or expiry using a client clock.

The lease is set **only on removal**, never on retrieval. A read-side `SET`
would write from every connected process, contend on one Cluster slot, and
mutex readers against each other — the opposite of see-but-do-not-own.

- Pub/Sub seeing does not touch the store. The published payload is already
  a copy.
- Stored read (`afind` / hydrate): GET the lease key; if it is present,
  treat the entry as gone or expiring; if it is absent, GET the entry.
  No `SET`. Both GETs SHOULD be one read-only Function so the snapshot is
  consistent on the slot.
- Expire Function: SET the lease (`PX` short), then if Redis `TIME` is past
  the sender deadline delete the entry and its expiry index in that same
  invocation. If the worker dies after SET, `PX` drops the lease so later
  reads and a later expire tick can proceed.

The lease MUST NOT confer ownership, MUST NOT be taken by readers, MUST NOT
be renewed as a handler budget, and MUST NOT call EventQueue claim Functions.

Memory expiry deletes the stored copy and its deadline in one locked
section; it MAY omit a removal lease because there is no overlapping GET
outside that lock.

**Alternative:** readers `SET NX` a lease, expire skips while it exists.
Rejected: every receiver becomes a writer; Cluster amplifies that; readers
block each other.

**Alternative:** Redis `SETEX` / key TTL as the sole expiry. Rejected: it
does not keep entry and deadline index in one Function, and it does not
publish a removal lease for readers to observe.

**Alternative:** hold any lease for the whole listener. Rejected: that is an
ownership claim by another name.

### Worker pops due IDs; enqueue populates the index

The expiry structure is a hash-tagged ZSET ordered by deadline, not the
EventQueue pending stack. New records enter it only from
`django_queue_notification_store` at enqueue: one Function writes the
payload, `ZADD`s the ID with score = Redis `TIME` + sender lifetime, then
`PUBLISH`es. There is no later “register for expiry” step.

The worker loop, on its tick, asks for the next due member (`ZRANGE` by
score, `LIMIT 0 1` against Redis `TIME`).
`django_queue_notification_expire` pops it only if that score is due: set
removal lease, delete payload, `ZREM` from the ZSET. If the earliest
member is still in the future, the Function leaves the index unchanged.

The notification runtime task is Pub/Sub-blocked like an observer, so that
tick MUST NOT wait for the next publish. The loop waits on Pub/Sub with a
short timeout. On message: dispatch from the published copy. On timeout or
after dispatch: try to pop-and-expire the next due member.

Every connected runtime MAY tick; expire is idempotent. The worker SHALL
NOT `SCAN` entry keys.

**Alternative:** expire only when a Pub/Sub message arrives. Rejected: an
idle alias would leak stored keys until the next publish.

**Alternative:** Redis key TTL as discovery. Rejected: no deadline index,
and readers cannot observe a removal lease.

### Local dispatch invokes every eligible listener

No rotation cursor, no consume-on-`True`. Filters still skip. Exceptions are
logged per listener; siblings still run. Return values are ignored for
cross-process lifetime; they do not remove the stored entry.

**Alternative:** one mandatory “terminator” listener. Rejected: that is
EventQueue ownership. Apps that want a handler list still wrap it in one
listener.

### Runtime

One notification receiver task per configured notification alias on the
existing process-wide runtime, analogous to Redis observer receivers: start
with Django, no `runqueues`. That task receives Pub/Sub and, on a periodic
tick, expires due stored entries from the deadline index. Memory: in-process
publish to local listeners only. Expire is atomic under the provider lock
and MAY omit a removal lease; stored reads GET the entry. A heap or
equivalent deadline index drives expiry.

## Risks / Trade-offs

- [A process that is down misses the event] → Document as the product
  contract; use EventQueue or AsyncQueue when work must run once later.
- [Pub/Sub payload size / lost subscriber] → Keep payloads small; the stored
  key is for `afind` until expiry, not catch-up.
- [Slow local listener vs lifetime] → Lifetime does not wait for handlers;
  handlers must tolerate the payload vanishing from `afind` after expiry.
- [Idle Pub/Sub never expires leftovers] → Pub/Sub wait has a timeout tick;
  expiry does not depend on the next publish.
- [Confusion with EventQueue docs] → README table is three rows; EventQueue
  is distributed processing (claim then own), never “fan-out”.

## Migration Plan

1. Add backends and workers; EventQueue behaviour unchanged.
2. Document the third queue type and fix EventQueue terminology; operators
   opt in via `QUEUES`.
3. Roll back by removing notification aliases; no EventQueue migration.

## Open Questions

None that block the spec. Removal-lease `PX` duration is an implementation
detail so long as it is short, not taken on read, and not renewed as a
processing budget.
