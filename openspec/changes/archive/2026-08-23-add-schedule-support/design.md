## Context

Redis identified queues already have a durable entry record, normal pending
indexes, a delayed-release ZSET for previously claimed work, priority-aware
claim scripts, and a Redis-aligned queue clock. See `proposal.md` for the
motivation and the delta specifications for observable behaviour.

## Goals / Non-Goals

**Goals:**

- Atomically persist and schedule future identified AsyncQueue entries.
- Make due work enter the ordinary FIFO or priority dispatch path without a
  worker claiming it early.
- Preserve existing reliable-delivery, recovery, release, and lifecycle rules.
- Let `django-redis-tasks` translate its Django-facing `run_after` value into
  the queue-facing availability instant without owning scheduling mechanics.

**Non-Goals:**

- Add an `available_at` field to `QueueEntry` or expose it in the durable entry
  record.
- Change EventQueue delivery semantics, add an in-process timer service, or
  guarantee execution at an exact instant.
- Merge first-availability scheduling with the delayed-release mechanism used
  after a claim is relinquished.

## Decisions

### Expose an absolute `available_at` enqueue argument

`aenqueue()` and `enqueue()` receive `available_at: ClockTime | None`. An
absolute instant maps directly to Django's `run_after`, survives caller retries
without recalculating an elapsed duration, and matches Redis sorted-set scores.
The generic API accepts it on queue variants where it is inapplicable, as with
`priority`, preserving call-site compatibility.

Alternative: accept a duration. Rejected because a duration needs a clock at
enqueue time, makes retries time-relative, and cannot losslessly represent an
upstream absolute run-after value.

### Maintain a dedicated scheduled ZSET

Redis stores future entry IDs in `{queue}:entries:scheduled`, scored by UTC
epoch microseconds. Its logical selection order is `available_at`, then higher
priority, then arrival: a claim releases only the first due availability group,
selecting its highest-priority entry. Immediate entries use the existing pending
list or priority ZSET. The scheduled ZSET does not reuse
`{queue}:entries:delayed`: that index means a previously claimed entry has been
released and carries recovery/retry semantics, whereas scheduled membership
means the entry has never been dispatchable.

Alternative: one key per entry. Rejected because finding due work would require
key scans and cannot preserve atomic batch promotion.

### Use atomic Lua scripts for enqueue, promotion, and cleanup

The Redis provider's Lua scripts own the scheduling mutations for enqueue,
promotion, claim, and cleanup. The delayed-enqueue script writes the entry
record and either scheduled or ordinary pending membership in one transaction.
Its claim and direct-dequeue scripts read Redis `TIME`, atomically release at
most one due scheduled entry per attempt, and use priority only to select within
that entry's availability group. Delete and queued-terminal cleanup scripts
remove scheduled membership alongside all existing indexes.

This eliminates the race where a worker claims an entry after it is stored but
before a separate scheduling write parks it.

Reusable Redis Functions are deferred to the separate `refactor-lua-functions`
change. That refactor will consolidate the shared Lua behaviour without
changing this scheduling contract.

Alternative: have a worker timer promote entries. Rejected because crashes,
multiple workers, and polling races would make promotion non-durable and
non-atomic.

### Count scheduled work as pending

`ahas_pending()` includes the scheduled ZSET. `runqueues` therefore starts and
keeps the configured worker service for a queue with only future work; workers
may poll normally until due entries are promoted.

### Give memory AsyncQueue matching semantics where practical

The memory provider can retain scheduled IDs against the existing local queue
clock and promote them before its ordinary selection. Event queues accept but
ignore `available_at`, matching their separate delivery model.

## Risks / Trade-offs

- [A worker polls future-only work repeatedly] → Keep normal idle polling; no
  entry is claimed or retained by the worker before it is due.
- [Promotion of many overdue entries extends claim latency] → Release one due
  scheduled entry per claim and continue on later claim cycles.
- [A scheduled ID survives record deletion] → Use the same atomic cleanup path
  for explicit deletion and queued-to-terminal transitions.
- [Priority score rules differ from scheduling scores] → Keep priority storage
  and availability storage as independent indexes; derive priority from the
  durable entry during promotion.

## Migration Plan

The feature adds an optional argument and a new empty Redis key, so existing
entries and callers require no migration. Deploy code before using
`available_at`; rollback is safe after scheduled entries have been allowed to
run or explicitly removed, because an older release will not promote the new
scheduled index.
