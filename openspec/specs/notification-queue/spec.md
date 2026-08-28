# Notification Queue

## Purpose

Owner-less, best-effort notification delivery: every Django process that sees
a payload may handle it, and none of them owns it. There is no claim winner,
no durable rewind, and no lifecycle result. The sender sets lifetime; the
notification worker expires stored entries. Redis expiry sets a short-lived
lease; stored reads only GET that lease, then GET the entry if it is absent.
Memory expiry is atomic under the provider lock and MAY omit a lease. Local
`@queue_listener` registrations on each process that saw the notification all
run.

## Requirements

### Requirement: Provide a NotificationQueue semantic facade
The system SHALL provide `NotificationQueue` beneath `BaseQueue`, beside
`AsyncQueue` and `EventQueue`. Memory, Redis, and Redis Cluster SHALL offer
concrete notification backends. Notification queues SHALL NOT persist terminal
task results, SHALL NOT expose `list`/`prune`, and SHALL NOT use EventQueue
claim, release, consume-remove, lease renewal as a processing budget, or
expired-claim recovery on the delivery path.

#### Scenario: Enqueue a notification
- **WHEN** a caller enqueues a JSON-serialisable payload on a NotificationQueue
- **THEN** the system accepts it without recording a task result
- **AND** connected processes that see the payload may handle it
- **AND** no process owns that payload

#### Scenario: Ignore priority and available_at
- **WHEN** a caller supplies `priority` or `available_at` on notification
  enqueue
- **THEN** those arguments are ignored the same way EventQueue ignores them

### Requirement: Deliver to every connected node that sees the payload
A Redis or Redis Cluster notification backend SHALL publish each notification
so that every process with an active receiver for that alias at publish time
can see and dispatch it. Seeing SHALL NOT require winning a claim. A process
that is not connected SHALL NOT be guaranteed to receive that notification
later. The system SHALL NOT present notification storage as a rewindable
stream. Memory notification queues SHALL deliver only inside the publishing
process.

#### Scenario: Two connected processes
- **WHEN** two Django processes have an active notification receiver for the
  same Redis alias and a producer enqueues one payload
- **THEN** both processes can dispatch that payload to their local listeners
- **AND** neither process owns the payload

#### Scenario: Disconnected process
- **WHEN** a process has no active receiver at publish time
- **THEN** it is not required to observe that notification after connecting

### Requirement: Expire notifications by sender-set lifetime
`timeout_seconds` on a notification SHALL mean a positive remaining lifetime
from enqueue, chosen by the sender. The queue `TIMEOUT` (default 60 seconds)
SHALL apply when the entry does not set a lifetime. The notification worker
SHALL expire at most one due stored copy per service tick once that
lifetime has elapsed, without a terminal entry record. `find` / `afind`
MAY still succeed after the sender lifetime until a tick reaches that copy.
Delivery SHALL NOT take an ownership claim in order to keep the payload
alive.

The store SHALL index each notification in a queue-owned deadline sorted set
scored by Redis `TIME` plus remaining lifetime. The worker SHALL discover due
entries from that index, not by scanning entry keys. Discovery and expiry
SHALL use Redis `TIME`, not a client clock. The worker SHALL run expiry on a
periodic tick even when no Pub/Sub message arrives. Concurrent workers MAY
all tick; expiry SHALL be idempotent.

#### Scenario: Expire an unpublished remainder
- **WHEN** a notification's sender-set lifetime elapses
- **THEN** the notification worker removes that stored payload without a
  task result, at most one due copy per service tick

#### Scenario: Discover due entries from the deadline index
- **WHEN** the worker's expiry tick runs
- **THEN** it selects the earliest entry ID whose deadline score is at most
  Redis `TIME`
- **AND** it expires at most that one due member on this tick
- **AND** it does not SCAN stored entry keys to find them

#### Scenario: Expire while idle
- **WHEN** no notification is published for longer than the queue lifetime
- **THEN** the worker still expires stored entries that have come due, at
  most one per service tick

### Requirement: Removal lease; stored reads are GET-only
The Redis expire path SHALL set a short-lived lease (`SET PX`) before
deleting a stored notification. Memory expiry MAY omit that lease: delete of
the stored copy and its deadline is atomic under the provider lock. Stored
reads SHALL NOT `SET` a lease. A Redis stored read SHALL GET the lease key
and, only when it is absent, GET the entry. If the lease is present the
reader SHALL treat the entry as unavailable. Pub/Sub seeing SHALL NOT
require a stored read. The lease SHALL NOT confer ownership, SHALL NOT
prevent other processes from seeing the same published message, and SHALL
NOT be renewed as a handler execution budget.

#### Scenario: Read sees an in-progress Redis expiry
- **WHEN** the Redis worker has set the removal lease for an entry
- **THEN** a stored read observes the lease and does not return that entry
  as live

#### Scenario: Read does not write
- **WHEN** two processes `afind` the same live notification
- **THEN** neither process `SET`s a lease
- **AND** both may receive the stored payload

### Requirement: Invoke all eligible local listeners
When a connected process sees a notification, it SHALL invoke every locally
registered eligible `@queue_listener` for that alias (filters still skip).
Listener return values SHALL NOT remove the notification and SHALL NOT confer
ownership. An exception in one local listener SHALL be logged and SHALL NOT
prevent other local listeners on that process from running.

#### Scenario: Two local listeners
- **WHEN** a process has two eligible notification listeners and sees a
  payload
- **THEN** both listeners are invoked
- **AND** neither returning `True` prevents another process from seeing the
  same notification

#### Scenario: Filter skip
- **WHEN** a notification listener filter returns false
- **THEN** that listener is skipped and other local listeners still run
