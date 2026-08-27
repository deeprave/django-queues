## Purpose

Owner-less, best-effort notification delivery: every Django process connected
at publish time receives the same short-lived payload independently. There is
no claim winner, no durable replay, and no lifecycle result. Payloads expire
by TTL. Local `@queue_listener` registrations on each connected node all see
the notification.

## ADDED Requirements

### Requirement: Provide a NotificationQueue semantic facade
The system SHALL provide `NotificationQueue` beneath `BaseQueue`, beside
`AsyncQueue` and `EventQueue`. Memory, Redis, and Redis Cluster SHALL offer
concrete notification backends. Notification queues SHALL NOT persist terminal
task results, SHALL NOT expose `list`/`prune`, and SHALL NOT use claim, lease
renewal, or expired-claim recovery on the delivery path.

#### Scenario: Enqueue a notification
- **WHEN** a caller enqueues a JSON-serialisable payload on a NotificationQueue
- **THEN** the system accepts it without recording a task result
- **AND** connected processes with listeners for that alias may receive it

#### Scenario: Ignore priority and available_at
- **WHEN** a caller supplies `priority` or `available_at` on notification
  enqueue
- **THEN** those arguments are ignored the same way EventQueue ignores them

### Requirement: Deliver to every connected node
A Redis or Redis Cluster notification backend SHALL publish each notification
so that every process with an active receiver for that alias at publish time
can dispatch it. A process that is not connected SHALL NOT be guaranteed to
receive that notification later. Memory notification queues SHALL deliver only
inside the publishing process.

#### Scenario: Two connected processes
- **WHEN** two Django processes have an active notification receiver for the
  same Redis alias and a producer enqueues one payload
- **THEN** both processes can dispatch that payload to their local listeners

#### Scenario: Disconnected process
- **WHEN** a process has no active receiver at publish time
- **THEN** it is not required to observe that notification after connecting

### Requirement: Expire notifications by lifetime
`timeout_seconds` on a notification SHALL mean a positive remaining lifetime
from enqueue. The queue `TIMEOUT` (default 60 seconds) SHALL apply when the
entry does not set a lifetime. After that lifetime the payload SHALL be gone
without a terminal entry record. Delivery SHALL NOT take a worker claim in
order to keep the payload alive.

#### Scenario: Expire an unpublished remainder
- **WHEN** a notification's lifetime elapses
- **THEN** the backend removes any remaining payload without a task result

### Requirement: Invoke all eligible local listeners
When a connected process receives a notification, it SHALL invoke every
locally registered eligible `@queue_listener` for that alias (filters still
skip). Listener return values SHALL NOT remove the notification from other
processes. An exception in one local listener SHALL be logged and SHALL NOT
prevent other local listeners on that process from running.

#### Scenario: Two local listeners
- **WHEN** a process has two eligible notification listeners and receives a
  payload
- **THEN** both listeners are invoked
- **AND** neither returning `True` prevents the other process from receiving
  the same notification

#### Scenario: Filter skip
- **WHEN** a notification listener filter returns false
- **THEN** that listener is skipped and other local listeners still run
