# Async Queue Backends

## Purpose

Define the asynchronous backend contract and its synchronous compatibility
surface.

## Requirements

### Requirement: Present an asynchronous backend contract
Queue backends SHALL expose every public operation that performs storage work
as an awaitable method named with an `a` prefix: `aenqueue`, `afind`,
`adequeue`, `ahas_pending`, and `aclose`. Retained `AsyncQueue` lifecycle
backends SHALL additionally provide `aprune`. These
SHALL be the implementations, not wrappers. A backend supporting identified
entry dispatch MUST implement the applicable operations. Lifecycle transitions to `running`,
`succeeded`, `failed`, `cancelled`, and `timeout` are worker-internal
operations and SHALL NOT be public queue APIs.

#### Scenario: Await an entry operation
- **WHEN** a caller awaits an entry operation on any configured backend
- **THEN** the operation completes without dispatching work to a worker thread

#### Scenario: Implement a custom backend
- **WHEN** an application supplies a queue backend implementing the asynchronous
  methods
- **THEN** a worker dispatches through it without the backend defining any
  synchronous entry method

### Requirement: Keep the synchronous names working for synchronous callers
Each asynchronous operation SHALL have a synchronous counterpart under the name
that operation has today -- `enqueue`, `find`, `close`, and the rest --
delegating to the asynchronous implementation through the framework's
synchronous-to-asynchronous bridge. A synchronous caller SHALL observe the same
behaviour, return value, and exceptions as before this change.

#### Scenario: Enqueue from a synchronous Django view
- **WHEN** synchronous application code calls `enqueue` with a payload and an
  optional budget
- **THEN** it receives the entry identifier, exactly as it did when the backend
  was synchronous

#### Scenario: Refuse a synchronous call from a running event loop
- **WHEN** code already running on an event loop calls a synchronous wrapper
- **THEN** the call raises, directing the caller to await the asynchronous
  method instead of blocking the loop it is running on

### Requirement: Cross between synchronous and asynchronous code through the framework
The package SHALL perform every crossing between synchronous and asynchronous
execution using the bridges the framework provides, and SHALL NOT dispatch to a
thread directly or implement an adaptor of its own.

#### Scenario: Bridge a blocking operation
- **WHEN** an operation blocks by design and must be awaited
- **THEN** it is bridged with the framework's asynchronous adaptor rather than
  by direct thread dispatch

### Requirement: Bind connection resources to the event loop that uses them
A backend holding loop-affine connection resources SHALL acquire them for the
running event loop rather than at construction, so resources created on one
loop are never used from another. Disposal SHALL release the resources
belonging to the loop that disposes them.

#### Scenario: Use one queue from a worker loop and a synchronous caller
- **WHEN** the same configured queue is used by a worker on its event loop and
  by a synchronous caller whose bridge runs on a different loop
- **THEN** each obtains connection resources belonging to its own loop and
  neither observes the other's

#### Scenario: Dispose a queue
- **WHEN** a caller closes a queue
- **THEN** the connection resources for that loop are released and the queue
  can acquire fresh resources if used again

### Requirement: List retained entry snapshots
Each AsyncQueue backend SHALL provide synchronous and asynchronous operations
that return its currently retained immutable QueueEntry snapshots for observer
bootstrap. The operations SHALL return queued, running, and terminal entries.

#### Scenario: List an AsyncQueue's retained entries
- **WHEN** an observer runtime requests the snapshots for an AsyncQueue
- **THEN** it receives every retained entry snapshot in that queue

### Requirement: Prune a retained AsyncQueue entry
`AsyncQueue` SHALL expose `aprune(entry_id)` and its synchronous counterpart
`prune(entry_id)` for removing one retained terminal entry.
`BaseQueue`, `EventQueue`, and `NotificationQueue` SHALL NOT expose these
entry-retention operations. Scheduled cleanup and explicit pruning SHALL use
the same removal behavior.

#### Scenario: Prune from synchronous application code
- **WHEN** synchronous application code prunes one terminal AsyncQueue entry
- **THEN** it observes the same removal and exception behavior as
  `aprune`

### Requirement: Report an identified AsyncQueue entry that does not exist
AsyncQueue entry lookup and explicit pruning SHALL raise
`QueueEntryNotFoundError` when the requested retained entry ID has no durable
record. `QueueEmptyException` SHALL remain reserved for queue-dequeue
operations, and `QueueEntryMissingError` SHALL remain specific to
reliable-delivery claim settlement.

#### Scenario: Look up an absent entry
- **WHEN** a caller retrieves an AsyncQueue entry ID whose retained record does
  not exist
- **THEN** the backend raises `QueueEntryNotFoundError`

### Requirement: Separate task and event queue semantics
The system SHALL provide `AsyncQueue`, `EventQueue`, and `NotificationQueue`
semantic base classes beneath `BaseQueue`. Existing Redis and memory queues
SHALL retain AsyncQueue semantics. Redis and memory event queue variants SHALL
remove consumed, rejected, and expired events instead of persisting terminal
states. Redis and memory notification queue variants SHALL let every
connected receiver that sees a payload handle it, own none of them, and
expire stored entries by sender-set lifetime. Provider composition and
transport-specific delivery behaviour are defined by the
`provider-composition` capability.

#### Scenario: Retain an AsyncQueue outcome
- **WHEN** an AsyncQueue worker records a terminal outcome
- **THEN** its entry remains available under the existing lifecycle contract

#### Scenario: List retained AsyncQueue snapshots
- **WHEN** an application or observer needs its initial AsyncQueue state
- **THEN** it can call `alist()` (or synchronous `list()`) to obtain the
  retained entry snapshots

#### Scenario: Remove a consumed event
- **WHEN** an event worker acknowledges an event it owns
- **THEN** the backend removes its pending representation and entry record

#### Scenario: See a notification without owning it
- **WHEN** a producer enqueues on a NotificationQueue while two receivers are
  connected
- **THEN** both receivers can dispatch that payload
- **AND** neither receiver owns it via claim, release, or consume-remove

### Requirement: Use canonical lifecycle-record operation names
AsyncQueue backend contracts SHALL use the canonical names defined by the API
naming capability for lifecycle-record operations and their asynchronous
counterparts. A qualifier SHALL remain only where it distinguishes a
lifecycle-record operation from an existing raw-value queue operation.

#### Scenario: Implement a custom backend after the naming cleanup
- **WHEN** an application implements an AsyncQueue backend
- **THEN** it implements only the canonical synchronous and asynchronous
  lifecycle-record operation names

#### Scenario: Call a canonical lifecycle-record operation
- **WHEN** application code calls a canonical lifecycle-record operation
- **THEN** the backend performs the same queue behavior previously associated
  with the superseded operation

#### Scenario: Provide the retained record collection
- **WHEN** observation or administration needs current retained records
- **THEN** an AsyncQueue provides `alist` without a redundant `_entries` suffix

### Requirement: Dispatch tracked entries in priority order on a priority backend
A backend declared as a priority variant SHALL dispatch retained, entry-tracked
work in descending priority order — the highest-priority queued entry SHALL be
the next one an entry-tracked dequeue operation returns, ahead of any
lower-priority entry regardless of enqueue order. Among entries sharing the same
priority, the backend SHALL preserve that variant's existing ordering guarantee
(FIFO for a plain priority queue). This ordering guarantee applies to the
entry-tracked enqueue/dequeue operations that produce and consume `QueueEntry`
records; it does not alter the separate untracked value-only API a caller may
use directly against the same backend.

A non-priority backend SHALL continue to dispatch entries in that backend's
existing order (FIFO or LIFO) and MUST NOT consult an entry's `priority` field.

#### Scenario: Dispatch the higher-priority entry first
- **WHEN** two entries are enqueued on a priority backend through the
  entry-tracked enqueue operation, the lower-priority entry first and the
  higher-priority entry second
- **THEN** an entry-tracked dequeue returns the higher-priority entry before
  the lower-priority one

#### Scenario: Preserve arrival order within equal priority
- **WHEN** two entries of equal priority are enqueued on a priority backend
  through the entry-tracked enqueue operation, in a given order
- **THEN** entry-tracked dequeues return them in that same order

#### Scenario: Track a dispatched priority entry
- **WHEN** an entry enqueued with a priority on a priority backend is
  dequeued through the entry-tracked dequeue operation
- **THEN** the returned entry is a full `QueueEntry` that can be found by its
  identifier and carries its lifecycle transitions, the same as on a
  non-priority backend

#### Scenario: Ignore priority on a non-priority backend
- **WHEN** an entry is enqueued with a non-zero priority on a backend that is
  not a priority variant
- **THEN** the backend dispatches it in that backend's existing FIFO or LIFO
  order, unaffected by its priority value

### Requirement: Accept an optional availability instant for identified enqueue
`AsyncQueue.aenqueue()` and its synchronous `enqueue()` counterpart SHALL
accept an optional `available_at` instant for identified entries. Backends that
do not implement delayed AsyncQueue dispatch SHALL accept the argument without
changing their existing dispatch behaviour.

#### Scenario: Enqueue future identified work asynchronously
- **WHEN** asynchronous application code awaits `aenqueue()` with `available_at`
- **THEN** the call returns an entry identifier and applies the backend's
  delayed-dispatch capability where supported

#### Scenario: Keep non-delayed queue compatibility
- **WHEN** a caller supplies `available_at` to a queue variant that does not
  implement delayed AsyncQueue dispatch
- **THEN** the queue accepts the call and retains its existing dispatch behaviour

### Requirement: Report scheduled work as pending
An AsyncQueue backend that implements delayed dispatch SHALL report scheduled
entries as pending work, including when every queued entry is future-dated.

#### Scenario: Start a configured queue with only future work
- **WHEN** a configured queue contains one or more future scheduled entries and
  no immediately dispatchable entries
- **THEN** its pending-work check reports work so its worker service remains
  active
