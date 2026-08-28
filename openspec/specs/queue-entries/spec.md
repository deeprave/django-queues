# Queue Entries

## Purpose

Define identified queue entries, their lifecycle, and their timekeeping
contract across generic queue backends.

## Requirements

### Requirement: Enqueue identified JSON-serialisable entries
The system SHALL provide an entry-oriented enqueue operation that accepts any
JSON-serialisable payload value, generates a UUID version 7 identifier, records
the queue namespace and `queued_at` timestamp, persists a `queued` entry, and
returns the generated identifier. The operation MUST reject a payload that does
not survive JSON serialisation before it persists a pending entry.
The operation SHALL be awaitable, and SHALL remain callable synchronously under
its existing name for callers that are not running on an event loop.

#### Scenario: Enqueue a JSON value
- **WHEN** a caller enqueues a JSON-serialisable payload on a named queue
- **THEN** the system returns a UUIDv7 identifier and persists an entry with
  `status` equal to `queued` and a non-null `queued_at` timestamp

#### Scenario: Reject a non-JSON payload
- **WHEN** a caller enqueues a value that cannot be JSON serialised
- **THEN** the system raises a serialisation error and does not create a pending
  queue entry

#### Scenario: Enqueue from asynchronous code
- **WHEN** asynchronous code awaits the enqueue operation
- **THEN** it receives the same identifier and persists the same entry as the
  synchronous call, without leaving the event loop

### Requirement: Expose immutable entry records
The system SHALL represent entries in Python as frozen value objects and SHALL
serialise them as JSON objects for durable storage. Entry records MUST contain
`id`, `queue`, `status`, `queued_at`, `dispatched_at`, `finished_at`, `payload`,
`result`, `error`, `timeout_seconds`, and `priority` fields. The `timeout_seconds` field
carries that entry's execution budget, or nothing when the entry was enqueued
without one. It is named for what it holds: a duration, not the instant at which
the entry expires, which is the one confusion the lifecycle instants beside it
make easy. The `priority` field is an integer dispatch priority, defaulting to
`0` when an entry is enqueued without one; a higher value SHALL dispatch before
a lower one.

Lifecycle timestamps SHALL be held as `ClockTime` values and stored as a float
count of seconds since the Unix epoch. The durable representation MUST NOT
encode a timezone or require string parsing to read, and restoring an entry MUST
yield timestamps equal to those it was stored with. An execution budget is a
duration, not an instant, and SHALL remain a plain count of seconds.

#### Scenario: Retrieve a queued entry
- **WHEN** a caller retrieves an entry by an identifier returned from enqueue
- **THEN** the system returns an immutable entry record with the original
  payload and required lifecycle fields

#### Scenario: Store a lifecycle timestamp
- **WHEN** an entry carrying lifecycle timestamps is written to its durable
  representation
- **THEN** each timestamp appears as a float count of seconds since the Unix
  epoch

#### Scenario: Round-trip an entry without losing its instant
- **WHEN** an entry is stored and restored
- **THEN** its restored lifecycle timestamps equal the values it was created
  with, on every backend

#### Scenario: Reject a lifecycle timestamp that is not an instant
- **WHEN** an entry is constructed with a lifecycle timestamp that is not a
  `ClockTime`, including a null where one is required
- **THEN** construction fails

#### Scenario: Reject a malformed durable record
- **WHEN** a record is restored whose stored identifier, status or lifecycle
  timestamp cannot be read back, whether because its type or its value is wrong
- **THEN** restoration fails with a single error naming the field, chained to
  the cause

#### Scenario: Reject a record that omits a required field
- **WHEN** a record is restored that has no value at all for a required field
- **THEN** restoration fails the same way, naming the field that is absent

#### Scenario: Report a duration shorter than a second
- **WHEN** an entry's handler ran for a fraction of a second
- **THEN** the reported duration carries that fraction rather than truncating to
  zero

#### Scenario: Report how long an entry waited and ran
- **WHEN** an entry that was dispatched and finished is read
- **THEN** it reports the seconds between being queued and dispatched, and the
  seconds between being dispatched and finishing

#### Scenario: Report no duration before the instants exist
- **WHEN** an entry that has not been dispatched, or has been dispatched but not
  finished, is read
- **THEN** the durations its instants cannot yet describe are absent

#### Scenario: Report no duration when the instants contradict
- **WHEN** an entry holds a later lifecycle instant that precedes an earlier one
- **THEN** the duration between them is absent rather than negative

#### Scenario: Keep durations out of the durable record
- **WHEN** an entry is written to its durable representation
- **THEN** that representation carries only the instants, and a restored entry
  reports the same durations as the entry it was restored from

#### Scenario: Retrieve an entry enqueued with a budget
- **WHEN** a caller retrieves an entry that was enqueued with an execution
  budget
- **THEN** the returned record and its durable representation both carry that
  budget

#### Scenario: Default an entry's priority
- **WHEN** a caller enqueues a payload without specifying a priority
- **THEN** the returned record and its durable representation carry priority
  `0`

#### Scenario: Retrieve an AsyncQueue entry enqueued with a priority
- **WHEN** a caller retrieves an `AsyncQueue` entry that was enqueued with an
  explicit priority
- **THEN** the returned record and its durable representation both carry that
  priority

### Requirement: Enqueue an identified AsyncQueue entry with a dispatch priority
`AsyncQueue`'s entry-oriented enqueue operation SHALL accept an optional
integer `priority`, defaulting to `0`, and persist it on the resulting entry
alongside the standard lifecycle fields. Supplying a priority MUST NOT change
any other enqueue behaviour: JSON validation, identifier generation, and the
`queued` status and `queued_at` timestamp are unaffected.

`EventQueue`'s and `NotificationQueue`'s entry-oriented enqueue operations
SHALL accept the same optional `priority` keyword, for signature compatibility
with the shared enqueue contract, but MUST ignore it: the persisted `priority`
is always `0` regardless of the value supplied. Priority ordering is a
task-dispatch concept. `EventQueue` is distributed processing: a worker that
claims an event thereafter owns it. `NotificationQueue` is see-but-do-not-own:
every connected receiver that sees a notification may handle it, and none
owns it. Neither uses dispatch priority to choose a consumer.

#### Scenario: Enqueue an AsyncQueue entry with an explicit priority
- **WHEN** a caller enqueues a JSON-serialisable payload on an `AsyncQueue`
  with an explicit priority value
- **THEN** the system persists an entry whose `priority` field equals that
  value, alongside the standard `queued` status and `queued_at` timestamp

#### Scenario: EventQueue ignores a supplied priority
- **WHEN** a caller enqueues a JSON-serialisable payload on an `EventQueue`
  with an explicit priority value
- **THEN** the system persists an entry whose `priority` field is `0`, and
  claimed, owned delivery in the winning process is unaffected

#### Scenario: NotificationQueue ignores a supplied priority
- **WHEN** a caller enqueues a JSON-serialisable payload on a
  `NotificationQueue` with an explicit priority value
- **THEN** the system treats `priority` as `0`, and see-but-do-not-own
  delivery is unaffected

### Requirement: Record entry lifecycle outcomes

AsyncQueue lifecycle transitions are worker-internal operations. A worker SHALL
record `running`, terminal, and recovery outcomes without exposing public queue
mutation methods for those transitions. `cancelled` remains a valid reserved
terminal status, but no current worker path produces it.

#### Scenario: Record successful handling
- **WHEN** a worker handler returns a result for a running entry
- **THEN** the entry is stored with status `succeeded`, its `result` value, and a
  non-null `finished_at` timestamp

#### Scenario: Record failed handling
- **WHEN** a worker handler raises an exception
- **THEN** the entry is stored with status `failed`, a structured error value
  containing only a safe exception class and message, and a non-null
  `finished_at` timestamp

#### Scenario: Record a failure before handler dispatch
- **WHEN** queue processing detects a validation, transport, or other
  pre-dispatch failure for a queued entry
- **THEN** the entry is stored with status `failed`, a structured error value,
  a non-null `finished_at` timestamp, and no `dispatched_at` timestamp

#### Scenario: Reserve cancelled handling
- **WHEN** a worker-internal lifecycle operation records a running entry as
  cancelled
- **THEN** the entry is stored with status `cancelled` and a non-null
  `finished_at` timestamp

#### Scenario: Record a timed-out handling
- **WHEN** a worker abandons a handler that exceeded its execution budget
- **THEN** the entry is stored with status `timeout` and a non-null
  `finished_at` timestamp, and no further transition is permitted

#### Scenario: Recover an abandoned running entry
- **WHEN** reliable-delivery recovery reclaims an expired running entry
- **THEN** it resets the entry to `queued` and clears its execution timestamps
- **AND** its next worker attempt records a new `dispatched_at` timestamp

### Requirement: Use queue-authoritative lifecycle time
Redis-backed queues SHALL source lifecycle timestamps from a Redis-aligned UTC
clock. The clock MUST obtain an initial Redis time calibration, calculate interim
timestamps from local UTC plus the cached Redis-to-local offset, and start no
more than one background refresh every 600 seconds. Redis unavailability and a
Redis/local UTC drift greater than 180 seconds MUST make initial calibration
fail clearly. After an initial calibration, a failed background refresh SHALL
retain the last good offset and retry no earlier than the next refresh interval.
Non-Redis queues SHALL document their use of local UTC fallback time.

A clock SHALL report the current instant as a `ClockTime`, and a Redis-aligned
clock SHALL build it from the whole second and microsecond counts the server
reports, without constructing an intermediate datetime or string. Its
calibration offset SHALL be a count of seconds. A queue SHALL expose its clock,
so a component recording times alongside that queue's entries can use the same
basis rather than local time.

#### Scenario: Timestamp entries without repeated Redis time calls
- **WHEN** a Redis queue creates two lifecycle timestamps within the configured
  refresh interval
- **THEN** it derives the later timestamp from its existing Redis calibration
  without issuing a second Redis `TIME` command

#### Scenario: Refresh Redis time without delaying timestamping
- **WHEN** a Redis calibration reaches its refresh interval
- **THEN** the queue returns timestamps using its current offset while one
  background refresh obtains the next Redis calibration

#### Scenario: Read a Redis-aligned instant without an intermediate form
- **WHEN** a Redis-aligned clock reports the current instant
- **THEN** it builds that instant from the second and microsecond integers the
  server reports, without constructing a datetime or a string on the way

#### Scenario: Share a queue's clock with a component it creates
- **WHEN** a component asks a queue for its clock
- **THEN** it receives the clock that queue timestamps its entries with

### Requirement: Construct configured entry subclasses
Each queue backend SHALL create and restore entries with its alias's resolved
`ENTRY_CLASS`. The class MUST extend `QueueEntry`; the resulting value MUST
retain all base entry fields, immutable lifecycle behaviour, and JSON durable
representation. Fields the subclass declares MUST be persisted and restored
alongside the base fields without the subclass overriding any conversion
method. A backend MUST NOT instantiate an entry during settings or queue
construction; it SHALL do so only for enqueue, restore, or lifecycle operations
that require an entry value.

#### Scenario: Enqueue with a custom entry class
- **WHEN** a queue defines a valid `ENTRY_CLASS` subclass and a caller enqueues
  a JSON-serialisable payload
- **THEN** the backend stores and returns that entry subclass with the standard
  queued lifecycle fields

#### Scenario: Persist a field the subclass declares
- **WHEN** a queue's `ENTRY_CLASS` declares a JSON-serialisable field beyond the
  base entry's and an entry is stored and read back
- **THEN** the restored entry carries that field's value, with no conversion
  method overridden on the subclass

#### Scenario: Construct an idle configured queue
- **WHEN** Django initialises a queue with a valid custom `ENTRY_CLASS` but no
  entry operation occurs
- **THEN** the queue is constructed without creating an entry instance

#### Scenario: Restore a custom entry after a lifecycle transition
- **WHEN** a backend retrieves or updates an entry written with its configured
  entry subclass
- **THEN** it restores the configured subclass and preserves its standard
  lifecycle transition semantics

### Requirement: Remove expired event entries
For an event queue, `timeout_seconds` SHALL mean the event's positive lifetime
while it is unclaimed. A consumed, rejected, or expired unclaimed event SHALL
be removed without a task terminal result. Redis and memory backends SHALL
prune expired unclaimed events while receiving and during idle cleanup.

#### Scenario: Expire an unconsumed event
- **WHEN** an unclaimed event remains available for its resolved lifetime
- **THEN** the backend logs and removes it without a terminal entry record

#### Scenario: Reach expiry while claiming
- **WHEN** an unclaimed event reaches its resolved lifetime immediately before a worker claims it
- **THEN** the claim atomically removes the event and does not dispatch it
