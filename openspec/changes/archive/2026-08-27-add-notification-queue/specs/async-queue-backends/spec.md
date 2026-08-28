## MODIFIED Requirements

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

### Requirement: Separate task and event queue semantics
The system SHALL provide `AsyncQueue`, `EventQueue`, and `NotificationQueue`
semantic base classes beneath `BaseQueue`. Existing Redis and memory queues
SHALL retain AsyncQueue semantics. Redis and memory event queue variants SHALL
remove consumed, rejected, and expired events instead of persisting terminal
states. Redis and memory notification queue variants SHALL let every
connected receiver that sees a payload handle it, own none of them, and
expire stored entries by sender-set lifetime. Provider composition and transport-specific delivery behaviour are
defined by the `provider-composition` capability.

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
