## MODIFIED Requirements

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
task-dispatch concept. `EventQueue` is a claimed stream: one worker consumes
each event. `NotificationQueue` is owner-less broadcast to connected nodes.
Neither uses dispatch priority to choose a consumer.

#### Scenario: Enqueue an AsyncQueue entry with an explicit priority
- **WHEN** a caller enqueues a JSON-serialisable payload on an `AsyncQueue`
  with an explicit priority value
- **THEN** the system persists an entry whose `priority` field equals that
  value, alongside the standard `queued` status and `queued_at` timestamp

#### Scenario: EventQueue ignores a supplied priority
- **WHEN** a caller enqueues a JSON-serialisable payload on an `EventQueue`
  with an explicit priority value
- **THEN** the system persists an entry whose `priority` field is `0`, and
  claimed delivery to listeners in the winning process is unaffected

#### Scenario: NotificationQueue ignores a supplied priority
- **WHEN** a caller enqueues a JSON-serialisable payload on a
  `NotificationQueue` with an explicit priority value
- **THEN** the system treats `priority` as `0`, and connected-node delivery
  is unaffected
