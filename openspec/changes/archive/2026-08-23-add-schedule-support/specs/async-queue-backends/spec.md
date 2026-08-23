## ADDED Requirements

### Requirement: Accept an optional availability instant for identified enqueue
`AsyncQueue.aenqueue()` and its synchronous `enqueue()` counterpart SHALL
accept an optional `available_at` instant for identified entries. Backends that
do not implement delayed AsyncQueue dispatch SHALL accept the argument without
changing their existing dispatch behaviour.

#### Scenario: Enqueue future identified work asynchronously
- **WHEN** asynchronous application code awaits `aenqueue()` with `available_at`
- **THEN** the call returns an entry identifier and applies the backend's delayed-dispatch capability where supported

#### Scenario: Keep non-delayed queue compatibility
- **WHEN** a caller supplies `available_at` to a queue variant that does not implement delayed AsyncQueue dispatch
- **THEN** the queue accepts the call and retains its existing dispatch behaviour

### Requirement: Report scheduled work as pending
An AsyncQueue backend that implements delayed dispatch SHALL report scheduled
entries as pending work, including when every queued entry is future-dated.

#### Scenario: Start a configured queue with only future work
- **WHEN** a configured queue contains one or more future scheduled entries and no immediately dispatchable entries
- **THEN** its pending-work check reports work so its worker service remains active
