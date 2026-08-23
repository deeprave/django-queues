# Redis Entry Claims

## Purpose

Define atomic Redis claim ownership for identified queue entries.

## Requirements

### Requirement: Claim entries atomically
Redis queues SHALL atomically remove a pending entry from pending visibility and
record its worker-owned claim before returning it.

#### Scenario: Competing workers claim one entry
- **WHEN** two workers attempt to claim the same pending entry
- **THEN** exactly one worker receives that entry

### Requirement: Acknowledge owned claims
The system SHALL acknowledge a claim only when the requesting worker ID matches
the claim owner.

#### Scenario: Reject another worker acknowledgement
- **WHEN** a different worker acknowledges a claim
- **THEN** the claim remains recorded

### Requirement: Keep direct dequeue dispatch atomic
Redis entry-tracked direct dequeue SHALL preserve the same single-dispatch
guarantee as worker claims when promoting eligible scheduled work. It SHALL not
make one retained entry available to more than one consumer.

#### Scenario: Concurrent direct dequeue of due work
- **WHEN** multiple consumers dequeue from a Redis queue with one due scheduled
  entry
- **THEN** at most one consumer receives that entry
