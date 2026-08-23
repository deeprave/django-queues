## ADDED Requirements

### Requirement: Keep direct dequeue dispatch atomic
Redis entry-tracked direct dequeue SHALL preserve the same single-dispatch
guarantee as worker claims when promoting eligible scheduled work. It SHALL not
make one retained entry available to more than one consumer.

#### Scenario: Concurrent direct dequeue of due work
- **WHEN** multiple consumers dequeue from a Redis queue with one due scheduled
  entry
- **THEN** at most one consumer receives that entry
