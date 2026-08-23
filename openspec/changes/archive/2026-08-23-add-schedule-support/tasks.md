## 1. Public AsyncQueue API

- [x] 1.1 Add `available_at: ClockTime | None = None` to identified
  `AsyncQueue.aenqueue()` and synchronous `enqueue()` signatures, preserving
  existing immediate enqueue behaviour.
- [x] 1.2 Route the argument through the entry enqueue hooks and ensure
  non-delayed queue variants accept and ignore it where required.
- [x] 1.3 Document the queue-facing `available_at` contract and its intended
  translation from an upstream absolute scheduling instant.

## 2. Redis scheduled-entry storage

- [x] 2.1 Add the `{queue}:entries:scheduled` ZSET naming and provider helpers
  for scheduled-entry membership.
- [x] 2.2 Implement a Lua-backed enqueue path that atomically writes the entry
  record and selects immediate pending membership or future scheduled
  membership using Redis-authoritative time.
- [x] 2.3 Extend ordinary and priority claim scripts to promote due scheduled
  IDs before their existing claim selection, using durable entry priority for
  priority queues.
- [x] 2.4 Keep scheduled promotion distinct from delayed-release and lease
  recovery behaviour.
- [x] 2.5 Include scheduled entries in Redis pending-work checks so workers
  remain active for future-only queues.

## 3. Lifecycle and cleanup correctness

- [x] 3.1 Extend atomic Redis deletion to remove scheduled membership alongside
  durable records, pending indexes, delayed-release state, and claim state.
- [x] 3.2 Remove scheduled membership in queued-to-terminal pre-dispatch
  cleanup paths.
- [x] 3.3 Verify recovery, release, cancellation, and explicit deletion cannot
  leave a scheduled ID eligible after its entry record is gone.

## 4. Memory backend parity

- [x] 4.1 Add local scheduled-entry tracking to the memory provider using its
  queue clock.
- [x] 4.2 Promote due memory entries into ordinary FIFO or priority selection
  before claiming.
- [x] 4.3 Include scheduled memory entries in pending-work and cleanup paths.

## 5. Tests and verification

- [x] 5.1 Test immediate, future, and already-due `available_at` values on
  memory AsyncQueue backends.
- [x] 5.2 Test Redis atomic delayed enqueue so a future entry cannot be claimed
  before its due instant.
- [x] 5.3 Test due promotion, normal and priority ordering, equal-priority
  ties, and future-only pending-work reporting on Redis.
- [x] 5.4 Test explicit deletion and queued pre-dispatch terminal transitions
  remove scheduled entries.
- [x] 5.5 Run the focused queue/provider/worker tests, then the full lint,
  formatting, type, and test suite.

## 6. Accepted review remediation

- [x] 6.1 Atomically release one due scheduled entry per Redis claim or direct
  dequeue, ordered by availability then priority within that availability group.
- [x] 6.2 Make Redis priority direct dequeue promote due scheduled work and
  remove non-queued scheduled records during promotion.
- [x] 6.3 Make queued terminal cleanup remove scheduled membership atomically.
- [x] 6.4 Add regression coverage for direct dequeue, already-due availability,
  EventQueue compatibility, terminal cleanup, and availability-group priority.
- [x] 6.5 Run focused and full validation after the remediation.
- [x] 6.6 Make memory scheduled promotion release one earliest due entry per
  dispatch round, with priority within an equal-availability group, and cover
  the FIFO and priority cases.
- [x] 6.7 Add Redis regression coverage for already-due enqueue eligibility and
  availability-first priority promotion across due availability groups.
