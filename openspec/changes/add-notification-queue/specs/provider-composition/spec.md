## MODIFIED Requirements

### Requirement: Compose providers into semantic queue facades
`AsyncQueue`, `EventQueue`, and `NotificationQueue` SHALL provide generic
queue-facing semantics by composing a provider. They SHALL NOT inherit, mirror,
or expose a provider's transport operations as queue methods. Concrete backend
queues, including `RedisAsyncQueue`, `RedisEventQueue`,
`RedisNotificationQueue`, `MemoryAsyncQueue`, `MemoryEventQueue`, and
`MemoryNotificationQueue`, SHALL only select and inject their provider and
default worker behaviour.

#### Scenario: Construct a Redis async queue
- **WHEN** an application constructs `RedisAsyncQueue`
- **THEN** it receives an `AsyncQueue` semantic facade composed with a Redis
  provider and a Redis-aware default async-queue worker

#### Scenario: Construct a Redis notification queue
- **WHEN** an application constructs `RedisNotificationQueue`
- **THEN** it receives a `NotificationQueue` semantic facade composed with a
  Redis provider and a Redis-aware notification worker that does not claim

#### Scenario: Use a queue-facing API
- **WHEN** application code produces, reads, or administers queue entries
- **THEN** it uses queue semantic operations and does not receive a provider
  instance or transport coordination value

### Requirement: Keep delivery semantics transport-specific
The common `QueueProvider` protocol SHALL initially declare only asynchronous
resource closure. It SHALL NOT declare a clock, storage, pending-work, claim,
renew, acknowledge, release, settle, recovery, retention, or pruning operation.
An operation SHALL be promoted into the common protocol only after multiple
providers require the same transport-independent contract. A transport-aware
worker SHALL implement delivery using its provider's native model.

#### Scenario: Redis delivery
- **WHEN** a Redis default worker dispatches an AsyncQueue or EventQueue entry
- **THEN** it uses Redis claim, lease renewal, acknowledgement, settlement,
  recovery, and retention operations owned by the Redis provider as applicable
  to that semantic type

#### Scenario: Redis notification delivery
- **WHEN** a Redis notification worker dispatches a payload
- **THEN** it does not take a claim or renew a lease in order to deliver it

#### Scenario: Introduce a non-Redis transport
- **WHEN** a JetStream, NATS, Kafka, or SQS provider is added
- **THEN** it can select a worker that uses its native acknowledgement,
  visibility, or commit model without implementing Redis claim semantics

#### Scenario: Require a shared provider operation
- **WHEN** more than one provider needs the same transport-independent
  operation
- **THEN** that operation may be promoted into `QueueProvider` with scenarios
  covering each provider

### Requirement: Select workers by concrete backend
Each concrete queue backend SHALL declare an overridable default worker class
appropriate to its provider. `RedisAsyncQueue` and `RedisEventQueue` SHALL
select Redis-aware workers that use claims where that semantic type requires
them. `RedisNotificationQueue` SHALL select a Redis-aware notification worker
that does not claim. Memory queue variants SHALL select memory-aware workers.
Common worker base classes SHALL NOT access a composed provider or require
claim, acknowledgement, retry, renewal, recovery, or settlement operations.
Queue configuration SHALL continue to permit an explicit compatible worker
override.

#### Scenario: Run configured Redis queues
- **WHEN** Django starts configured Redis task, event, and notification queues
- **THEN** their respective Redis-aware workers are selected without callers
  supplying transport details

#### Scenario: Run configured memory queues
- **WHEN** Django starts configured memory async, event, or notification queues
- **THEN** their respective memory-aware workers are selected without callers
  supplying transport details

#### Scenario: Override a worker
- **WHEN** configuration supplies a compatible worker class for a concrete
  queue backend
- **THEN** the queue uses that override while preserving the backend's provider
  composition
