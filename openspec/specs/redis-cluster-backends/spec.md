# Redis Cluster Backends

## Purpose

Define explicit Redis Cluster queue backends that retain the Redis queue
semantics while routing operations through the Cluster topology safely.

## Requirements

### Requirement: Select Redis Cluster through explicit queue backends
The package SHALL provide explicit Redis Cluster variants for FIFO async,
stack async, priority async, JSON async, and event queues. A configured Cluster
backend SHALL treat `LOCATION` as a Cluster seed URL and SHALL discover the
topology through that seed. Ordinary Redis queue backends SHALL continue to use
the standalone Redis topology and SHALL NOT infer or switch to Cluster mode.

#### Scenario: Configure a Cluster async queue
- **WHEN** a `QUEUES` alias selects a Redis Cluster async backend with a valid
  seed URL
- **THEN** the alias constructs a queue that routes Redis operations through
  the discovered Cluster topology

#### Scenario: Configure a standalone Redis queue
- **WHEN** a `QUEUES` alias selects an ordinary Redis async backend
- **THEN** it retains standalone Redis client behaviour without topology
  detection

### Requirement: Restrict Cluster queues to database zero
Redis Cluster queue backends SHALL require database `0`. They SHALL reject a
configured seed URL that selects any other Redis database before processing
queue work.

#### Scenario: Reject a non-zero Cluster database
- **WHEN** a Redis Cluster backend is configured with a URL selecting database
  `1` or another non-zero database
- **THEN** configuration raises an actionable error identifying database `0` as
  the only supported Cluster database

### Requirement: Preserve queue semantics on a Cluster backend
Cluster queue variants SHALL preserve the corresponding standalone backend's
public queue, lifecycle, scheduling, priority, claim, recovery, and event
delivery semantics. Every queue-owned key used in one multi-key operation SHALL
remain in the configured queue's shared hash slot.

#### Scenario: Execute a tracked priority operation on a Cluster queue
- **WHEN** a priority queue operation updates an entry record and its queue
  indexes
- **THEN** it completes with the same observable result as the corresponding
  standalone priority backend without a cross-slot error

#### Scenario: Route a queue after topology discovery
- **WHEN** the Cluster topology identifies a primary other than the seed node
  as owner of a queue's hash slot
- **THEN** queue operations are routed to that primary without application
  reconfiguration
