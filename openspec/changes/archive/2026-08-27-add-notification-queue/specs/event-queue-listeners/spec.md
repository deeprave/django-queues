## MODIFIED Requirements

### Requirement: Register process-local event listeners
The system SHALL export `queue_listener` from `django_queue`. The decorator
SHALL register the decorated sync or async callable for one named EventQueue
or NotificationQueue alias and accept an optional `filter` callable receiving
a QueueEntry and returning a bool. Registration SHALL be process-local and
SHALL NOT require an async-queue handler. EventQueue dispatch (rotating
fairness, consume on `True`/`False`, Redis claims) is unchanged. Notification
dispatch is defined by the `notification-queue` capability.

#### Scenario: Register an asynchronous listener
- **WHEN** an application decorates an async function with `@queue_listener("events")`
- **THEN** the function is registered for the events queue in that Django process

#### Scenario: Register a notification listener
- **WHEN** an application decorates a function with `@queue_listener` for a
  NotificationQueue alias
- **THEN** the function is registered for that alias in that Django process

#### Scenario: Skip a filtered event
- **WHEN** a listener filter returns false for an event
- **THEN** dispatch skips that listener and continues to the next listener

#### Scenario: Reject an AsyncQueue listener
- **WHEN** an application decorates a listener for an AsyncQueue
- **THEN** registration raises an error identifying that listeners require an
  EventQueue or NotificationQueue
