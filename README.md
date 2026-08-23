[![Build Status](https://img.shields.io/github/actions/workflow/status/deeprave/django-queues/python-test-and-build.yml?branch=main&label=CI&logo=github)](https://github.com/deeprave/django-queues/actions/workflows/python-test-and-build.yml)
[![Maintenance](https://img.shields.io/badge/maintenance-active-brightgreen.svg)](https://github.com/deeprave/django-queues)
[![PyPI version](https://img.shields.io/pypi/v/django-queues.svg?logo=pypi&logoColor=white)](https://pypi.org/project/django-queues/)
[![PyPI downloads](https://img.shields.io/pypi/dm/django-queues.svg?logo=pypi&logoColor=white)](https://pypi.org/project/django-queues/)
[![Python versions](https://img.shields.io/pypi/pyversions/django-queues.svg?logo=python&logoColor=white)](https://pypi.org/project/django-queues/)

# Django Queues

This is an implementation of message queues for Django.

The current implementation supports an in-memory queue and a Redis-backed pub/sub queue with Redis Functions (Lua) for atomic updates.

## Requirements

`django-queues` requires Python 3.14 or later, as queue entry IDs use the standard-library UUIDv7 implementation to support ordering introduced in Python 3.14. Redis-backed queues require Redis 7 or later. The application requires FCALL permission, and the deployment `redis_lua_lib` management command requires Function-library deployment permissions. Redis Cluster is not currently supported.

## Choose a queue type

Choose the semantic queue type first; choose its memory or Redis backend second.

| Choose | Use it for | Classes | Consumer model | Retention |
| --- | --- | --- | --- | --- |
| **Async queue** | Work that runs later and whose progress or outcome must be inspectable | `MemoryAsyncQueue`, `RedisAsyncQueue`, and their stack/priority variants | An async `HANDLER`, normally run by `manage.py runqueues` | A durable lifecycle: `queued`, `running`, then `succeeded`, `failed`, or `timeout` until pruning |
| **Event queue** | Short-lived notifications delivered to one or more local listeners | `MemoryEventQueue`, `RedisEventQueue` | `@queue_listener`; Django starts the queue runtime once at process startup when at least one queue is configured | Consumed, retried, or expired; no durable outcome record |

Async queues are the correct choice when a producer needs to determine a result, observe lifecycle progress, or retain completed work temporarily.

Event queues are for streaming data in "fan-out" fashion to one or more consumers, and no result needs to be retrieved afterwards.

## Backend choices

Currently only two:

- **Memory** queues exist only while the application process runs. Configured
  `MemoryAsyncQueue` instances are local to the resolving process and thread;
  `MemoryEventQueue` is process-scoped.
- **Redis** queues are shared and persistent. Use them when producers and
  consumers run in different processes, containers, or hosts. Install support
  with `pip install "django-queues[redis]"`.

FIFO, LIFO stack, and priority ordering are available for async queues. Event queues use their selected backend's transient delivery semantics.

## Configuration

Queues are configured in the Django settings module, and use a simple and familiar configuration format like **DATABASES** and **CACHES**.

### Async queues

An async queue is a good choice for task monitoring or collecting results from code that has been handed off to another thread, process, or computer.

An async queue used only by producers is configured as follows:

```python
QUEUES = {
    "default": {
        "BACKEND": "django_queue.backends.redis.RedisAsyncQueueJson",
        "LOCATION": f"redis://localhost:6379/12",
        "maxsize": 64,
    },
}
```

This configures a Redis-backed FIFO async queue with JSON values. Redis-backed queues own their asynchronous connections; application code does not supply Redis client instances.

## ⚠️ Required Redis initialisation

> **Redis-backed queues cannot start until the bundled `django_queues` Redis
> Function library has been installed on every target Redis instance.** This is
> a required deployment step, not an application-startup responsibility.

Run this with a deployment credential after Redis is available and before any
application or worker that uses Redis-backed queues starts:

```sh
python manage.py redis_lua_lib --deploy
```

The deployment credential needs permission to inspect and load Redis Function
libraries, and to acquire and release the command's short-lived deployment
lease (`SET`, `EVAL`, and the scripted `GET`/`DEL` release). Application
credentials need only the Function calls required by normal queue operation.

The command derives and deduplicates its Redis targets from `QUEUES`. Its
exceptional `--redis-url` option deploys to only that URL, but avoid it where
possible because command-line URLs can expose credentials through shell
history. The command without `--deploy` is a non-mutating preflight check; it
reports the installed library and API versions, and fails if deployment is
required.

Then, with the application credential, verify the application can invoke the
library:

```sh
python manage.py redis_lua_compat
```

The demo Compose configurations show the intended startup ordering: their
dashboard service waits for Redis to become healthy, runs
`redis_lua_lib --deploy`, and only then starts Django. See
[`demo_aq/compose.yaml`](demo_aq/compose.yaml),
[`demo_eq/compose.yaml`](demo_eq/compose.yaml), and
[`demo_pq/compose.yaml`](demo_pq/compose.yaml).

### Redis persistence

Redis Functions are persisted with Redis data: they are included in RDB
snapshots and the AOF. They are therefore not inherently lost on restart. A
Docker container that is recreated, however, loses its writable filesystem
unless Redis data is stored in a persistent volume. The demo Redis services
deliberately have no `/data` volume, so `docker compose down` followed by a new
`up` starts a fresh Redis database and requires the library to be deployed
again.

> **Deployment advice:** mount Redis `/data` on persistent storage and
> configure Redis persistence (AOF is usually appropriate) when the deployed
> library should survive Redis-service recreation. Without that volume,
> recreating Redis discards the library and it must be deployed again before
> applications start. Keep the explicit deployment step as a startup/deployment
> safety check.

To implement a stack (LIFO), use
`django_queue.backends.redis.RedisAsyncStackJson`, or add `"stack": True`.

All aliases are validated and initialised when Django starts. Application code can retrieve a configured queue through `queues["alias"]`; initialisation only constructs queue services and never starts a worker. Queue aliases may contain only ASCII letters, digits, `_`, and `-`.

### Configuration reference

The alias is the queue's stable application identity. It is the key in
`QUEUES`, not a separate setting; `queue_name` is not a supported option.

| Setting | Applies to | Meaning |
| --- | --- | --- |
| `BACKEND` | All queues; required | Dotted class path for the queue backend. It selects both the semantic kind (`AsyncQueue` or `EventQueue`) and storage provider. |
| `LOCATION` | All queues | Backend location. Redis backends require a Redis URL such as `redis://localhost:6379/12`; memory backends ignore it and may omit it. |
| `HANDLER` | Async queues only | Dotted path to the async callable that handles entries. Its presence opts that alias into `manage.py runqueues`; it is not passed to the backend. Event queues reject it because they use listeners. |
| `WORKER` | Optional | Compatible concrete worker class or dotted class path. Omit it to use the backend's default; Redis and memory workers are provider-specific. |
| `ENTRY_CLASS` | Optional | `QueueEntry` subclass or dotted class path used for queue entries. It defaults to `QueueEntry`; extra fields must be JSON-serialisable. |
| `TIMEOUT` | All queues | For an async queue, the default execution budget for its handlers (600 seconds when unset). For an event queue, the unclaimed event lifetime (60 seconds when unset). An entry-specific `timeout_seconds` takes precedence. |
| `RETENTION_TIMEOUT` | Async queues only | Terminal-record retention in seconds. Defaults to 600; set to `None` to disable automatic cleanup. Event queues do not retain terminal records. |

Built-in backend options are deliberately small:

| Option | Applies to | Meaning |
| --- | --- | --- |
| `maxsize` | Memory and Redis raw-value operations | Maximum number of values accepted by `add`; `0` (the default) is unbounded. |
| `stack` | Redis queues and memory async queues | Use LIFO ordering instead of FIFO. Prefer the explicit `RedisAsyncStack` backend where one exists. |
| `encoding` | Redis queues | Python codec used for raw Redis values; defaults to UTF-8. |

Custom backends may document additional options. Queue metadata (`HANDLER`,
`WORKER`, `ENTRY_CLASS`, `TIMEOUT`, and `RETENTION_TIMEOUT`) is consumed by
Django Queue and is never forwarded to a backend constructor.

### Event queues

`MemoryEventQueue` and `RedisEventQueue` deliver short-lived events to local listeners instead of retaining async-work outcomes. Configure one explicitly, then register one or more listeners in application code:

```python
# settings.py
QUEUES = {
    "events": {
        "BACKEND": "django_queue.backends.redis.RedisEventQueue",
        "LOCATION": "redis://localhost:6379/12",
    },
}


# myproject/listeners.py
from django_queue import queue_listener


@queue_listener("events")
async def send_notification(entry):
    await notify(entry.payload)
    return True
```

An eligible listener returning `True` consumes and removes the event. Returning `False` logs a rejection and also removes it; returning `None` lets the next listener see it. If every listener passes, or a filter/listener raises, the event is released for a short delayed retry. Events expire unconsumed after an entry-specific `timeout_seconds`, the queue `TIMEOUT`, or 60 seconds by default. They never acquire a task result or terminal entry record.

Django starts one process-local queue runtime once, at process startup, when `QUEUES` is non-empty. It owns one background thread and one asyncio loop, shared by every configured event queue's worker task and every observed async queue's Redis receiver task. An alias may set `WORKER` to an event-worker subclass compatible with the selected backend: memory event queues require a subclass of their memory-aware worker and Redis event queues require a subclass of their Redis-aware worker. Async-queue `HANDLER` metadata is invalid because listeners provide event dispatch. Memory event queues are local to that process. Redis workers use claims so processes compete for one active delivery, but ordering remains indeterminate across multiple listeners, processes, or retries. Use one listener in one process when strict ordering is required. An active Redis listener renews its claim while it runs; if its worker stops before settling the event, a later dispatcher recovers the expired claim for redelivery. The runtime retries an event dispatcher that stops from an infrastructure failure with bounded backoff.

### Async queue handlers and extensions

Each alias may optionally choose the concrete worker and entry types it uses:

```python
QUEUES = {
    "requests": {
        "BACKEND": "django_queue.backends.redis.RedisAsyncQueue",
        "LOCATION": "redis://redis:6379/12",
        "HANDLER": "myproject.queue_handlers.process_request",
        "WORKER": "myproject.workers.RequestWorker",
        "ENTRY_CLASS": "myproject.entries.RequestEntry",
    },
}
```

`HANDLER` must resolve to an asynchronous callable. `runqueues` imports each configured handler at startup, waits until that queue has work, then creates its configured worker and passes the handler to it. A queue without `HANDLER` remains producer-only until application code supplies another worker.

`WORKER` and `ENTRY_CLASS` each accept either a class object or a dotted import path. Each backend selects a provider-compatible default worker: memory async and event queues use memory-aware workers, while Redis async and event queues use Redis-aware workers that manage transport delivery internally. `AsyncQueueWorker` and `EventQueueWorker` are orchestration bases, not default workers for every backend. A configured async-queue worker must be compatible with its backend's selected worker type and use the normal queue-lookup and handler-mapping constructor. A queue constructs its worker with its own clock, so a subclass that overrides `__init__` must accept a `clock` keyword and pass it to `super().__init__`, or accept `**kwargs` and forward them. Django validates and imports entry and worker types during queue configuration. A worker is constructed only when its queue first becomes active; an entry only when it is enqueued, restored, or updated.

`RETENTION_TIMEOUT` controls how long terminal entry records remain available. A running worker removes expired terminal records during its normal loop. `prune(entry_id)` and `await aprune(entry_id)` remove one terminal record immediately.

Custom queue backends that support identified entry dispatch must implement `has_pending()`, returning whether `dequeue()` can immediately return an entry. They must also implement `aprune()` and `_aprune_expired()`: pruning rejects non-terminal entries, removes the durable record, and publishes an observer-only `terminated` snapshot. Workers publish an entry's initial lifecycle snapshot when they first observe it. Custom backends that emit Django's `entry_enqueued` signal must call `send_entry_enqueued()` after durable enqueue; that signal is separate from lifecycle observation. Built-in backends expose `queue_name`, their stable entry namespace.

## Usage

Within an application, data is added to the queue by using the `add` method:

Example

```python
from django_queue import queue
...
   queue.add({"some": "object", "with": "values"})
...
```

Priority queues require slightly different handling in that a priority should be set to determine the order in which messages are consumed and when added should be done as a `(priority, value)` tuple:

```python
from django_queue import queue
...
   queue.add((10, {"some": "object", "with": "values"}))
...
```

Multiple values can be added in the one `add()` call if required.

### Instants

`ClockTime` is how this package names a point in time: an immutable value holding whole seconds and microseconds since the Unix epoch. It exists so an instant and a duration cannot be confused — a duration is expressed as a plain count of seconds.

```python
from datetime import UTC, datetime

from django_queue import ClockTime

moment = datetime(2026, 8, 3, 23, 33, 20, 250_000, tzinfo=UTC)

instant = ClockTime.from_timestamp(1785800000.25)  # a count of seconds
instant = ClockTime.from_timeval(1785800000, 250_000)  # a Redis TIME pair
instant = ClockTime.from_datetime(moment)  # a timezone-aware datetime

instant.to_timestamp()  # 1785800000.25, the durable form
instant.to_datetime()  # an aware UTC datetime, for calendar work
```

Instants compare and order chronologically. Subtracting one from another gives the seconds between them, and adding or subtracting a count of seconds gives another instant, with the duration on either side:

```python
elapsed = finished - started  # a float count of seconds
later = started + 600.0  # a ClockTime
same = 600.0 + started  # the order of operands does not matter
started + finished  # TypeError: adding two instants means nothing
```

Construction rejects anything that cannot describe an instant. A component of
the wrong type raises `TypeError` — including a `bool`, which is an integer in Python but not a moment. A microsecond component outside `[0, 1000000)`, a naive datetime, a count of seconds that is NaN or infinite, or a time before the epoch raises `ValueError`. The epoch is a floor on arithmetic too: shifting back past it fails rather than yielding a negative time.

An instant does not convert to a number implicitly. `float(instant)` raises, and so does `json.dumps` on one, so a caller that wants a number asks for it.

In a Redis-backed environment the base instant used is that returned by the Redis server's TIME command, so that multiple consumers across systems (such as a horizontally scaled Django application) are effectively synced with the Redis server when it comes to internal handling of event times and durations.

### Identified lifecycle records

The lifecycle-record API is appropriate when a producer needs to poll the outcome of work processed later. Payloads and handler results must be JSON-serialisable. The queue generates the UUIDv7 identifier and owns all lifecycle timestamps, taking them from its own clock — Redis-aligned for a Redis queue, local time otherwise. That clock is available as `queue.clock`, so anything recording times alongside a queue's entries can share its basis.

`queued_at`, `dispatched_at` and `finished_at` are `ClockTime` values, stored as a float count of seconds since the epoch. Nothing parses a string or resolves a timezone to read one, and a stored instant is directly usable as a Redis sorted-set score.

Because those instants share one basis, an entry can report elapsed time directly:

```python
entry.queued_for  # seconds it waited before a worker picked it up
entry.ran_for  # seconds its handler took
```

Each is a count of seconds carrying its microseconds, not a whole number of them — a handler that ran for 137 microseconds reports `0.000137`, which matters because most work finishes in well under a second.

Both are derived from the instants rather than stored, so they cannot disagree with them, and both are `None` until the instants describing them exist — an entry still waiting has not waited zero seconds. They are also `None` if the instants contradict, which a clock recalibrating backwards can cause: a negative elapsed time is meaningless rather than merely small.

### Asynchronous queue API and heartbeat

The `a`-prefixed lifecycle operations are the primary API in asynchronous code: `aenqueue`, `afind`, `alist`, `adequeue`, and `ahas_pending`. All must be `await`ed when called.

Lifecycle transitions are worker-internal operations, not producer APIs. AsyncQueue backends also expose `aprune` for explicit retained-entry removal. Built-in queues also expose `aadd`, `aget`, `apoll`, `apeek`, `asize`, and `aclear` for raw queue values. Await these from an ASGI view, a handler, or another coroutine:

```python
entry_id = await queue.aenqueue({"request_id": 42})
entry = await queue.afind(entry_id)
```

The corresponding synchronous methods remain for synchronous Django code. They must not be called from a running event loop; use the `a`-prefixed operation instead. A custom backend implements the asynchronous methods, while the base class supplies the synchronous wrappers. `len(queue)`, `bool(queue)`, and `is_empty()` are likewise synchronous-only; use `asize()` or `ais_empty()` in an event loop. Synchronous wrapper calls release their bridge-loop resources after each operation, so a custom backend's `aclose()` must be idempotent.

`runqueues` disposes its queues on its owning event loop. Other async hosts must await `aclose_queues()` before closing that loop; `close_queues()` only serves synchronous-wrapper resources and cannot close a different loop's Redis client.

For a Redis backend, a synchronous queue operation uses a fresh bridge-loop connection and Redis `TIME` calibration before closing it. Prefer the async API in asynchronous or high-volume producer code, where the loop-local client and clock are reused.

Long-running handlers may call `heartbeat()` after genuine progress to restart their current execution budget as they approach its deadline:

```python
from django_queue import heartbeat


async def process_request(entry):
    await store_progress(entry.payload)
    heartbeat()
    return {"processed": True}
```

Heartbeat extends only the local execution budget. It is neither a lease renewal nor an ownership or delivery guarantee; a later claim-and-recovery backend may add those guarantees. It is not a keepalive to call on a timer or in a loop: doing so disables the protection the budget provides.

```python
from django_queue import queue

entry_id = queue.enqueue({"request_id": 42})
entry = queue.find(entry_id)

assert entry.status == "queued"
```

### Lifecycle observation

Use `queue_observer` for best-effort, passive async-queue monitoring. A subscription receives immutable entry snapshots from an async-queue worker; it cannot affect async-queue execution.

```python
from django_queue import queue_observer


def update_dashboard(entry):
    print(entry.id, entry.status)


subscription = queue_observer("default", update_dashboard)
subscription.unsubscribe()  # stop future local delivery
```

`queue_observer` also works as a decorator, useful for registering an observer at import time without triggering any backend I/O immediately:

```python
from django_queue import queue_observer


@queue_observer("default")
def update_dashboard(entry):
    print(entry.id, entry.status)
```

A decorator-registered observer records its registration immediately but activates — fetching retained snapshots and beginning delivery — only once the process-wide queue runtime starts. `update_dashboard._queue_observer_subscription` is usable immediately, before or after activation, to unsubscribe.

Memory queues notify only within the same Django process. Redis queues use best-effort Pub/Sub: a disconnected observer can miss transitions. Register a new observer when a new retained-state bootstrap is needed. Observer callback failures are logged and do not affect queue processing. Each observed queue's local delivery queue holds up to 128 snapshots; later snapshots are dropped when it is full, with one warning logged for that queue's process-local lifetime.

When a worker receives an entry, it first publishes that entry's persisted `queued` snapshot, then publishes `running` and its terminal state after each state is stored. A running worker also scans retained entries once per second and publishes snapshots it has not previously seen, using the queue-owned UUIDv7 IDs as its cursor. This makes entries changed outside the worker's own dispatch path observable; when the entry is later dispatched, the cursor avoids republishing its queued snapshot. An entry awaiting a worker remains available in the retained snapshots delivered at subscription.

Retention cleanup and explicit pruning remove a terminal record and publish one final immutable entry-shaped snapshot to its observers with `status == "terminated"`. This final snapshot is never persisted as a retained record, although `terminated` is the final lifecycle state after any completed state. Dashboards can use it to remove the entry from their projection. A later `find()` or `afind()` for the removed ID raises `QueueEntryNotFoundError`.

All configured async-queue workers and Redis observer receivers share one process-wide background thread and asyncio loop (`QueueRuntime`), started once when the process comes up. A Redis-backed alias's receiver task begins as soon as that alias has its first observer registration, and is shared by every later registration for the same alias — no matter how many threads or queue instances register against it. It blocks in Pub/Sub while idle rather than polling, consumes no CPU while it waits, and does not keep Django alive during shutdown. If it exits because Redis fails, it logs the failure and clears its receiver task; existing observer registrations stay in place but stop receiving live snapshots until a later registration for that alias starts a fresh receiver task.

An entry normally transitions through `queued`, `running`, and one completed
status: `succeeded`, `failed`, or `timeout`. Each completed status transitions to `terminated` when pruning removes its retained record. Failed entries expose only an exception type and safe message; the worker logs the traceback for diagnosis. A fourth completed status, `cancelled`, exists on the backend contract but no worker path produces it: a handler that finishes during shutdown is recorded by what it returned, and one that overruns is recorded as `timeout`. It is reserved for a deliberate per-entry cancellation the queue does not yet offer.

`failed` may also be recorded directly from `queued` when dispatch cannot begin, for example after queue-side validation or transport failure. Such an entry has `finished_at` but no `dispatched_at`.

### Asynchronous worker

An application or management command explicitly owns the worker task. It must not be started from a request handler or Django app initialisation hook.

```python
import asyncio

from django_queue import AsyncQueueWorker, queues


async def process_request(entry):
    return {"processed": entry.payload["request_id"]}


worker = queues["default"].create_worker("default", process_request)
asyncio.run(worker.run())
```

The worker dispatches one entry at a time and runs until cancelled. On cancellation it stops accepting new entries, gives an active handler its configured grace period, then cancels it if needed.

Redis queues use leased claims for at-least-once delivery. A worker claims an
entry, renews its lease while dispatching, and atomically settles its terminal entry outcome only while it still owns that claim. Expired claims return the same entry ID to pending work, so a process failure can cause the handler to execute more than once. Handlers that make external changes must therefore be idempotent. Queue backends without claim-lease support retain best-effort delivery.

Claim, renewal, acknowledgement, recovery, and settlement are Redis delivery operations, owned by the Redis worker and its private queue provider rather than the public queue API. Other transports may use a different native model. Redis keys, Functions, timestamps, and record layout are not public contract. Redis Cluster is not supported by the Redis delivery implementation.

If a terminal outcome cannot be persisted because of an infrastructure failure, the worker logs the failure and continues. When it can still read a `running` entry, it makes one best-effort attempt to record a safe `QueuePersistenceError` failure outcome. If it cannot confirm either terminal outcome, the worker raises `QueuePersistenceError` rather than accepting further entries.

Loss of claim ownership is different: the worker stops handling that entry without recording an outcome, then continues serving later work. Recovery or the worker that acquired the claim owns the retry and its terminal outcome.

### Execution budgets

Every dispatch runs under a budget: a count of seconds after which the worker stops waiting, cancels the handler, and records the entry as `timeout`. The worker then moves to the next entry, so one handler that never returns cannot starve an alias. A budget is always in force — an unbounded handler is the defect the budget exists to remove — so there is no value meaning unlimited.

The budget is resolved per dispatch, taking the first of these that is set:

1. the worker's `timeout_seconds` override, which applies to every alias it serves
2. the entry's own `timeout_seconds`, set when it was enqueued
3. the alias's `TIMEOUT` setting
4. 600 seconds

```python
QUEUES = {
    "default": {
        "BACKEND": "django_queue.backends.redis.RedisAsyncQueueJson",
        "LOCATION": "redis://localhost:6379/12",
        "TIMEOUT": 30,
    },
}
```

`TIMEOUT` is a finite positive number of seconds, validated when settings are initialised rather than at first dispatch, so a bad value fails at startup. The same rule applies wherever a budget is supplied — the setting, the `enqueue` keyword, and the worker override all reject a non-number with `TypeError` and a zero, negative, infinite or NaN value with `ValueError`, at the point it is supplied. There is no value meaning unlimited, infinity included.

A single piece of work that legitimately takes longer carries its own budget:

```python
entry_id = queue.enqueue({"request_id": 42}, timeout_seconds=120)
```

The budget expires on the event loop's monotonic clock, while the entry's timestamps are read from the queue's own clock. They are deliberately independent: the budget decides when to stop, and the entry records what happened. A timed-out entry's `ran_for` is a wall-clock measurement and will not equal the budget that expired.

Custom entry-capable backends must implement the worker-internal `_amark_timed_out(entry_id)` alongside the other terminal transitions, moving a `running` entry to `timeout` and setting `finished_at`.

The shutdown grace period is separate from the budget: it bounds how long a cancelled worker waits for an active handler, and its expiry is also recorded as `timeout`.

A handler that raises `TimeoutError` of its own — from `asyncio.wait_for`, an HTTP client, or a database driver — is recorded `failed` with that error, not `timeout`. Only the budget actually running out means the handler never answered.

### Entry priority

`priority` is an `int` on `QueueEntry`, defaulting to `0`; a higher value dispatches first. It is only consulted by the priority-variant backends (`MemoryAsyncPriorityQueue`, `RedisAsyncPriorityQueue`, `RedisAsyncPriorityQueueJson`) on the identified-entry path — `enqueue`/`aenqueue` accept it as a keyword:

```python
entry_id = queue.enqueue({"request_id": 42}, priority=10)
```

Non-priority backends and event queues accept the keyword but ignore it, dispatching in their own existing order regardless. Equal-priority entries dispatch in arrival order on every priority backend.

### Scheduled availability

Identified async queues also accept `available_at`, a `ClockTime` absolute
instant at which work becomes eligible for ordinary dispatch:

```python
from django_queue.clock import ClockTime

entry_id = queue.enqueue(
    {"request_id": 42}, available_at=ClockTime.from_datetime(run_after)
)
```

An omitted or already-passed instant dispatches normally. A future instant is
retained as queued work but does not reserve a worker; the Redis backend uses
its server clock to promote due work, while the memory backend uses its queue
clock. This is designed for upstream task APIs to translate their absolute
schedule instant directly. Event queues accept `available_at` for API
compatibility and ignore it.

`QueueEntry` itself accepts any `int` — it does not know which backend an entry is destined for, and a non-priority backend must be free to ignore the value entirely, so it never rejects one on a priority backend's behalf. The Redis priority backend (`RedisAsyncPriorityQueue`, `RedisAsyncPriorityQueueJson`) packs `priority` and an arrival-order sequence number into one ZSET score, which is only exact up to a double's 53-bit integer range; that backend rejects a `priority` beyond ±100,000 with `ValueError` when the entry is actually pushed to its tracked pending store, keeping every score comfortably inside the exact range. The in-memory priority backend (`MemoryAsyncPriorityQueue`) has no such bound — Python integers are arbitrary precision.

### Worker observability

Each `AsyncQueueWorker` has a generated UUIDv7 identity and exposes a frozen, process-local `snapshot`. It reports the current run state, registered queue aliases, active queue name and entry ID, total dispatches, and confirmed persisted terminal outcomes:

```python
from django_queue import WorkerSnapshot

snapshot: WorkerSnapshot = worker.snapshot
health = {
    "worker_id": str(snapshot.worker_id),
    "running": snapshot.running,
    "queue": snapshot.active_queue_name,
    "succeeded": snapshot.succeeded_count,
}
```

The worker emits INFO lifecycle records with `queue_worker_event` set to `started`, `dispatch_started`, `terminal_recorded`, or `stopped`. Their structured fields are prefixed with `queue_worker_` and include the same worker ID, running state, registered queue aliases, active queue name and entry ID, start time, dispatch count, and outcome counters as the snapshot. Counters advance only after the corresponding terminal entry state has been confirmed in the backend. `timed_out_count` is counted separately from `cancelled_count`, so a handler abandoned on its budget is distinguishable from one the queue was told to cancel.

`started_at` comes from the worker's clock, which its queue supplies when it creates the worker, so a worker's recorded time and the entries it dispatches share one basis and elapsed time across them is meaningful. A worker built directly defaults to local time and accepts a `clock` argument. Like every instant the package reports it is a `ClockTime`, rendered in structured log records as a count of seconds rather than an ISO string.

`running_for` reports how long the worker has been running, measured on that same clock, and stops advancing once the worker leaves its dispatch loop so it reports how long it ran. Structured records carry it, and a terminal outcome record also carries the entry's `queued_for` and `ran_for`.

Reading a running worker's snapshot samples the queue clock to measure `running_for`, so on a Redis-backed queue a snapshot read takes the clock's lock and may trigger its periodic recalibration. A stopped worker reads its recorded stop instant instead and touches no clock. Read snapshots from the worker's event loop for a consistent observation; they do not coordinate cross-thread reads. A shutdown can interrupt the worker's acknowledgement of an in-flight terminal write, so the final snapshot records only terminal outcomes the worker observed before it stopped.

Snapshots and log records are local to the worker process. Collect logs or add an exporter in application infrastructure to aggregate multiple `runqueues` or web processes; this package does not provide distributed liveness or metrics.

### External `runqueues` worker

For production, run queue processing as a separate Django process and use a shared backend such as Redis. Declare an asynchronous handler on each queue that the process should dispatch:

```python
# settings.py
QUEUES = {
    "requests": {
        "BACKEND": "django_queue.backends.redis.RedisAsyncQueueJson",
        "LOCATION": "redis://redis:6379/12",
        "HANDLER": "myproject.queue_handlers.process_request",
    },
}


# myproject/queue_handlers.py
async def process_request(entry):
    return {"processed": entry.payload["request_id"]}
```

Start it as its own service or container command:

```console
python manage.py runqueues
```

`runqueues` validates every configured `HANDLER` and `WORKER`, exiting non-zero on a configuration error, then waits to create each configured worker until that alias has pending entry work. It reports the configured handler count at startup and each alias as its worker begins. Once started, a worker runs until it receives `SIGINT` or `SIGTERM`; shutdown cooperatively stops all active workers. Queue definitions without `HANDLER` remain available to application code but are not dispatched; when no handlers are configured, the command reports this and exits successfully. A worker failure is logged while the remaining queues stay watched; the command exits non-zero only when no configured queue is left.

With all queues, the `get()`, `peek()`, and `poll()` methods return the object. Priority queue backends honour priority on both APIs: the raw value API via `add()`'s `(priority, value)` tuple, and identified entries via `enqueue()`/`aenqueue()`'s `priority` keyword — see [Entry priority](#entry-priority).

## API reference

All queues expose raw-value operations. `stack`, `capacity`, and `queue_name` describe the selected backend; `len(queue)` and `bool(queue)` are synchronous conveniences.

| Raw-value operation | Meaning |
| --- | --- |
| `add` / `aadd` | Add one or more raw values. Priority queues accept `(priority, value)` values. |
| `get` / `aget` | Remove and return the next raw value. |
| `poll` / `apoll` | Wait for and remove the next raw value. Redis priority queues accept `timeout` and `retries`. |
| `peek` / `apeek` | Return the next raw value without removing it. |
| `size` / `asize`, `is_empty` / `ais_empty` | Inspect raw-value availability. |
| `clear` / `aclear`, `close` / `aclose` | Clear raw values or release queue resources. |

### AsyncQueue lifecycle API

Only `AsyncQueue` implementations retain lifecycle records. Their synchronous methods have `a`-prefixed async counterparts.

| Operation | Meaning |
| --- | --- |
| `enqueue` / `aenqueue` | Create a durable queued record and return its UUIDv7 ID. |
| `find` / `afind` | Return one retained record by ID. |
| `dequeue` / `adequeue` | Remove the next pending record from delivery while retaining its record. |
| `has_pending` / `ahas_pending` | Report whether delivery work is available. |
| `list` / `alist` | Return retained records for administration or observer bootstrap. |
| `prune` / `aprune` | Remove one retained terminal record and publish its observer-only `terminated` state. |

Lifecycle transitions are worker-internal. `enqueue` emits Django's `entry_enqueued` signal after durable storage; lifecycle observers receive records when workers first observe them and as their state changes.

### EventQueue delivery API

`EventQueue` uses `enqueue` / `aenqueue` to create a transient event, `find` / `afind` to inspect one live event, and `dequeue` / `adequeue` for direct consumption. It deliberately has no `list` or `prune` lifecycle API: event records are consumed or expire without a terminal outcome, and listeners receive them through `@queue_listener`.

### Exceptions

- `InvalidQueueBackendError`: invalid Django `QUEUES` configuration.
- `QueueFullException` and `QueueEmptyException`: raw-value capacity or
  availability errors.
- `QueueEntryNotFoundError`: `find` or `prune` requested a retained record
  that no longer exists.
- `QueueEntryMissingError`: an internal worker/provider recovery condition:
  a previously claimed record disappeared unexpectedly.
- `QueueEncodingException` and `QueueValueError`: invalid stored values.
