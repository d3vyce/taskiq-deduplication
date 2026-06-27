# Usage

## Setup

Register `RedisDeduplicationMiddleware` on your broker before the application starts:

```python
from taskiq_redis import ListQueueBroker
from taskiq_deduplication import RedisDeduplicationMiddleware

broker = ListQueueBroker("redis://localhost:6379").with_middlewares(
    RedisDeduplicationMiddleware(redis_url="redis://localhost:6379"),
)
```

## Middleware options

| Parameter | Type | Default | Description |
|---|---|---|---|
| `redis_url` | `str \| RedisDsn` | — | Redis connection URL passed to `Redis.from_url`. Accepts a plain string or a pydantic [`RedisDsn`](https://docs.pydantic.dev/latest/api/networks/#pydantic.networks.RedisDsn). |
| `default_deduplication` | `bool` | `True` | Whether deduplication is enabled for all tasks by default. Set `False` to opt-in per task instead of opting out. |
| `default_ttl` | `int` | `300` | Default lock TTL in seconds. Overridden per task with the `deduplication_ttl` label. |
| `key_prefix` | `str` | `"taskiq:deduplication"` | Prefix for all Redis lock keys. |
| `startup_retries` | `int` | `3` | Number of connection attempts during broker startup. |
| `startup_retry_delay` | `float` | `1.0` | Base delay in seconds between retries (exponential backoff: delay × 2^n). |
| `heartbeat` | `bool` | `True` | Whether to periodically re-extend the lock TTL while the task runs (see [Long-running tasks](#long-running-tasks-and-the-heartbeat)). |
| `heartbeat_interval` | `float \| None` | `None` | Seconds between heartbeat refreshes. When `None`, defaults to a third of the task's TTL (1s floor). |

```python
broker = ListQueueBroker("redis://localhost:6379").with_middlewares(
    RedisDeduplicationMiddleware(
        redis_url="redis://localhost:6379",
        default_deduplication=True,
        default_ttl=60,
        key_prefix="myapp:dedup",
        startup_retries=5,
        startup_retry_delay=0.5,
    ),
)
```

## Long-running tasks and the heartbeat

The lock is created with a TTL so a crashed worker cannot leak it forever. Without
any refresh, a task that runs longer than its TTL would let the lock expire
**mid-execution**, allowing a duplicate to be dispatched.

To prevent this, the middleware starts a background **heartbeat** in `pre_execute`
that re-extends the lock TTL while the task runs (atomically, only if the lock is
still owned by the running task). It is cancelled when the task completes or fails.
This means you do **not** need to size `default_ttl` to your slowest task — the TTL
only needs to outlive a single heartbeat interval; it acts purely as a safety net
for worker crashes.

```python
RedisDeduplicationMiddleware(
    redis_url="redis://localhost:6379",
    default_ttl=60,        # safety-net TTL; refreshed every ~20s while running
    heartbeat_interval=20, # optional; defaults to default_ttl / 3
)
```

If you disable the heartbeat (`heartbeat=False`), the invariant **TTL must exceed
the slowest task** applies: set `default_ttl` (or the per-task `deduplication_ttl`
label) above your worst-case task duration, or duplicates may slip through.

## Startup resilience

On startup the middleware verifies the Redis connection with a `PING`. If Redis is
temporarily unavailable, it retries with exponential backoff.
After all attempts are exhausted a `ConnectionError` is raised and the broker
fails to start.

Adjust `startup_retries` and `startup_retry_delay` to suit your deployment:

```python
RedisDeduplicationMiddleware(
    redis_url="redis://localhost:6379",
    startup_retries=5,
    startup_retry_delay=2.0,
)
```

## How it works

When a task is dispatched, the middleware acquires a Redis lock keyed on the task's
fingerprint. Any subsequent dispatch with the same fingerprint raises
`DuplicateTaskError` while the lock is held. The lock is released automatically when
the task completes or fails.

## Handling duplicates

When a duplicate is detected, the middleware logs a warning and raises
`DuplicateTaskError`, which prevents the task from reaching the broker.
Catch it at the call site if you need to handle it explicitly:

```python
from taskiq_deduplication import DuplicateTaskError

try:
    await my_task.kiq(user_id=42)
except DuplicateTaskError:
    pass  # task is already queued or running
```

`DuplicateTaskError` carries structured attributes describing the collision:

```python
try:
    await my_task.kiq(user_id=42)
except DuplicateTaskError as err:
    logger.info(
        "Skipped %s; already held by %s (key=%s)",
        err.task_name,
        err.holder_task_id,
        err.key,
    )
```

- `task_name` — name of the task that was rejected.
- `key` — Redis lock key whose owner caused the rejection.
- `holder_task_id` — `task_id` of the task currently holding the lock, or `None`
  if it could not be retrieved.

## Per-task label overrides

Labels can be set at the task level (applied to every call) or at call time.

### Task-level (decorator)

```python
@broker.task(deduplication_ttl=60)
async def my_task(user_id: int) -> None:
    ...
```

### Call-level (kicker)

```python
await my_task.kicker().with_labels(deduplication_ttl=60).kiq(user_id=42)
```

### Available labels

| Label | Type | Description |
|---|---|---|
| `deduplication` | `bool` | Set `False` to opt out of deduplication entirely for this task. |
| `deduplication_ttl` | `int` | Lock TTL in seconds. Overrides the middleware `default_ttl`. |
| `deduplication_key` | `str` | Explicit lock key. Skips fingerprint computation entirely. |
| `deduplication_key_fields` | `list[str]` | Subset of kwargs to include in the fingerprint. Ignored if `deduplication_key` is set. |

## Fingerprint and key customisation

By default the lock key is a SHA-256 fingerprint of the task name and all kwargs.

### Explicit key

Use `deduplication_key` when you want full control over the lock key, regardless of
the kwargs:

```python
@broker.task(deduplication_key="send-welcome-email")
async def send_welcome_email(user_id: int, locale: str) -> None:
    ...
```

All calls to this task share a single lock, no matter what arguments are passed.

### Partial key (key fields)

Use `deduplication_key_fields` to deduplicate only on a subset of kwargs.
Here, two calls with the same `user_id` but different `locale` are treated as
duplicates:

```python
@broker.task(deduplication_key_fields=["user_id"])
async def send_welcome_email(user_id: int, locale: str) -> None:
    ...
```

If a listed field is absent from a task's kwargs, it is dropped from the
fingerprint and a warning is logged, since this can make genuinely different
calls collide on the same lock.

## Opting out per task

```python
@broker.task(deduplication=False)
async def always_run(payload: str) -> None:
    ...
```
