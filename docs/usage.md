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
| `redis_url` | `str` | — | Redis connection URL passed to `Redis.from_url`. |
| `default_deduplication` | `bool` | `True` | Whether deduplication is enabled for all tasks by default. Set `False` to opt-in per task instead of opting out. |
| `default_ttl` | `int` | `300` | Default lock TTL in seconds. Overridden per task with the `deduplication_ttl` label. |
| `key_prefix` | `str` | `"taskiq:deduplication"` | Prefix for all Redis lock keys. |

```python
broker = ListQueueBroker("redis://localhost:6379").with_middlewares(
    RedisDeduplicationMiddleware(
        redis_url="redis://localhost:6379",
        default_deduplication=True,
        default_ttl=60,
        key_prefix="myapp:dedup",
    ),
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

## Opting out per task

```python
@broker.task(deduplication=False)
async def always_run(payload: str) -> None:
    ...
```
