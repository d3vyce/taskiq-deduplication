# Taskiq Deduplication

Redis-backed deduplication middleware for Taskiq that prevents duplicate tasks from being queued or executed concurrently.

[![CI](https://github.com/d3vyce/taskiq-deduplication/actions/workflows/ci.yml/badge.svg)](https://github.com/d3vyce/taskiq-deduplication/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/d3vyce/taskiq-deduplication/graph/badge.svg)](https://codecov.io/gh/d3vyce/taskiq-deduplication)
[![ty](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ty/main/assets/badge/v0.json)](https://github.com/astral-sh/ty)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

**Documentation**: [https://taskiq-deduplication.d3vyce.fr](https://taskiq-deduplication.d3vyce.fr)

**Source Code**: [https://github.com/d3vyce/taskiq-deduplication](https://github.com/d3vyce/taskiq-deduplication)

---

## Installation

```bash
uv add "taskiq-deduplication"
```

## Quick Start

```python
from taskiq_redis import ListQueueBroker
from taskiq_deduplication import RedisDeduplicationMiddleware, DuplicateTaskError

broker = ListQueueBroker("redis://localhost:6379").with_middlewares(
    RedisDeduplicationMiddleware(redis_url="redis://localhost:6379"),
)

@broker.task
async def send_report(user_id: int) -> None:
    ...

# First dispatch acquires the lock — succeeds.
await send_report.kiq(user_id=42)

# Second dispatch while the first is queued or running — raises.
try:
    await send_report.kiq(user_id=42)
except DuplicateTaskError:
    pass  # already queued or running
```

## Features

- **Sender-side deduplication** — rejects duplicate tasks at dispatch time via a Redis lock, before they reach the broker.
- **Atomic lock release** — lock is released on completion or error via a Lua check-and-delete; only the owning task can release its lock.
- **Configurable TTL** — set a global default or override per task with the `deduplication_ttl` label.
- **Explicit lock key** — pin any task to a fixed Redis key with `deduplication_key`, bypassing fingerprint computation entirely.
- **Partial fingerprint** — deduplicate on a subset of kwargs with `deduplication_key_fields`, ignoring irrelevant arguments.
- **Per-task opt-out** — disable deduplication for individual tasks with the `deduplication` label.
- **Startup resilience** — automatic reconnection with exponential backoff if Redis is unavailable at broker startup.

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.
