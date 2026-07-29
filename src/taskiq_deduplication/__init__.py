"""Redis-backed deduplication middleware for Taskiq."""

from .middleware import DuplicateTaskError, RedisDeduplicationMiddleware

__version__ = "1.2.0"

__all__ = [
    "DuplicateTaskError",
    "RedisDeduplicationMiddleware",
]
