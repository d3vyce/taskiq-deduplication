"""Redis-backed deduplication middleware for Taskiq."""

from .middleware import DuplicateTaskError, RedisDeduplicationMiddleware

__version__ = "1.0.2"

__all__ = [
    "DuplicateTaskError",
    "RedisDeduplicationMiddleware",
]
