"""Redis-backed deduplication middleware for Taskiq."""

from .middleware import DuplicateTaskError, RedisDeduplicationMiddleware
from .schedule import RedisDeduplicationScheduleSource

__version__ = "1.1.0"

__all__ = [
    "DuplicateTaskError",
    "RedisDeduplicationMiddleware",
    "RedisDeduplicationScheduleSource",
]
