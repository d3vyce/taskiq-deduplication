"""FastAPI utilities package."""

from .middleware import DuplicateTaskError, RedisDeduplicationMiddleware

__version__ = "3.1.1"

__all__ = [
    "DuplicateTaskError",
    "RedisDeduplicationMiddleware",
]
