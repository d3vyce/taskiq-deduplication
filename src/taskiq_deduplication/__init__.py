"""FastAPI utilities package."""

from .middleware import DuplicateTaskError, RedisDeduplicationMiddleware

__version__ = "1.0.1"

__all__ = [
    "DuplicateTaskError",
    "RedisDeduplicationMiddleware",
]
