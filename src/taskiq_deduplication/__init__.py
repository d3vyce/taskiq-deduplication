"""FastAPI utilities package."""

from .middleware import DuplicateTaskError, RedisDeduplicationMiddleware

__version__ = "1.0.0"

__all__ = [
    "DuplicateTaskError",
    "RedisDeduplicationMiddleware",
]
