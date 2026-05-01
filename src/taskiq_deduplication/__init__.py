"""FastAPI utilities package."""

from .middleware import DuplicateTaskError, RedisDeduplicationMiddleware
from .utils import check_and_delete

__version__ = "3.1.1"

__all__ = [
    "DuplicateTaskError",
    "RedisDeduplicationMiddleware",
    "check_and_delete",
]
