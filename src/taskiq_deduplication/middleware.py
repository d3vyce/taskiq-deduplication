import hashlib
import json
import logging
from typing import Any

from redis.asyncio import Redis
from taskiq import TaskiqMessage, TaskiqResult
from taskiq.abc.middleware import TaskiqMiddleware

from .utils import check_and_delete

logger = logging.getLogger(__name__)

DEDUP_LABEL = "deduplication"
DEDUP_TTL_LABEL = "deduplication_ttl"
DEDUP_KEY_FIELDS_LABEL = "deduplication_key_fields"
DEDUP_EXPLICIT_KEY_LABEL = "deduplication_key"


class DuplicateTaskError(Exception):
    """Raised when a task with identical name and kwargs is already queued or running."""


class RedisDeduplicationMiddleware(TaskiqMiddleware):
    """Prevents duplicate tasks from being queued.

    When a task is dispatched, a Redis lock is acquired for the duration of its
    execution. Any subsequent task with the same fingerprint is rejected with
    ``DuplicateTaskError`` while the lock is held. The lock is released automatically
    on completion or error.

    Attributes:
        redis_url: Redis connection URL passed to ``Redis.from_url``.
        default_deduplication: Whether deduplication is enabled by default.
        default_ttl: Default lock TTL in seconds.
        key_prefix: Prefix for all Redis lock keys.
    """

    def __init__(
        self,
        redis_url: str,
        default_deduplication: bool = True,
        default_ttl: int = 300,
        key_prefix: str = "taskiq:deduplication",
    ) -> None:
        self.redis_url = redis_url
        self.default_deduplication = default_deduplication
        self.default_ttl = default_ttl
        self.key_prefix = key_prefix
        self._redis: Redis | None = None

    async def startup(self) -> None:
        self._redis = Redis.from_url(self.redis_url)

    async def shutdown(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()

    def _build_deduplication_key(self, message: TaskiqMessage) -> str | None:
        explicit_key: str | None = message.labels.get(DEDUP_EXPLICIT_KEY_LABEL)
        if explicit_key is not None:
            return f"{self.key_prefix}:{explicit_key}"

        key_fields: list[str] | None = message.labels.get(DEDUP_KEY_FIELDS_LABEL)
        kwargs = (
            {k: v for k, v in message.kwargs.items() if k in key_fields}
            if key_fields is not None
            else message.kwargs
        )
        try:
            payload = json.dumps(
                {"task": message.task_name, "kwargs": kwargs},
                sort_keys=True,
            )
        except TypeError:
            return None
        fingerprint = hashlib.sha256(payload.encode()).hexdigest()[:16]
        return f"{self.key_prefix}:{fingerprint}"

    def _is_enabled(self, labels: dict[str, Any]) -> bool:
        return bool(labels.get(DEDUP_LABEL, self.default_deduplication))

    def _get_ttl(self, labels: dict[str, Any]) -> int:
        return int(labels.get(DEDUP_TTL_LABEL, self.default_ttl))

    async def _release_if_owned(self, key: str, task_id: str) -> None:
        if self._redis is None:
            raise RuntimeError(
                "RedisDeduplicationMiddleware.startup() was never called."
            )
        released = await check_and_delete(self._redis, key, task_id)
        if released:
            logger.debug("Released lock %s", key)
        else:
            logger.debug("Skipped release of lock %s: not owned by this task", key)

    async def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
        if not self._is_enabled(message.labels):
            return message

        if self._redis is None:
            raise RuntimeError(
                "RedisDeduplicationMiddleware.startup() was never called."
            )
        key = self._build_deduplication_key(message)
        if key is None:
            logger.warning(
                "Task %s has non-JSON-serializable kwargs; deduplication skipped."
                " Use the deduplication_key label to deduplicate this task.",
                message.task_name,
            )
            return message
        ttl = self._get_ttl(message.labels)

        logger.debug("Acquiring lock %s for task %s", key, message.task_name)
        acquired = await self._redis.set(key, message.task_id, ex=ttl, nx=True)
        if not acquired:
            logger.warning(
                "Duplicate task %s dropped (key=%s).",
                message.task_name,
                key,
            )
            raise DuplicateTaskError(
                f"Task {message.task_name!r} with the same arguments is already queued or running."
            )

        logger.debug("Lock %s acquired for task %s", key, message.task_name)
        return message

    async def post_execute(
        self,
        message: TaskiqMessage,
        result: TaskiqResult,
    ) -> None:
        if not self._is_enabled(message.labels):
            return
        key = self._build_deduplication_key(message)
        if key is None:
            return
        await self._release_if_owned(key, message.task_id)

    async def on_error(
        self,
        message: TaskiqMessage,
        result: TaskiqResult,
        exception: BaseException,
    ) -> None:
        if not self._is_enabled(message.labels):
            return
        key = self._build_deduplication_key(message)
        if key is None:
            return
        await self._release_if_owned(key, message.task_id)
