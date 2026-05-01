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
    """
    Prevents duplicate tasks from being queued or executed concurrently.

    Two protection layers:
    - Sender-side (pre_send): sets a queue lock in Redis. A second task with
      the same fingerprint is rejected while the lock is held.
    - Worker-side (pre_execute): observational only — logs concurrent duplicate
      executions but never raises, avoiding retry storms with SmartRetryMiddleware.
      Tasks must be idempotent to rely on this layer for correctness.

    Per-task label overrides (each maps to a same-named default_* init parameter):
        deduplication: bool              — set False to opt out entirely
        deduplication_ttl: int           — lock TTL in seconds
        deduplication_key: str           — explicit lock key, skips fingerprint computation
        deduplication_key_fields: list[str] — kwargs to include in the fingerprint
                                             (ignored if deduplication_key is set)

    Set raise_on_duplicate=False at init to log sender-side duplicates instead
    of raising — the duplicate task is still sent to the broker in that mode.
    """

    def __init__(
        self,
        redis_url: str,
        default_deduplication: bool = True,
        default_ttl: int = 300,
        key_prefix: str = "taskiq:deduplication",
        raise_on_duplicate: bool = True,
    ) -> None:
        self.redis_url = redis_url
        self.default_deduplication = default_deduplication
        self.default_ttl = default_ttl
        self.key_prefix = key_prefix
        self.raise_on_duplicate = raise_on_duplicate
        self._redis: Redis | None = None

    async def startup(self) -> None:
        self._redis = Redis.from_url(self.redis_url)

    async def shutdown(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()

    def default_build_deduplication_key(self, message: TaskiqMessage) -> str:
        explicit_key: str | None = message.labels.get(DEDUP_EXPLICIT_KEY_LABEL)
        if explicit_key is not None:
            return f"{self.key_prefix}:{explicit_key}"

        key_fields: list[str] | None = message.labels.get(DEDUP_KEY_FIELDS_LABEL)
        kwargs = (
            {k: v for k, v in message.kwargs.items() if k in key_fields}
            if key_fields is not None
            else message.kwargs
        )
        payload = json.dumps(
            {"task": message.task_name, "kwargs": kwargs},
            sort_keys=True,
        )
        fingerprint = hashlib.sha256(payload.encode()).hexdigest()[:16]
        return f"{self.key_prefix}:{fingerprint}"

    def _exec_key(self, base_key: str) -> str:
        return f"{base_key}:exec"

    def _is_enabled(self, labels: dict[str, Any]) -> bool:
        return bool(labels.get(DEDUP_LABEL, self.default_deduplication))

    def _get_ttl(self, labels: dict[str, Any]) -> int:
        return int(labels.get(DEDUP_TTL_LABEL, self.default_ttl))

    async def _release_if_owned(self, key: str, task_id: str, description: str) -> None:
        assert self._redis is not None
        released = await check_and_delete(self._redis, key, task_id)
        if released:
            logger.debug("Released %s %s", description, key)
        else:
            logger.debug(
                "Skipped release of %s %s: not owned by this task", description, key
            )

    async def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
        if not self._is_enabled(message.labels):
            return message

        assert self._redis is not None
        key = self.default_build_deduplication_key(message)
        ttl = self._get_ttl(message.labels)

        logger.debug("Acquiring queue lock %s for task %s", key, message.task_name)
        acquired = await self._redis.set(key, message.task_id, ex=ttl, nx=True)
        if not acquired:
            logger.info(
                "Duplicate task %s deduplicated at send time (key=%s).",
                message.task_name,
                key,
            )
            if self.raise_on_duplicate:
                raise DuplicateTaskError(
                    f"Task {message.task_name!r} with the same arguments is already queued or running."
                )
            logger.warning("Sending duplicate task %s anyway.", message.task_name)
        else:
            logger.debug("Queue lock %s acquired for task %s", key, message.task_name)

        return message

    async def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        if not self._is_enabled(message.labels):
            return message

        assert self._redis is not None
        key = self._exec_key(self.default_build_deduplication_key(message))
        ttl = self._get_ttl(message.labels)

        logger.debug("Acquiring execution lock %s for task %s", key, message.task_name)
        acquired = await self._redis.set(key, message.task_id, ex=ttl, nx=True)
        if acquired:
            logger.debug(
                "Execution lock %s acquired for task %s", key, message.task_name
            )
        else:
            # Never raise here: raising in pre_execute with retry_on_error=True causes
            # SmartRetryMiddleware to retry repeatedly against the same live lock.
            logger.info(
                "Concurrent duplicate execution of task %s detected (key=%s), proceeding anyway.",
                message.task_name,
                key,
            )

        return message

    async def post_execute(
        self,
        message: TaskiqMessage,
        result: TaskiqResult,
    ) -> None:
        if not self._is_enabled(message.labels):
            return
        base_key = self.default_build_deduplication_key(message)
        await self._release_if_owned(base_key, message.task_id, "queue lock")
        await self._release_if_owned(
            self._exec_key(base_key), message.task_id, "execution lock"
        )

    async def on_error(
        self,
        message: TaskiqMessage,
        result: TaskiqResult,
        exception: BaseException,
    ) -> None:
        if not self._is_enabled(message.labels):
            return
        base_key = self.default_build_deduplication_key(message)
        await self._release_if_owned(base_key, message.task_id, "queue lock")
        await self._release_if_owned(
            self._exec_key(base_key), message.task_id, "execution lock"
        )
