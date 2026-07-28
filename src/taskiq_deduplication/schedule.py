import logging

from taskiq import ScheduledTask, ScheduleSource
from taskiq.exceptions import ScheduledTaskCancelledError
from taskiq.utils import maybe_awaitable

from .middleware import RedisDeduplicationMiddleware

logger = logging.getLogger(__name__)


class RedisDeduplicationScheduleSource(ScheduleSource):
    """Skips scheduled firings whose fingerprint is already locked.

    Wraps a ``ScheduleSource`` and peeks the lock in ``pre_send``, raising
    ``ScheduledTaskCancelledError`` on a hit so the scheduler skips the firing
    cleanly instead of raising ``DuplicateTaskError`` out of ``kiq()``. The
    atomic acquire/release lifecycle stays owned by ``middleware``.

    Attributes:
        source: The wrapped ``ScheduleSource``.
        middleware: The ``RedisDeduplicationMiddleware`` instance registered
            on the broker. Must be the same instance, so both share one Redis
            connection and configuration. Its ``startup()`` must have run
            before ``pre_send()`` is invoked.
    """

    def __init__(
        self,
        source: ScheduleSource,
        middleware: RedisDeduplicationMiddleware,
    ) -> None:
        self.source = source
        self.middleware = middleware

    async def startup(self) -> None:
        await self.source.startup()

    async def shutdown(self) -> None:
        await self.source.shutdown()

    async def get_schedules(self) -> list[ScheduledTask]:
        return await self.source.get_schedules()

    async def add_schedule(self, schedule: ScheduledTask) -> None:
        await self.source.add_schedule(schedule)

    async def delete_schedule(self, schedule_id: str) -> None:
        await self.source.delete_schedule(schedule_id)

    async def post_send(self, task: ScheduledTask) -> None:
        await maybe_awaitable(self.source.post_send(task))

    async def pre_send(self, task: ScheduledTask) -> None:
        await maybe_awaitable(self.source.pre_send(task))

        try:
            held = await self.middleware._peek(task.task_name, task.labels, task.kwargs)
        except RuntimeError:
            logger.error(
                "RedisDeduplicationMiddleware.startup() was never called; "
                "cannot deduplicate scheduled task %s.",
                task.task_name,
            )
            raise

        if held is None:
            return
        key, holder_task_id = held
        logger.warning(
            "Duplicate scheduled task %s skipped before dispatch "
            "(key=%s, holder_task_id=%s).",
            task.task_name,
            key,
            holder_task_id,
        )
        raise ScheduledTaskCancelledError()
