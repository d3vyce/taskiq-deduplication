import logging

import pytest
from taskiq import InMemoryBroker, ScheduleSource, TaskiqScheduler
from taskiq.exceptions import ScheduledTaskCancelledError

from taskiq_deduplication import RedisDeduplicationMiddleware
from taskiq_deduplication.middleware import (
    DEDUP_EXPLICIT_KEY_LABEL,
    DEDUP_KEY_FIELDS_LABEL,
    DEDUP_LABEL,
)
from taskiq_deduplication.schedule import RedisDeduplicationScheduleSource


class FakeScheduleSource(ScheduleSource):
    def __init__(self):
        self.startup_called = False
        self.shutdown_called = False
        self.schedules_to_return = []
        self.added = []
        self.deleted = []
        self.pre_send_calls = []
        self.post_send_calls = []
        self.pre_send_raises = None

    async def startup(self):
        self.startup_called = True

    async def shutdown(self):
        self.shutdown_called = True

    async def get_schedules(self):
        return self.schedules_to_return

    async def add_schedule(self, schedule):
        self.added.append(schedule)

    async def delete_schedule(self, schedule_id):
        self.deleted.append(schedule_id)

    async def pre_send(self, task):
        self.pre_send_calls.append(task)
        if self.pre_send_raises is not None:
            raise self.pre_send_raises

    async def post_send(self, task):
        self.post_send_calls.append(task)


@pytest.fixture
def fake_source():
    return FakeScheduleSource()


@pytest.fixture
def wrapper(fake_source, middleware):
    return RedisDeduplicationScheduleSource(fake_source, middleware)


class TestDelegation:
    async def test_startup_delegates(self, wrapper, fake_source):
        await wrapper.startup()
        assert fake_source.startup_called

    async def test_shutdown_delegates(self, wrapper, fake_source):
        await wrapper.shutdown()
        assert fake_source.shutdown_called

    async def test_get_schedules_delegates(
        self, wrapper, fake_source, make_scheduled_task
    ):
        task = make_scheduled_task()
        fake_source.schedules_to_return = [task]
        assert await wrapper.get_schedules() == [task]

    async def test_add_schedule_delegates(
        self, wrapper, fake_source, make_scheduled_task
    ):
        task = make_scheduled_task()
        await wrapper.add_schedule(task)
        assert fake_source.added == [task]

    async def test_delete_schedule_delegates(self, wrapper, fake_source):
        await wrapper.delete_schedule("some-id")
        assert fake_source.deleted == ["some-id"]

    async def test_post_send_delegates(self, wrapper, fake_source, make_scheduled_task):
        task = make_scheduled_task()
        await wrapper.post_send(task)
        assert fake_source.post_send_calls == [task]

    async def test_pre_send_delegates(self, wrapper, fake_source, make_scheduled_task):
        task = make_scheduled_task()
        await wrapper.pre_send(task)
        assert fake_source.pre_send_calls == [task]


class TestPreSend:
    async def test_no_lock_held_passes(self, wrapper, make_scheduled_task):
        task = make_scheduled_task()
        result = await wrapper.pre_send(task)
        assert result is None

    async def test_wrapped_source_cancellation_propagates(
        self, wrapper, fake_source, make_scheduled_task
    ):
        fake_source.pre_send_raises = ScheduledTaskCancelledError()
        with pytest.raises(ScheduledTaskCancelledError):
            await wrapper.pre_send(make_scheduled_task())

    async def test_lock_held_raises_scheduled_task_cancelled_error(
        self, wrapper, middleware, make_message, make_scheduled_task
    ):
        await middleware.pre_send(make_message(task_name="my_task", kwargs={"a": 1}))
        task = make_scheduled_task(task_name="my_task", kwargs={"a": 1})
        with pytest.raises(ScheduledTaskCancelledError):
            await wrapper.pre_send(task)

    async def test_peek_does_not_acquire_or_mutate(
        self, wrapper, middleware, fake_redis, make_scheduled_task
    ):
        task = make_scheduled_task(task_name="my_task", kwargs={"a": 1})
        await wrapper.pre_send(task)
        key = middleware._build_key(task.task_name, task.labels, task.kwargs)
        assert not await fake_redis.exists(key)
        assert task.labels == {}
        assert task.kwargs == {"a": 1}

    async def test_peek_is_read_only_when_lock_held(
        self, wrapper, middleware, fake_redis, make_message, make_scheduled_task
    ):
        held_msg = make_message(task_name="my_task", task_id="holder", kwargs={"a": 1})
        await middleware.pre_send(held_msg)
        key = middleware._build_deduplication_key(held_msg)
        ttl_before = await fake_redis.ttl(key)
        holder_before = await fake_redis.get(key)

        task = make_scheduled_task(task_name="my_task", kwargs={"a": 1})
        with pytest.raises(ScheduledTaskCancelledError):
            await wrapper.pre_send(task)

        # The peek must not have re-set the key (TTL untouched) or changed
        # its owner.
        assert await fake_redis.get(key) == holder_before
        assert await fake_redis.ttl(key) <= ttl_before

    async def test_deduplication_disabled_label_bypasses_peek(
        self, wrapper, middleware, make_message, make_scheduled_task
    ):
        await middleware.pre_send(make_message(task_name="my_task", kwargs={"a": 1}))
        task = make_scheduled_task(
            task_name="my_task", kwargs={"a": 1}, labels={DEDUP_LABEL: False}
        )
        await wrapper.pre_send(task)  # should not raise

    async def test_deduplication_key_label_respected(
        self, wrapper, middleware, make_message, make_scheduled_task
    ):
        await middleware.pre_send(
            make_message(kwargs={"a": 1}, labels={DEDUP_EXPLICIT_KEY_LABEL: "fixed"})
        )
        task = make_scheduled_task(
            kwargs={"a": 999}, labels={DEDUP_EXPLICIT_KEY_LABEL: "fixed"}
        )
        with pytest.raises(ScheduledTaskCancelledError):
            await wrapper.pre_send(task)

    async def test_deduplication_key_fields_label_respected(
        self, wrapper, middleware, make_message, make_scheduled_task
    ):
        await middleware.pre_send(
            make_message(
                kwargs={"a": 1, "b": 2},
                labels={DEDUP_KEY_FIELDS_LABEL: ["a"]},
            )
        )
        task = make_scheduled_task(
            kwargs={"a": 1, "b": 999},
            labels={DEDUP_KEY_FIELDS_LABEL: ["a"]},
        )
        with pytest.raises(ScheduledTaskCancelledError):
            await wrapper.pre_send(task)

    async def test_non_serializable_kwargs_skips_peek_silently(
        self, wrapper, make_scheduled_task, caplog
    ):
        task = make_scheduled_task(kwargs={"dt": object()})
        with caplog.at_level(logging.WARNING, logger="taskiq_deduplication.schedule"):
            await wrapper.pre_send(task)  # should not raise
        assert not any("non-JSON-serializable" in r.message for r in caplog.records)

    async def test_pre_send_without_middleware_startup_raises_runtime_error(
        self, fake_source, make_scheduled_task
    ):
        mw = RedisDeduplicationMiddleware(redis_url="redis://localhost")
        w = RedisDeduplicationScheduleSource(fake_source, mw)
        with pytest.raises(RuntimeError, match="startup"):
            await w.pre_send(make_scheduled_task())

    async def test_pre_send_without_middleware_startup_logs_before_raising(
        self, fake_source, make_scheduled_task, caplog
    ):
        mw = RedisDeduplicationMiddleware(redis_url="redis://localhost")
        w = RedisDeduplicationScheduleSource(fake_source, mw)
        with caplog.at_level(logging.ERROR, logger="taskiq_deduplication.schedule"):
            with pytest.raises(RuntimeError):
                await w.pre_send(make_scheduled_task())
        assert any("startup" in r.message for r in caplog.records)

    async def test_different_kwargs_both_pass(self, wrapper, make_scheduled_task):
        await wrapper.pre_send(make_scheduled_task(kwargs={"x": 1}))
        await wrapper.pre_send(make_scheduled_task(kwargs={"x": 2}))


class TestSchedulerIntegration:
    async def test_second_firing_cancelled_without_uncaught_exception(
        self, middleware, fake_source, make_message, make_scheduled_task
    ):
        broker = InMemoryBroker().with_middlewares(middleware)
        wrapper = RedisDeduplicationScheduleSource(fake_source, middleware)
        scheduler = TaskiqScheduler(broker=broker, sources=[wrapper])

        task_name = "my_task"

        # Simulate a still-running first firing by holding the lock directly.
        await middleware.pre_send(
            make_message(task_name=task_name, task_id="holder", kwargs={})
        )

        scheduled = make_scheduled_task(task_name=task_name, kwargs={})

        # Must be cancelled gracefully by on_ready()'s own except clause, not
        # raise DuplicateTaskError out of kiq() into an uncaught exception.
        await scheduler.on_ready(wrapper, scheduled)
