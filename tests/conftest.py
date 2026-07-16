import pytest
import fakeredis.aioredis
from redis.asyncio import Redis
from taskiq import ScheduledTask, TaskiqMessage, TaskiqResult

from taskiq_deduplication import RedisDeduplicationMiddleware


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def fake_redis():
    client = fakeredis.aioredis.FakeRedis()
    yield client
    await client.aclose()


@pytest.fixture
def middleware(fake_redis):
    mw = RedisDeduplicationMiddleware(redis_url="redis://localhost")
    mw._redis = fake_redis
    return mw


@pytest.fixture
async def real_redis():
    client = Redis.from_url("redis://localhost:6379/15")
    try:
        await client.ping()
    except Exception:
        await client.aclose()
        pytest.skip("Redis not available at localhost:6379")
        return
    await client.flushdb()
    yield client
    await client.flushdb()
    await client.aclose()


@pytest.fixture
def make_message():
    def _make(task_name="my_task", task_id="task-1", labels=None, kwargs=None):
        return TaskiqMessage(
            task_id=task_id,
            task_name=task_name,
            labels=labels or {},
            labels_types={},
            args=[],
            kwargs=kwargs or {},
        )

    return _make


@pytest.fixture
def make_result():
    def _make(is_err=False):
        return TaskiqResult(
            is_err=is_err,
            log="",
            return_value=None,
            execution_time=0.0,
        )

    return _make


@pytest.fixture
def make_scheduled_task():
    def _make(
        task_name="my_task",
        schedule_id="schedule-1",
        labels=None,
        kwargs=None,
        cron="* * * * *",
    ):
        return ScheduledTask(
            task_name=task_name,
            schedule_id=schedule_id,
            labels=labels or {},
            args=[],
            kwargs=kwargs or {},
            cron=cron,
        )

    return _make
