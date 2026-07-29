import fakeredis.aioredis
import pytest
from redis.asyncio import Redis
from taskiq import TaskiqMessage, TaskiqResult


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def fake_redis():
    return fakeredis.aioredis.FakeRedis()


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
    def _make(
        task_name="my_task", task_id="task-1", labels=None, kwargs=None, args=None
    ):
        return TaskiqMessage(
            task_id=task_id,
            task_name=task_name,
            labels=labels or {},
            labels_types={},
            args=args or [],
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
