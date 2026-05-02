import pytest
import fakeredis.aioredis
from taskiq import TaskiqMessage, TaskiqResult


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def fake_redis():
    client = fakeredis.aioredis.FakeRedis()
    yield client
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
