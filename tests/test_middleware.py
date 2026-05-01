from unittest.mock import AsyncMock, patch

import pytest

from taskiq_deduplication import DuplicateTaskError, RedisDeduplicationMiddleware
from taskiq_deduplication.middleware import (
    DEDUP_EXPLICIT_KEY_LABEL,
    DEDUP_KEY_FIELDS_LABEL,
    DEDUP_LABEL,
    DEDUP_TTL_LABEL,
)


@pytest.fixture
def middleware(fake_redis):
    mw = RedisDeduplicationMiddleware(redis_url="redis://localhost")
    mw._redis = fake_redis
    return mw


@pytest.fixture
def middleware_raise(fake_redis):
    mw = RedisDeduplicationMiddleware(
        redis_url="redis://localhost", raise_on_duplicate=True
    )
    mw._redis = fake_redis
    return mw


class TestDefaultBuildDeduplicationKey:
    def test_same_kwargs_same_key(self, middleware, make_message):
        m1 = make_message(kwargs={"a": 1, "b": 2})
        m2 = make_message(kwargs={"a": 1, "b": 2})
        assert middleware._build_deduplication_key(
            m1
        ) == middleware._build_deduplication_key(m2)

    def test_different_kwargs_different_key(self, middleware, make_message):
        m1 = make_message(kwargs={"a": 1})
        m2 = make_message(kwargs={"a": 2})
        assert middleware._build_deduplication_key(
            m1
        ) != middleware._build_deduplication_key(m2)

    def test_kwarg_order_invariant(self, middleware, make_message):
        m1 = make_message(kwargs={"a": 1, "b": 2})
        m2 = make_message(kwargs={"b": 2, "a": 1})
        assert middleware._build_deduplication_key(
            m1
        ) == middleware._build_deduplication_key(m2)

    def test_different_task_names_different_keys(self, middleware, make_message):
        m1 = make_message(task_name="task_a", kwargs={"x": 1})
        m2 = make_message(task_name="task_b", kwargs={"x": 1})
        assert middleware._build_deduplication_key(
            m1
        ) != middleware._build_deduplication_key(m2)

    def test_explicit_key_label(self, middleware, make_message):
        m = make_message(labels={DEDUP_EXPLICIT_KEY_LABEL: "my-lock"})
        key = middleware._build_deduplication_key(m)
        assert key == "taskiq:deduplication:my-lock"

    def test_explicit_key_ignores_kwargs(self, middleware, make_message):
        m1 = make_message(kwargs={"a": 1}, labels={DEDUP_EXPLICIT_KEY_LABEL: "fixed"})
        m2 = make_message(kwargs={"a": 99}, labels={DEDUP_EXPLICIT_KEY_LABEL: "fixed"})
        assert middleware._build_deduplication_key(
            m1
        ) == middleware._build_deduplication_key(m2)

    def test_key_fields_filters_kwargs(self, middleware, make_message):
        m1 = make_message(
            kwargs={"a": 1, "b": 2, "c": 3},
            labels={DEDUP_KEY_FIELDS_LABEL: ["a", "b"]},
        )
        m2 = make_message(
            kwargs={"a": 1, "b": 2, "c": 999},
            labels={DEDUP_KEY_FIELDS_LABEL: ["a", "b"]},
        )
        assert middleware._build_deduplication_key(
            m1
        ) == middleware._build_deduplication_key(m2)

    def test_key_fields_different_included_fields(self, middleware, make_message):
        m1 = make_message(
            kwargs={"a": 1, "b": 2},
            labels={DEDUP_KEY_FIELDS_LABEL: ["a"]},
        )
        m2 = make_message(
            kwargs={"a": 1, "b": 99},
            labels={DEDUP_KEY_FIELDS_LABEL: ["a"]},
        )
        assert middleware._build_deduplication_key(
            m1
        ) == middleware._build_deduplication_key(m2)

    def test_key_prefix_in_output(self, make_message):
        mw = RedisDeduplicationMiddleware(
            redis_url="redis://localhost", key_prefix="myapp:locks"
        )
        mw._redis = None
        m = make_message()
        key = mw._build_deduplication_key(m)
        assert key.startswith("myapp:locks:")


class TestPreSend:
    @pytest.mark.anyio
    async def test_first_send_passes(self, middleware, make_message):
        msg = make_message()
        result = await middleware.pre_send(msg)
        assert result is msg

    @pytest.mark.anyio
    async def test_duplicate_raises_when_opted_in(self, middleware_raise, make_message):
        msg = make_message()
        await middleware_raise.pre_send(msg)
        with pytest.raises(DuplicateTaskError):
            await middleware_raise.pre_send(make_message())

    @pytest.mark.anyio
    async def test_duplicate_no_raise_by_default(self, middleware, make_message):
        msg = make_message()
        await middleware.pre_send(msg)
        result = await middleware.pre_send(make_message())
        assert result is not None

    @pytest.mark.anyio
    async def test_deduplication_disabled_label(self, middleware, make_message):
        msg1 = make_message(labels={DEDUP_LABEL: False})
        msg2 = make_message(labels={DEDUP_LABEL: False})
        await middleware.pre_send(msg1)
        await middleware.pre_send(msg2)  # should not raise

    @pytest.mark.anyio
    async def test_deduplication_disabled_by_default(self, fake_redis, make_message):
        mw = RedisDeduplicationMiddleware(
            redis_url="redis://localhost", default_deduplication=False
        )
        mw._redis = fake_redis
        msg = make_message()
        await mw.pre_send(msg)
        await mw.pre_send(make_message())  # should not raise

    @pytest.mark.anyio
    async def test_ttl_applied(self, middleware, fake_redis, make_message):
        msg = make_message(labels={DEDUP_TTL_LABEL: 42})
        await middleware.pre_send(msg)
        key = middleware._build_deduplication_key(msg)
        ttl = await fake_redis.ttl(key)
        assert 0 < ttl <= 42

    @pytest.mark.anyio
    async def test_different_kwargs_both_pass(self, middleware, make_message):
        await middleware.pre_send(make_message(kwargs={"x": 1}))
        await middleware.pre_send(make_message(kwargs={"x": 2}))  # different key


class TestPreExecute:
    @pytest.mark.anyio
    async def test_first_execute_passes(self, middleware, make_message):
        msg = make_message()
        result = await middleware.pre_execute(msg)
        assert result is msg

    @pytest.mark.anyio
    async def test_duplicate_execute_does_not_raise(self, middleware, make_message):
        msg = make_message()
        await middleware.pre_execute(msg)
        # second execution must never raise, per SmartRetryMiddleware contract
        result = await middleware.pre_execute(make_message())
        assert result is not None

    @pytest.mark.anyio
    async def test_deduplication_disabled_label(self, middleware, make_message):
        msg = make_message(labels={DEDUP_LABEL: False})
        await middleware.pre_execute(msg)
        await middleware.pre_execute(make_message(labels={DEDUP_LABEL: False}))

    @pytest.mark.anyio
    async def test_exec_key_separate_from_queue_key(
        self, middleware, fake_redis, make_message
    ):
        msg = make_message()
        await middleware.pre_send(msg)
        await middleware.pre_execute(msg)
        base_key = middleware._build_deduplication_key(msg)
        exec_key = middleware._exec_key(base_key)
        assert base_key != exec_key
        assert await fake_redis.exists(base_key)
        assert await fake_redis.exists(exec_key)


class TestPostExecute:
    @pytest.mark.anyio
    async def test_releases_queue_lock(
        self, middleware, fake_redis, make_message, make_result
    ):
        msg = make_message()
        await middleware.pre_send(msg)
        key = middleware._build_deduplication_key(msg)
        assert await fake_redis.exists(key)

        await middleware.post_execute(msg, make_result())
        assert not await fake_redis.exists(key)

    @pytest.mark.anyio
    async def test_releases_exec_lock(
        self, middleware, fake_redis, make_message, make_result
    ):
        msg = make_message()
        await middleware.pre_execute(msg)
        exec_key = middleware._exec_key(middleware._build_deduplication_key(msg))
        assert await fake_redis.exists(exec_key)

        await middleware.post_execute(msg, make_result())
        assert not await fake_redis.exists(exec_key)

    @pytest.mark.anyio
    async def test_deduplication_disabled_noop(
        self, middleware, fake_redis, make_message, make_result
    ):
        msg = make_message()
        await middleware.pre_send(msg)
        key = middleware._build_deduplication_key(msg)

        disabled_msg = make_message(labels={DEDUP_LABEL: False})
        await middleware.post_execute(disabled_msg, make_result())
        # lock set by pre_send with deduplication enabled is still there
        assert await fake_redis.exists(key)


class TestOnError:
    @pytest.mark.anyio
    async def test_releases_queue_lock_on_error(
        self, middleware, fake_redis, make_message, make_result
    ):
        msg = make_message()
        await middleware.pre_send(msg)
        key = middleware._build_deduplication_key(msg)
        assert await fake_redis.exists(key)

        await middleware.on_error(msg, make_result(is_err=True), RuntimeError("boom"))
        assert not await fake_redis.exists(key)

    @pytest.mark.anyio
    async def test_releases_exec_lock_on_error(
        self, middleware, fake_redis, make_message, make_result
    ):
        msg = make_message()
        await middleware.pre_execute(msg)
        exec_key = middleware._exec_key(middleware._build_deduplication_key(msg))
        assert await fake_redis.exists(exec_key)

        await middleware.on_error(msg, make_result(is_err=True), RuntimeError("boom"))
        assert not await fake_redis.exists(exec_key)

    @pytest.mark.anyio
    async def test_deduplication_disabled_noop(
        self, middleware, fake_redis, make_message, make_result
    ):
        msg = make_message()
        await middleware.pre_send(msg)
        key = middleware._build_deduplication_key(msg)

        disabled_msg = make_message(labels={DEDUP_LABEL: False})
        await middleware.on_error(
            disabled_msg, make_result(is_err=True), RuntimeError("x")
        )
        assert await fake_redis.exists(key)


class TestAtomicRelease:
    @pytest.mark.anyio
    async def test_only_owner_can_release(self, middleware, fake_redis, make_message):
        owner_msg = make_message(task_id="owner-task")
        key = middleware._build_deduplication_key(owner_msg)

        await fake_redis.set(key, "owner-task", ex=300)

        # other task should NOT release the lock
        await middleware._release_if_owned(key, "other-task", "queue lock")
        assert await fake_redis.exists(key)

        # owner task should release it
        await middleware._release_if_owned(key, "owner-task", "queue lock")
        assert not await fake_redis.exists(key)

    @pytest.mark.anyio
    async def test_release_missing_key_is_noop(self, middleware, fake_redis):
        await middleware._release_if_owned(
            "taskiq:deduplication:nonexistent", "some-task", "queue lock"
        )
        # no error raised


class TestLifecycle:
    @pytest.mark.anyio
    async def test_startup_creates_redis_client(self):
        mw = RedisDeduplicationMiddleware(redis_url="redis://localhost")
        assert mw._redis is None
        with patch("redis.asyncio.Redis.from_url") as mock_from_url:
            mock_client = AsyncMock()
            mock_from_url.return_value = mock_client
            await mw.startup()
            mock_from_url.assert_called_once_with("redis://localhost")
            assert mw._redis is mock_client

    @pytest.mark.anyio
    async def test_shutdown_closes_redis_client(self):
        mw = RedisDeduplicationMiddleware(redis_url="redis://localhost")
        mock_client = AsyncMock()
        mw._redis = mock_client
        await mw.shutdown()
        mock_client.aclose.assert_called_once()

    @pytest.mark.anyio
    async def test_shutdown_without_startup_is_safe(self):
        mw = RedisDeduplicationMiddleware(redis_url="redis://localhost")
        await mw.shutdown()  # _redis is None, should not raise
