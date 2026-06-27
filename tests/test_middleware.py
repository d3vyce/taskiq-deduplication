from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import RedisDsn, TypeAdapter

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
        assert key is not None and key.startswith("myapp:locks:")

    def test_empty_kwargs_produces_consistent_key(self, middleware, make_message):
        m1 = make_message(kwargs={})
        m2 = make_message(kwargs={})
        assert middleware._build_deduplication_key(
            m1
        ) == middleware._build_deduplication_key(m2)

    def test_key_fields_empty_list_ignores_all_kwargs(self, middleware, make_message):
        m1 = make_message(kwargs={"a": 1}, labels={DEDUP_KEY_FIELDS_LABEL: []})
        m2 = make_message(kwargs={"a": 999}, labels={DEDUP_KEY_FIELDS_LABEL: []})
        assert middleware._build_deduplication_key(
            m1
        ) == middleware._build_deduplication_key(m2)

    def test_key_fields_absent_from_kwargs_are_ignored(self, middleware, make_message):
        m1 = make_message(
            kwargs={"order_id": 1}, labels={DEDUP_KEY_FIELDS_LABEL: ["user_id"]}
        )
        m2 = make_message(
            kwargs={"order_id": 999}, labels={DEDUP_KEY_FIELDS_LABEL: ["user_id"]}
        )
        assert middleware._build_deduplication_key(
            m1
        ) == middleware._build_deduplication_key(m2)

    def test_explicit_key_takes_precedence_over_key_fields(
        self, middleware, make_message
    ):
        m = make_message(
            kwargs={"a": 1},
            labels={DEDUP_EXPLICIT_KEY_LABEL: "my-lock", DEDUP_KEY_FIELDS_LABEL: ["a"]},
        )
        assert middleware._build_deduplication_key(m) == "taskiq:deduplication:my-lock"

    def test_non_serializable_kwargs_returns_none(self, middleware, make_message):
        m = make_message(kwargs={"dt": object()})
        assert middleware._build_deduplication_key(m) is None


class TestPreSend:
    async def test_first_send_passes(self, middleware, make_message):
        msg = make_message()
        result = await middleware.pre_send(msg)
        assert result is msg

    async def test_duplicate_raises(self, middleware, make_message):
        msg = make_message()
        await middleware.pre_send(msg)
        with pytest.raises(DuplicateTaskError):
            await middleware.pre_send(make_message())

    async def test_duplicate_error_carries_structured_attributes(
        self, middleware, make_message
    ):
        holder = make_message(task_id="holder-task")
        await middleware.pre_send(holder)
        key = middleware._build_deduplication_key(holder)
        with pytest.raises(DuplicateTaskError) as exc_info:
            await middleware.pre_send(make_message(task_id="loser-task"))
        err = exc_info.value
        assert err.task_name == "my_task"
        assert err.key == key
        assert err.holder_task_id == "holder-task"
        assert key in str(err)

    async def test_duplicate_error_holder_none_when_lock_released_in_race(
        self, make_message
    ):
        # The lock is released between the failed SET NX and the GET lookup, so
        # GET returns None and holder_task_id is left unset.
        redis = AsyncMock()
        redis.set.return_value = False
        redis.get.return_value = None
        mw = RedisDeduplicationMiddleware(redis_url="redis://localhost")
        mw._redis = redis
        with pytest.raises(DuplicateTaskError) as exc_info:
            await mw.pre_send(make_message())
        assert exc_info.value.holder_task_id is None

    async def test_deduplication_disabled_label(self, middleware, make_message):
        msg1 = make_message(labels={DEDUP_LABEL: False})
        msg2 = make_message(labels={DEDUP_LABEL: False})
        await middleware.pre_send(msg1)
        await middleware.pre_send(msg2)  # should not raise

    async def test_deduplication_disabled_by_default_init(
        self, fake_redis, make_message
    ):
        mw = RedisDeduplicationMiddleware(
            redis_url="redis://localhost", default_deduplication=False
        )
        mw._redis = fake_redis
        await mw.pre_send(make_message())
        await mw.pre_send(make_message())  # should not raise

    async def test_ttl_applied(self, middleware, fake_redis, make_message):
        msg = make_message(labels={DEDUP_TTL_LABEL: 42})
        await middleware.pre_send(msg)
        key = middleware._build_deduplication_key(msg)
        ttl = await fake_redis.ttl(key)
        assert 0 < ttl <= 42

    async def test_different_kwargs_both_pass(self, middleware, make_message):
        await middleware.pre_send(make_message(kwargs={"x": 1}))
        await middleware.pre_send(make_message(kwargs={"x": 2}))

    async def test_non_serializable_kwargs_skips_deduplication(
        self, middleware, make_message
    ):
        msg1 = make_message(kwargs={"dt": object()})
        msg2 = make_message(kwargs={"dt": object()})
        await middleware.pre_send(msg1)
        await middleware.pre_send(msg2)  # should not raise

    async def test_default_ttl_applied(self, fake_redis, make_message):
        mw = RedisDeduplicationMiddleware(redis_url="redis://localhost", default_ttl=77)
        mw._redis = fake_redis
        msg = make_message()
        await mw.pre_send(msg)
        key = mw._build_deduplication_key(msg)
        ttl = await fake_redis.ttl(key)
        assert 0 < ttl <= 77


class TestPostExecute:
    async def test_releases_lock(
        self, middleware, fake_redis, make_message, make_result
    ):
        msg = make_message()
        await middleware.pre_send(msg)
        key = middleware._build_deduplication_key(msg)
        assert await fake_redis.exists(key)

        await middleware.post_execute(msg, make_result())
        assert not await fake_redis.exists(key)

    async def test_deduplication_disabled_noop(
        self, middleware, fake_redis, make_message, make_result
    ):
        msg = make_message()
        await middleware.pre_send(msg)
        key = middleware._build_deduplication_key(msg)

        disabled_msg = make_message(labels={DEDUP_LABEL: False})
        await middleware.post_execute(disabled_msg, make_result())
        assert await fake_redis.exists(key)

    async def test_post_execute_after_ttl_expiry_is_safe(
        self, middleware, fake_redis, make_message, make_result
    ):
        msg = make_message()
        await middleware.pre_send(msg)
        await fake_redis.delete(middleware._build_deduplication_key(msg))
        await middleware.post_execute(msg, make_result())


class TestOnError:
    async def test_releases_lock_on_error(
        self, middleware, fake_redis, make_message, make_result
    ):
        msg = make_message()
        await middleware.pre_send(msg)
        key = middleware._build_deduplication_key(msg)
        assert await fake_redis.exists(key)

        await middleware.on_error(msg, make_result(is_err=True), RuntimeError("boom"))
        assert not await fake_redis.exists(key)

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


class TestSenderWorkerConfigMismatch:
    async def test_post_execute_releases_lock_despite_worker_disabled_default(
        self, fake_redis, make_message, make_result
    ):
        sender_mw = RedisDeduplicationMiddleware(
            redis_url="redis://localhost", default_deduplication=True
        )
        sender_mw._redis = fake_redis

        worker_mw = RedisDeduplicationMiddleware(
            redis_url="redis://localhost", default_deduplication=False
        )
        worker_mw._redis = fake_redis

        msg = make_message()
        await sender_mw.pre_send(msg)
        key = sender_mw._build_deduplication_key(msg)
        assert await fake_redis.exists(key)

        await worker_mw.post_execute(msg, make_result())
        assert not await fake_redis.exists(key)

    async def test_on_error_releases_lock_despite_worker_disabled_default(
        self, fake_redis, make_message, make_result
    ):
        sender_mw = RedisDeduplicationMiddleware(
            redis_url="redis://localhost", default_deduplication=True
        )
        sender_mw._redis = fake_redis

        worker_mw = RedisDeduplicationMiddleware(
            redis_url="redis://localhost", default_deduplication=False
        )
        worker_mw._redis = fake_redis

        msg = make_message()
        await sender_mw.pre_send(msg)
        key = sender_mw._build_deduplication_key(msg)
        assert await fake_redis.exists(key)

        await worker_mw.on_error(msg, make_result(is_err=True), RuntimeError("boom"))
        assert not await fake_redis.exists(key)


class TestRedispatchAfterRelease:
    async def test_redispatch_after_post_execute(
        self, middleware, make_message, make_result
    ):
        msg = make_message()
        await middleware.pre_send(msg)
        await middleware.post_execute(msg, make_result())
        await middleware.pre_send(make_message())

    async def test_redispatch_after_on_error(
        self, middleware, make_message, make_result
    ):
        msg = make_message()
        await middleware.pre_send(msg)
        await middleware.on_error(msg, make_result(is_err=True), RuntimeError("boom"))
        await middleware.pre_send(make_message())


class TestAtomicRelease:
    async def test_only_owner_can_release(self, middleware, fake_redis, make_message):
        owner_msg = make_message(task_id="owner-task")
        key = middleware._build_deduplication_key(owner_msg)

        await fake_redis.set(key, "owner-task", ex=300)

        await middleware._release_if_owned(key, "other-task")
        assert await fake_redis.exists(key)

        await middleware._release_if_owned(key, "owner-task")
        assert not await fake_redis.exists(key)

    async def test_release_missing_key_is_noop(self, middleware, fake_redis):
        await middleware._release_if_owned(
            "taskiq:deduplication:nonexistent", "some-task"
        )


class TestLifecycle:
    async def test_startup_creates_redis_client(self):
        mw = RedisDeduplicationMiddleware(redis_url="redis://localhost")
        assert mw._redis is None
        with patch("redis.asyncio.Redis.from_url") as mock_from_url:
            mock_client = AsyncMock()
            mock_client.register_script = MagicMock()
            mock_from_url.return_value = mock_client
            await mw.startup()
            mock_from_url.assert_called_once_with("redis://localhost")
            assert mw._redis is mock_client

    async def test_startup_accepts_redis_dsn(self):
        dsn = TypeAdapter(RedisDsn).validate_python("redis://localhost:6379/0")
        mw = RedisDeduplicationMiddleware(redis_url=dsn)
        with patch("redis.asyncio.Redis.from_url") as mock_from_url:
            mock_client = AsyncMock()
            mock_client.register_script = MagicMock()
            mock_from_url.return_value = mock_client
            await mw.startup()
            mock_from_url.assert_called_once_with("redis://localhost:6379/0")
            assert mw._redis is mock_client

    async def test_shutdown_closes_redis_client(self):
        mw = RedisDeduplicationMiddleware(redis_url="redis://localhost")
        mock_client = AsyncMock()
        mw._redis = mock_client
        await mw.shutdown()
        mock_client.aclose.assert_called_once()

    async def test_shutdown_without_startup_is_safe(self):
        mw = RedisDeduplicationMiddleware(redis_url="redis://localhost")
        await mw.shutdown()

    async def test_pre_send_without_startup_raises_runtime_error(self, make_message):
        mw = RedisDeduplicationMiddleware(redis_url="redis://localhost")
        with pytest.raises(RuntimeError, match="startup"):
            await mw.pre_send(make_message())


class TestStartupRetry:
    async def test_startup_succeeds_after_retries(self):
        mw = RedisDeduplicationMiddleware(
            redis_url="redis://localhost",
            startup_retries=3,
            startup_retry_delay=0.01,
        )
        with patch("redis.asyncio.Redis.from_url") as mock_from_url:
            mock_client = AsyncMock()
            mock_client.ping.side_effect = [
                ConnectionError("fail"),
                ConnectionError("fail"),
                None,
            ]
            mock_client.register_script = MagicMock()
            mock_from_url.return_value = mock_client
            await mw.startup()
            assert mw._redis is mock_client
            assert mock_client.ping.call_count == 3

    async def test_startup_raises_after_all_retries_exhausted(self):
        mw = RedisDeduplicationMiddleware(
            redis_url="redis://localhost",
            startup_retries=2,
            startup_retry_delay=0.01,
        )
        with patch("redis.asyncio.Redis.from_url") as mock_from_url:
            mock_client = AsyncMock()
            mock_client.ping.side_effect = ConnectionError("refused")
            mock_from_url.return_value = mock_client
            with pytest.raises(ConnectionError, match="2 attempts"):
                await mw.startup()
            assert mock_client.ping.call_count == 2

    async def test_startup_no_retry_on_first_success(self):
        mw = RedisDeduplicationMiddleware(
            redis_url="redis://localhost",
            startup_retries=3,
            startup_retry_delay=0.01,
        )
        with patch("redis.asyncio.Redis.from_url") as mock_from_url:
            mock_client = AsyncMock()
            mock_client.register_script = MagicMock()
            mock_from_url.return_value = mock_client
            await mw.startup()
            mock_client.ping.assert_called_once()

    async def test_startup_retry_delay_exponential(self):
        mw = RedisDeduplicationMiddleware(
            redis_url="redis://localhost",
            startup_retries=3,
            startup_retry_delay=0.01,
        )
        with (
            patch("redis.asyncio.Redis.from_url") as mock_from_url,
            patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
        ):
            mock_client = AsyncMock()
            mock_client.ping.side_effect = [
                ConnectionError("fail"),
                ConnectionError("fail"),
                None,
            ]
            mock_client.register_script = MagicMock()
            mock_from_url.return_value = mock_client
            await mw.startup()
            assert mock_sleep.call_count == 2
            assert mock_sleep.call_args_list[0].args[0] == 0.01
            assert mock_sleep.call_args_list[1].args[0] == 0.02

    async def test_startup_closes_failed_clients(self):
        mw = RedisDeduplicationMiddleware(
            redis_url="redis://localhost",
            startup_retries=3,
            startup_retry_delay=0.01,
        )
        failed1, failed2, good = AsyncMock(), AsyncMock(), AsyncMock()
        failed1.ping.side_effect = ConnectionError("fail")
        failed2.ping.side_effect = ConnectionError("fail")
        good.register_script = MagicMock()

        with patch(
            "redis.asyncio.Redis.from_url", side_effect=[failed1, failed2, good]
        ):
            await mw.startup()

        failed1.aclose.assert_called_once()
        failed2.aclose.assert_called_once()
        good.aclose.assert_not_called()
        assert mw._redis is good

    async def test_startup_closes_client_when_all_retries_exhausted(self):
        mw = RedisDeduplicationMiddleware(
            redis_url="redis://localhost",
            startup_retries=2,
            startup_retry_delay=0.01,
        )
        failed1, failed2 = AsyncMock(), AsyncMock()
        failed1.ping.side_effect = ConnectionError("fail")
        failed2.ping.side_effect = ConnectionError("fail")

        with patch("redis.asyncio.Redis.from_url", side_effect=[failed1, failed2]):
            with pytest.raises(ConnectionError):
                await mw.startup()

        failed1.aclose.assert_called_once()
        failed2.aclose.assert_called_once()


class TestLabelTypeParsing:
    async def test_bool_label_false_disables_dedup(self, fake_redis, make_message):
        mw = RedisDeduplicationMiddleware(redis_url="redis://localhost")
        mw._redis = fake_redis
        await mw.pre_send(make_message(labels={DEDUP_LABEL: False}))
        await mw.pre_send(make_message(labels={DEDUP_LABEL: False}))

    async def test_bool_label_true_enables_dedup(self, middleware, make_message):
        msg = make_message(labels={DEDUP_LABEL: True})
        await middleware.pre_send(msg)
        with pytest.raises(DuplicateTaskError):
            await middleware.pre_send(make_message(labels={DEDUP_LABEL: True}))

    async def test_string_bool_label_true_enables_dedup(self, middleware, make_message):
        # taskiq's prepare_label() stringifies True → "True" before pre_send runs
        msg = make_message(labels={DEDUP_LABEL: "True"})
        await middleware.pre_send(msg)
        with pytest.raises(DuplicateTaskError):
            await middleware.pre_send(make_message(labels={DEDUP_LABEL: "True"}))

    async def test_string_bool_label_false_disables_dedup(
        self, fake_redis, make_message
    ):
        # taskiq's prepare_label() stringifies False → "False" before pre_send runs
        mw = RedisDeduplicationMiddleware(redis_url="redis://localhost")
        mw._redis = fake_redis
        await mw.pre_send(make_message(labels={DEDUP_LABEL: "False"}))
        await mw.pre_send(make_message(labels={DEDUP_LABEL: "False"}))

    async def test_key_fields_list_parsed_correctly(self, middleware, make_message):
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

    async def test_key_fields_non_list_ignored(self, middleware, make_message):
        m = make_message(
            kwargs={"a": 1},
            labels={DEDUP_KEY_FIELDS_LABEL: "not-a-list"},
        )
        key = middleware._build_deduplication_key(m)
        assert key is not None

    async def test_invalid_bool_label_warns_and_uses_default(
        self, middleware, make_message, caplog
    ):
        import logging

        msg = make_message(labels={DEDUP_LABEL: "yes"})
        with caplog.at_level(logging.WARNING, logger="taskiq_deduplication.middleware"):
            await middleware.pre_send(msg)
        assert any("yes" in r.message for r in caplog.records)

    async def test_string_true_lowercase_enables_dedup(self, middleware, make_message):
        msg = make_message(labels={DEDUP_LABEL: "true"})
        await middleware.pre_send(msg)
        with pytest.raises(DuplicateTaskError):
            await middleware.pre_send(make_message(labels={DEDUP_LABEL: "true"}))

    async def test_string_false_lowercase_disables_dedup(
        self, fake_redis, make_message
    ):
        mw = RedisDeduplicationMiddleware(redis_url="redis://localhost")
        mw._redis = fake_redis
        await mw.pre_send(make_message(labels={DEDUP_LABEL: "false"}))
        await mw.pre_send(make_message(labels={DEDUP_LABEL: "false"}))

    async def test_invalid_key_fields_label_warns_and_falls_back_to_all_kwargs(
        self, middleware, make_message, caplog
    ):
        import logging

        msg = make_message(kwargs={"a": 1}, labels={DEDUP_KEY_FIELDS_LABEL: "user_id"})
        with caplog.at_level(logging.WARNING, logger="taskiq_deduplication.middleware"):
            key = middleware._build_deduplication_key(msg)
        assert any("user_id" in r.message for r in caplog.records)
        # falls back to full-kwargs fingerprint — key must still be produced
        assert key is not None

    async def test_key_fields_string_parses_to_non_list_warns(
        self, middleware, make_message, caplog
    ):
        import logging

        # ast.literal_eval succeeds but returns a dict, not a list
        msg = make_message(kwargs={"a": 1}, labels={DEDUP_KEY_FIELDS_LABEL: "{'a': 1}"})
        with caplog.at_level(logging.WARNING, logger="taskiq_deduplication.middleware"):
            key = middleware._build_deduplication_key(msg)
        assert any("{'a': 1}" in r.message for r in caplog.records)
        assert key is not None

    def test_stringified_key_fields_parsed_correctly(self, middleware, make_message):
        # taskiq's prepare_label() stringifies ["a", "b"] → "['a', 'b']" before pre_send
        m1 = make_message(
            kwargs={"a": 1, "b": 2, "c": 3},
            labels={DEDUP_KEY_FIELDS_LABEL: "['a', 'b']"},
        )
        m2 = make_message(
            kwargs={"a": 1, "b": 2, "c": 999},
            labels={DEDUP_KEY_FIELDS_LABEL: "['a', 'b']"},
        )
        assert middleware._build_deduplication_key(
            m1
        ) == middleware._build_deduplication_key(m2)

    def test_stringified_key_fields_different_included_fields(
        self, middleware, make_message
    ):
        m1 = make_message(
            kwargs={"a": 1, "b": 2},
            labels={DEDUP_KEY_FIELDS_LABEL: "['a']"},
        )
        m2 = make_message(
            kwargs={"a": 99, "b": 2},
            labels={DEDUP_KEY_FIELDS_LABEL: "['a']"},
        )
        assert middleware._build_deduplication_key(
            m1
        ) != middleware._build_deduplication_key(m2)

    async def test_invalid_ttl_string_warns_and_uses_default(
        self, middleware, fake_redis, make_message, caplog
    ):
        import logging

        msg = make_message(labels={DEDUP_TTL_LABEL: "oops"})
        with caplog.at_level(logging.WARNING, logger="taskiq_deduplication.middleware"):
            await middleware.pre_send(msg)
        assert any("oops" in r.message for r in caplog.records)
        key = middleware._build_deduplication_key(msg)
        ttl = await fake_redis.ttl(key)
        assert 0 < ttl <= middleware.default_ttl

    async def test_invalid_ttl_none_warns_and_uses_default(
        self, middleware, fake_redis, make_message, caplog
    ):
        import logging

        msg = make_message(labels={DEDUP_TTL_LABEL: None})
        with caplog.at_level(logging.WARNING, logger="taskiq_deduplication.middleware"):
            await middleware.pre_send(msg)
        assert any("None" in r.message for r in caplog.records)
        key = middleware._build_deduplication_key(msg)
        ttl = await fake_redis.ttl(key)
        assert 0 < ttl <= middleware.default_ttl


class TestKeyCaching:
    async def test_key_cached_during_pre_send(self, middleware, make_message):
        from taskiq_deduplication.middleware import _CACHED_KEY_LABEL

        msg = make_message()
        await middleware.pre_send(msg)
        assert _CACHED_KEY_LABEL in msg.labels

    async def test_post_execute_uses_cached_key(
        self, middleware, fake_redis, make_message, make_result
    ):
        from taskiq_deduplication.middleware import _CACHED_KEY_LABEL

        msg = make_message()
        await middleware.pre_send(msg)
        cached_key = msg.labels.get(_CACHED_KEY_LABEL)
        await middleware.post_execute(msg, make_result())
        assert not await fake_redis.exists(cached_key)

    async def test_on_error_uses_cached_key(
        self, middleware, fake_redis, make_message, make_result
    ):
        from taskiq_deduplication.middleware import _CACHED_KEY_LABEL

        msg = make_message()
        await middleware.pre_send(msg)
        cached_key = msg.labels.get(_CACHED_KEY_LABEL)
        await middleware.on_error(msg, make_result(is_err=True), RuntimeError("boom"))
        assert not await fake_redis.exists(cached_key)

    async def test_cached_key_none_when_key_build_fails(self, middleware, make_message):
        from taskiq_deduplication.middleware import _CACHED_KEY_LABEL

        msg = make_message(kwargs={"dt": object()})
        await middleware.pre_send(msg)
        assert msg.labels[_CACHED_KEY_LABEL] is None

    async def test_post_execute_noop_when_cached_key_is_none(
        self, middleware, fake_redis, make_message, make_result
    ):
        msg = make_message(kwargs={"dt": object()})
        await middleware.pre_send(msg)
        await middleware.post_execute(msg, make_result())

    async def test_on_error_noop_when_cached_key_is_none(
        self, middleware, fake_redis, make_message, make_result
    ):
        msg = make_message(kwargs={"dt": object()})
        await middleware.pre_send(msg)
        await middleware.on_error(msg, make_result(is_err=True), RuntimeError("boom"))

    async def test_release_if_owned_raises_without_redis(
        self, middleware, make_message
    ):
        middleware._redis = None
        with pytest.raises(RuntimeError, match="startup"):
            await middleware._release_if_owned("some-key", "some-task")


class TestTTLExpiry:
    async def test_lock_expiry_admits_duplicate(
        self, middleware, fake_redis, make_message
    ):
        msg = make_message()
        await middleware.pre_send(msg)
        # simulate TTL expiry by deleting the key
        await fake_redis.delete(middleware._build_deduplication_key(msg))
        # same fingerprint should now pass since lock is gone
        await middleware.pre_send(make_message())


class TestHeartbeat:
    async def test_pre_execute_starts_heartbeat(self, middleware, make_message):
        msg = make_message()
        await middleware.pre_send(msg)
        await middleware.pre_execute(msg)
        assert msg.task_id in middleware._heartbeats
        await middleware._cancel_heartbeat(msg.task_id)

    async def test_pre_execute_noop_when_heartbeat_disabled(
        self, fake_redis, make_message
    ):
        mw = RedisDeduplicationMiddleware(
            redis_url="redis://localhost", heartbeat=False
        )
        mw._redis = fake_redis
        msg = make_message()
        await mw.pre_send(msg)
        await mw.pre_execute(msg)
        assert msg.task_id not in mw._heartbeats

    async def test_pre_execute_noop_without_cached_key(self, middleware, make_message):
        # deduplication disabled -> pre_send never caches a key
        msg = make_message(labels={DEDUP_LABEL: False})
        await middleware.pre_send(msg)
        await middleware.pre_execute(msg)
        assert msg.task_id not in middleware._heartbeats

    async def test_heartbeat_refreshes_ttl(self, middleware, fake_redis, make_message):
        import asyncio

        # short ttl, tiny heartbeat interval so the lock would expire without refresh
        middleware.heartbeat_interval = 0.05
        msg = make_message(labels={DEDUP_TTL_LABEL: 1})
        await middleware.pre_send(msg)
        key = middleware._build_deduplication_key(msg)
        await middleware.pre_execute(msg)
        try:
            # let several heartbeats elapse — longer than the original 1s ttl
            await asyncio.sleep(0.3)
            assert await fake_redis.exists(key)
            ttl = await fake_redis.ttl(key)
            assert 0 < ttl <= 1
        finally:
            await middleware._cancel_heartbeat(msg.task_id)

    async def test_release_lock_cancels_heartbeat(
        self, middleware, fake_redis, make_message, make_result
    ):
        msg = make_message()
        await middleware.pre_send(msg)
        await middleware.pre_execute(msg)
        assert msg.task_id in middleware._heartbeats
        await middleware.post_execute(msg, make_result())
        assert msg.task_id not in middleware._heartbeats
        key = middleware._build_deduplication_key(msg)
        assert not await fake_redis.exists(key)

    async def test_heartbeat_stops_when_lock_lost(
        self, middleware, fake_redis, make_message
    ):
        import asyncio

        middleware.heartbeat_interval = 0.05
        msg = make_message(labels={DEDUP_TTL_LABEL: 1})
        await middleware.pre_send(msg)
        key = middleware._build_deduplication_key(msg)
        await middleware.pre_execute(msg)
        # another task steals the key
        await fake_redis.set(key, "other-task", ex=10)
        await asyncio.sleep(0.15)
        task = middleware._heartbeats.get(msg.task_id)
        # heartbeat loop should have returned on its own
        assert task is None or task.done()
        await middleware._cancel_heartbeat(msg.task_id)

    async def test_shutdown_cancels_heartbeats(self, fake_redis, make_message):
        mw = RedisDeduplicationMiddleware(redis_url="redis://localhost")
        mw._redis = fake_redis
        msg = make_message()
        await mw.pre_send(msg)
        await mw.pre_execute(msg)
        assert msg.task_id in mw._heartbeats
        await mw.shutdown()
        assert not mw._heartbeats

    async def test_default_heartbeat_interval_is_third_of_ttl(self, middleware):
        assert middleware._get_heartbeat_interval(300) == 100.0
        assert middleware._get_heartbeat_interval(1) == 1.0

    async def test_explicit_heartbeat_interval_overrides(self, fake_redis):
        mw = RedisDeduplicationMiddleware(
            redis_url="redis://localhost", heartbeat_interval=5.0
        )
        mw._redis = fake_redis
        assert mw._get_heartbeat_interval(300) == 5.0

    async def test_refresh_if_owned_raises_without_redis(self, middleware):
        middleware._redis = None
        with pytest.raises(RuntimeError, match="startup"):
            await middleware._refresh_if_owned("some-key", "some-task", 60)

    async def test_heartbeat_continues_after_refresh_error(
        self, middleware, make_message, caplog
    ):
        import asyncio
        import logging

        middleware.heartbeat_interval = 0.02
        # first refresh raises, subsequent ones succeed; the loop must survive
        middleware._refresh_if_owned = AsyncMock(
            side_effect=[ConnectionError("boom"), True, True, True]
        )
        msg = make_message()
        await middleware.pre_send(msg)
        with caplog.at_level(logging.WARNING, logger="taskiq_deduplication.middleware"):
            await middleware.pre_execute(msg)
            await asyncio.sleep(0.1)
            task = middleware._heartbeats.get(msg.task_id)
            # loop swallowed the error and kept running
            assert task is not None and not task.done()
            await middleware._cancel_heartbeat(msg.task_id)
        assert any("Failed to refresh lock" in r.message for r in caplog.records)
        assert middleware._refresh_if_owned.call_count >= 2


class TestExplicitKeyEdgeCases:
    def test_empty_string_key_produces_prefix_only_key(self, middleware, make_message):
        m = make_message(labels={DEDUP_EXPLICIT_KEY_LABEL: ""})
        key = middleware._build_deduplication_key(m)
        assert key == "taskiq:deduplication:"

    async def test_empty_string_key_acquires_lock(
        self, middleware, fake_redis, make_message
    ):
        msg = make_message(labels={DEDUP_EXPLICIT_KEY_LABEL: ""})
        await middleware.pre_send(msg)
        assert await fake_redis.exists("taskiq:deduplication:")
