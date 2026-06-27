"""Integration tests against a live Redis instance (localhost:6379/15).

These tests are skipped automatically when Redis is not reachable.
In CI a Redis service is started before the test step.
"""

import pytest

from taskiq_deduplication import DuplicateTaskError, RedisDeduplicationMiddleware
from taskiq_deduplication.utils import REFRESH_LUA_SCRIPT, RELEASE_LUA_SCRIPT


@pytest.fixture
def mw(real_redis):
    middleware = RedisDeduplicationMiddleware(redis_url="redis://localhost:6379/15")
    middleware._redis = real_redis
    middleware._release_script = real_redis.register_script(RELEASE_LUA_SCRIPT)
    middleware._refresh_script = real_redis.register_script(REFRESH_LUA_SCRIPT)
    return middleware


@pytest.mark.integration
async def test_full_lifecycle(mw, real_redis, make_message, make_result):
    msg = make_message()
    await mw.pre_send(msg)
    key = mw._build_deduplication_key(msg)
    assert await real_redis.exists(key)

    await mw.post_execute(msg, make_result())
    assert not await real_redis.exists(key)

    await mw.pre_send(make_message())


@pytest.mark.integration
async def test_duplicate_rejected(mw, make_message):
    await mw.pre_send(make_message())
    with pytest.raises(DuplicateTaskError):
        await mw.pre_send(make_message())


@pytest.mark.integration
async def test_redispatch_after_on_error(mw, make_message, make_result):
    msg = make_message()
    await mw.pre_send(msg)
    await mw.on_error(msg, make_result(is_err=True), RuntimeError("boom"))
    await mw.pre_send(make_message())


@pytest.mark.integration
async def test_lua_only_owner_can_release(mw, real_redis, make_message):
    owner_msg = make_message(task_id="owner")
    key = mw._build_deduplication_key(owner_msg)
    await real_redis.set(key, "owner", ex=60)

    await mw._release_if_owned(key, "intruder")
    assert await real_redis.exists(key)

    await mw._release_if_owned(key, "owner")
    assert not await real_redis.exists(key)


@pytest.mark.integration
async def test_ttl_is_applied(mw, real_redis, make_message):
    msg = make_message()
    await mw.pre_send(msg)
    key = mw._build_deduplication_key(msg)
    ttl = await real_redis.ttl(key)
    assert 0 < ttl <= mw.default_ttl


@pytest.mark.integration
async def test_heartbeat_keeps_long_running_lock_alive(
    mw, real_redis, make_message, make_result
):
    import asyncio

    from taskiq_deduplication.middleware import DEDUP_TTL_LABEL

    # 1s TTL with a sub-second heartbeat: without refresh the lock would expire.
    mw.heartbeat_interval = 0.2
    msg = make_message(labels={DEDUP_TTL_LABEL: 1})
    await mw.pre_send(msg)
    key = mw._build_deduplication_key(msg)
    await mw.pre_execute(msg)
    try:
        # outlive the original TTL; the heartbeat should keep the lock present
        await asyncio.sleep(1.5)
        assert await real_redis.exists(key)
        with pytest.raises(DuplicateTaskError):
            await mw.pre_send(make_message(labels={DEDUP_TTL_LABEL: 1}))
    finally:
        await mw.post_execute(msg, make_result())
    assert not await real_redis.exists(key)


@pytest.mark.integration
async def test_explicit_key_end_to_end(mw, real_redis, make_message, make_result):
    from taskiq_deduplication.middleware import DEDUP_EXPLICIT_KEY_LABEL

    msg = make_message(labels={DEDUP_EXPLICIT_KEY_LABEL: "my-lock"})
    await mw.pre_send(msg)
    assert await real_redis.exists("taskiq:deduplication:my-lock")

    with pytest.raises(DuplicateTaskError):
        await mw.pre_send(
            make_message(
                kwargs={"different": "kwargs"},
                labels={DEDUP_EXPLICIT_KEY_LABEL: "my-lock"},
            )
        )

    await mw.post_execute(msg, make_result())
    assert not await real_redis.exists("taskiq:deduplication:my-lock")
