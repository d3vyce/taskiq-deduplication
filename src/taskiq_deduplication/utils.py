from collections.abc import Awaitable
from typing import cast

from redis.asyncio import Redis


async def check_and_delete(redis: Redis, key: str, owner: str) -> bool:
    """Delete *key* only if its value equals *owner*. Returns True if deleted."""
    release_script = """
    if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('del', KEYS[1])
    else
        return 0
    end
    """

    released = await cast(Awaitable[int], redis.eval(release_script, 1, key, owner))
    return bool(released)
