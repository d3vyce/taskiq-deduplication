from redis.asyncio import Redis
from redis.commands.core import AsyncScript

RELEASE_LUA_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
"""


async def check_and_delete(redis: Redis, key: str, owner: str) -> bool:
    """Delete *key* only if its value equals *owner*.

    Args:
        redis: Async Redis client.
        key: Lock key to delete.
        owner: Expected value of the key (task_id).

    Returns:
        True if the key was deleted, False otherwise.
    """
    script: AsyncScript = redis.register_script(RELEASE_LUA_SCRIPT)
    released: int = await script(keys=[key], args=[owner])
    return bool(released)
