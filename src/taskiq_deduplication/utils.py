from typing import Any

RELEASE_LUA_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
"""


async def check_and_delete(script: Any, key: str, owner: str) -> bool:
    """Delete *key* only if its value equals *owner*.

    Args:
        script: Pre-registered Lua script object (from ``Redis.register_script``).
        key: Lock key to delete.
        owner: Expected value of the key (task_id).

    Returns:
        True if the key was deleted, False otherwise.
    """
    released: int = await script(keys=[key], args=[owner])
    return bool(released)
