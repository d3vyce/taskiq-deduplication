import ast
import logging
from typing import Any

logger = logging.getLogger(__name__)

RELEASE_LUA_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
"""

REFRESH_LUA_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('expire', KEYS[1], ARGV[2])
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


async def check_and_refresh(script: Any, key: str, owner: str, ttl: int) -> bool:
    """Extend *key*'s TTL to *ttl* only if its value equals *owner*.

    Args:
        script: Pre-registered Lua script object (from ``Redis.register_script``).
        key: Lock key to refresh.
        owner: Expected value of the key (task_id).
        ttl: New TTL in seconds.

    Returns:
        True if the TTL was extended, False if the key is missing or owned by
        another task.
    """
    refreshed: int = await script(keys=[key], args=[owner, ttl])
    return bool(refreshed)


def parse_bool_label(value: Any, default: bool, label_name: str = "") -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lower = value.lower()
        if lower == "true":
            return True
        if lower == "false":
            return False
    if value is not None:
        logger.warning(
            "Invalid %r value %r (expected bool); falling back to default (%r).",
            label_name,
            value,
            default,
        )
    return default


def parse_list_label(value: Any, label_name: str = "") -> list[str] | None:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = ast.literal_eval(value)
            if isinstance(parsed, list):
                return parsed
        except (ValueError, SyntaxError):
            pass
    if value is not None:
        logger.warning(
            "Invalid %r value %r (expected list[str]); ignoring.",
            label_name,
            value,
        )
    return None


def parse_int_label(value: Any, default: int, label_name: str = "") -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        logger.warning(
            "Invalid %r value %r (expected int); falling back to default (%d).",
            label_name,
            value,
            default,
        )
        return default
