# -*- coding: utf-8 -*-
from typing import Optional

from redis_pal import RedisPal

from pipelines.common.utils.utils import is_running_locally


def get_redis_client(
    host: str = "redis.redis.svc.cluster.local",
    port: int = 6379,
    db: int = 0,  # pylint: disable=C0103
    password: Optional[str] = None,
) -> RedisPal:
    """
    Returns a Redis client.
    """
    if is_running_locally():
        host = "localhost"
    return RedisPal(
        host=host,
        port=port,
        db=db,
        password=password,
    )
