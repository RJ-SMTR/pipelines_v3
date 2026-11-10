# -*- coding: utf-8 -*-
from typing import Optional, Union

from pipelines.common.utils.redis_pal import (
    REDIS_ERRORS,
    MigrationRedisPal,
    RedisClusterPal,
    RedisPal,
)
from pipelines.common.utils.utils import is_running_locally

OLD_REDIS_HOST = "redis.redis.svc.cluster.local"
NEW_REDIS_HOST = "redis-cluster-leader.redis.svc.cluster.local"


def get_redis_client(
    port: int = 6379,
    password: Optional[str] = None,
) -> Union[RedisPal, MigrationRedisPal]:
    """
    Returns a standalone Redis client locally and a dual-write client otherwise.

    During the migration to Redis Cluster, writes go to both the old Redis and
    the cluster, and reads return the most recent value between them.
    """
    if is_running_locally():
        return RedisPal(host="localhost", port=port, password=password)

    old = RedisPal(host=OLD_REDIS_HOST, port=port, password=password)
    try:
        new = RedisClusterPal(
            host=NEW_REDIS_HOST,
            port=port,
            password=password,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
    except REDIS_ERRORS as error:
        print(f"Falha ao conectar ao Redis Cluster, usando apenas o Redis antigo: {error}")
        new = None
    return MigrationRedisPal(old=old, new=new)
