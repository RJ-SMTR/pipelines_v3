# -*- coding: utf-8 -*-
from typing import Any, Optional, Union

from pipelines.common.utils.redis_pal import RedisClusterPal, RedisPal
from pipelines.common.utils.utils import is_running_locally


def get_redis_client(
    host: str = "redis-cluster-leader.redis.svc.cluster.local",
    port: int = 6379,
    db: int = 0,  # pylint: disable=C0103
    password: Optional[str] = None,
    **kwargs: Any,
) -> Union[RedisPal, RedisClusterPal]:
    """
    Returns a standalone Redis client locally and a cluster client otherwise.

    Additional keyword arguments are passed to RedisClusterPal when running
    outside a local environment.
    """
    if is_running_locally():
        return RedisPal(host="localhost", port=port, db=db, password=password)
    if db != 0:
        raise ValueError("Redis Cluster only supports database 0")
    cluster_kwargs = {"password": password, **kwargs}
    if "startup_nodes" not in cluster_kwargs:
        cluster_kwargs.update(host=host, port=port)
    return RedisClusterPal(**cluster_kwargs)
