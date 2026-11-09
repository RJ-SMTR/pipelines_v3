# -*- coding: utf-8 -*-
from unittest.mock import patch
from uuid import uuid4

import redis

from pipelines.common.utils.prefect import flow
from pipelines.common.utils.redis import get_redis_client
from pipelines.common.utils.redis_pal import RedisClusterPal, RedisPal
from pipelines.common.utils.utils import is_running_locally


def _test_redis_pal_methods(client_class, redis_client_class) -> None:
    stored_values = {}

    def fake_set(_self, *_args, name, value, **_kwargs):
        stored_values[name] = value
        return True

    def fake_get(_self, *_args, name, **_kwargs):
        return stored_values.get(name)

    client = client_class.__new__(client_class)
    expected_value = {"source": "redis-pal-compatibility", "items": [1, 2, 3]}

    with (
        patch.object(redis_client_class, "set", fake_set),
        patch.object(redis_client_class, "get", fake_get),
    ):
        assert client.set("compatibility", expected_value)
        assert client.get("compatibility") == expected_value

        result = client.get_with_timestamp("compatibility")
        assert result["value"] == expected_value
        assert result["last_modified"] > 0


@flow(log_prints=True)
def test__test_redis_cluster_compatibility() -> dict[str, object]:
    _test_redis_pal_methods(RedisPal, redis.Redis)
    _test_redis_pal_methods(RedisClusterPal, redis.RedisCluster)

    client = get_redis_client()
    expected_client_class = RedisPal if is_running_locally() else RedisClusterPal
    assert isinstance(client, expected_client_class)

    key = f"test.redis_pal_compatibility.{uuid4().hex}"
    expected_value = {"source": "redis-pal-compatibility", "value": 42}

    try:
        assert client.set(key, expected_value)
        assert client.get(key) == expected_value

        result = client.get_with_timestamp(key)
        assert result["value"] == expected_value
        assert result["last_modified"] > 0
    finally:
        client.delete(key)
        client.delete(f"{key}_timestamp")

    print(f"Redis compatibility checks passed with {type(client).__name__}")
    return {"client": type(client).__name__, "value": expected_value}


# @flow(log_prints=True)
# def test__test_redis_cluster_compatibility() -> list[str]:
#     use_pandas()
#     customer_ids = get_customer_ids()
#     results = process_customer.map(customer_ids)
#     return results
