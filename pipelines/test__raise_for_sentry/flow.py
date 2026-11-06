# -*- coding: utf-8 -*-
from time import sleep
from unittest.mock import patch
from uuid import uuid4

import redis
import sentry_sdk
from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.common.utils.prefect import flow
from pipelines.common.utils.redis import get_redis_client
from pipelines.common.utils.redis_pal import RedisClusterPal, RedisPal
from pipelines.common.utils.secret import get_env_secret
from pipelines.common.utils.utils import is_running_locally

# from pipelines.common.utils.state_handlers import handler_post_sentry


@task(cache_policy=NO_CACHE)
def task_raises_exception():
    print("Iniciando o flow test__raise_for_sentry")
    sleep(2)
    raise (KeyError("Erro proposital para testar integração com Sentry, rodando deployado"))


@task(cache_policy=NO_CACHE)
def initialize_sentry():
    print("Inicializando Sentry SDK")
    sentry_dsn = get_env_secret("sentry", "dsn")["dsn"]
    environment = "staging"
    print("Inicializando sentry_sdk com DSN:", sentry_dsn, "vartype: ", type(sentry_dsn))
    sentry_sdk.init(
        dsn=sentry_dsn,
        # debug=True,
        # enable_logs=True,
        # traces_sample_rate=0,
        environment=environment,
    )


@flow(
    log_prints=True,
)
def test__raise_for_sentry() -> list[str]:
    initialize_sentry()
    task_raises_exception()


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
def test__redis_pal_compatibility() -> dict[str, object]:
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


if __name__ == "__main__":
    test__raise_for_sentry()
