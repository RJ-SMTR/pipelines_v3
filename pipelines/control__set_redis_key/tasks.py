# -*- coding: utf-8 -*-
"""Tasks para controle de Redis"""

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.common.utils.redis import get_redis_client


@task(cache_policy=NO_CACHE)
def set_redis_keys(keys: dict):
    """
    Define múltiplas chaves e valores em Redis.

    Args:
        keys: Dicionário com chaves e valores a serem definidos em Redis
    """
    client = get_redis_client()
    for key, value in keys.items():
        current_value = client.get(key)
        print(f"key = {key}\nvalor atual = {current_value}\nNovo valor = {value}")
        client.set(key, value)
