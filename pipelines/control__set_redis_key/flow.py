# -*- coding: utf-8 -*-
"""
Flow de controle para alteração de valores em Redis

Executa a alteração de chaves e valores em Redis via API de controle manual.

Common: 2026-03-10
"""

from prefect import flow

from pipelines.control__set_redis_key.tasks import set_redis_keys


@flow(log_prints=True)
def control__set_redis_key(keys: dict):
    """
    Flow para alterar valores de chaves em Redis.

    Args:
        keys: Dicionário com chaves e valores a serem definidos em Redis
    """
    set_redis_keys(keys=keys)
