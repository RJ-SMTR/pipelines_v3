# -*- coding: utf-8 -*-
from prefect import flow, task
from time import sleep

from pipelines.common.utils.state_handlers import handler_post_sentry

@task
def task_raises_exception():
    print("Iniciando o flow test__raise_for_sentry")
    sleep(6)
    err = 1/0

@flow(
        log_prints=True, 
        on_running=[handler_post_sentry]
)
def test__raise_for_sentry() -> list[str]:
    task_raises_exception()
    # raise(ValueError("Erro proposital para testar integração com Sentry"))


if __name__ == "__main__":
    test__raise_for_sentry()