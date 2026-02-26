# -*- coding: utf-8 -*-
from prefect import flow
from time import sleep

from pipelines.common.utils.state_handlers import handler_post_sentry



@flow(
        log_prints=True, 
        on_running=[handler_post_sentry]
)
def test__raise_for_sentry() -> list[str]:
    print("Iniciando o flow test__raise_for_sentry")
    sleep(5)
    err = 1/0
    # raise(ValueError("Erro proposital para testar integração com Sentry"))


if __name__ == "__main__":
    test__raise_for_sentry()