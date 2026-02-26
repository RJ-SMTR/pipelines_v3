# -*- coding: utf-8 -*-
from prefect import flow, task
# import sentry_sdk
# from pipelines.common.utils.secret import get_secret
from time import sleep

from pipelines.common.utils.state_handlers import handler_post_sentry

@task
def task_raises_exception():
    print("Iniciando o flow test__raise_for_sentry")
    sleep(6)
    err = 1/0

# @task
# def initialize_sentry():
#     print("Inicializando Sentry SDK")
#     sentry_dsn = get_secret("sentry", "dsn")['dsn']
#     environment = 'staging'
#     print('Inicializando sentry_sdk com DSN:', sentry_dsn, "vartype: ", type(sentry_dsn))
#     sentry_sdk.init(
#         dsn=sentry_dsn,
#         traces_sample_rate=0,
#         environment=environment,
#     )

@flow(
        log_prints=True, 
        on_running=[handler_post_sentry]
)
def test__raise_for_sentry() -> list[str]:
    # initialize_sentry()
    task_raises_exception()
    # raise(ValueError("Erro proposital para testar integração com Sentry"))


if __name__ == "__main__":
    test__raise_for_sentry()