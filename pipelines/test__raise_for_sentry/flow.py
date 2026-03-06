# -*- coding: utf-8 -*-
from time import sleep

import sentry_sdk
from prefect import flow, task

from pipelines.common.utils.secret import get_env_secret

# from pipelines.common.utils.state_handlers import handler_post_sentry


@task
def task_raises_exception():
    print("Iniciando o flow test__raise_for_sentry")
    sleep(2)
    raise (KeyError("Erro proposital para testar integração com Sentry, rodando deployado"))


@task
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


if __name__ == "__main__":
    test__raise_for_sentry()
