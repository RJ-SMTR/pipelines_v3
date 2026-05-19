# -*- coding: utf-8 -*-
# registra flow
from time import sleep

from prefect import flow, task


@task
def wait_seconds(seconds):
    print(f"Esperando {seconds} segundos...")
    sleep(seconds)
    print("Finalizado!")


@task
def raise_error():
    raise Exception("Erro teste")


@flow(log_prints=True)
def teste__subflow(seconds=1, fail=False):  # noqa: PT028
    wait_seconds(seconds=seconds)
    if fail:
        raise_error()
