# -*- coding: utf-8 -*-
from prefect import flow, task
from prefect.flows import Flow


@task
def run_subflow(env: str, suflow: Flow, parameters: list[dict], deployment_name: str | None = None):
    pass


@flow(log_prints=True)
def teste__flow_com_subflow():
    pass
