# -*- coding: utf-8 -*-
# registra
import asyncio

from prefect import flow, runtime, task
from prefect.deployments import run_deployment
from prefect.flows import Flow

from pipelines.common.tasks import get_run_env
from pipelines.teste__subflow.flow import teste__subflow


@task
async def run_subflow(
    env: str,
    flow: Flow,
    parameters: list[dict] | None = None,
    maximum_parallelism: int = 1,
    deployment_name: str | None = None,
):
    parameters = parameters or [{}]

    flow_name = flow.name
    flow_type, pipeline = flow_name.split("--", maxsplit=1)

    flow_env = "prod" if env == "prod" else "staging"

    deployment_name = deployment_name or f"rj-{flow_type}--{pipeline.replace('-', '_')}--{flow_env}"

    deployment_name = f"{flow_name}/{deployment_name}"

    semaphore = asyncio.Semaphore(maximum_parallelism)

    async def _run(params):
        async with semaphore:
            return await run_deployment(
                name=deployment_name,
                parameters=params,
            )

    coroutines = [_run(params) for params in parameters]
    runs = await asyncio.gather(*coroutines)

    for run in runs:
        run.state.result()

    return runs


@flow(log_prints=True)
async def teste__flow_com_subflow():
    env = get_run_env(env=None, deployment_name=runtime.deployment.name)
    await run_subflow(
        env=env, flow=teste__subflow, parameters=[{"seconds": 1}, {"seconds": 2}, {"seconds": 3}]
    )

    await run_subflow(
        env=env,
        flow=teste__subflow,
        parameters=[{"seconds": 10}, {"seconds": 10}, {"seconds": 10}],
        maximum_parallelism=2,
    )

    await run_subflow(
        env=env,
        flow=teste__subflow,
        parameters=[
            {
                "fail": True,
            }
        ],
    )

    await run_subflow(
        env=env,
        flow=teste__subflow,
    )
