# -*- coding: utf-8 -*-
from typing import Optional

from prefect import runtime

from pipelines.common.tasks import get_run_env, initialize_sentry, run_subflow, setup_environment
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.common.utils.prefect import flow
from pipelines.control__jae_verificacao_captura.flow import control__jae_verificacao_captura
from pipelines.treatment__jae_timestamps_divergentes import constants
from pipelines.treatment__jae_timestamps_divergentes.tasks import (
    create_downstream_subflows_params,
    create_materialization_subflows_params,
    create_recapture_subflows_params,
    create_transacao_valor_ordem_params,
    create_verificacao_captura_params,
    get_gaps_from_result_table,
    run_updates,
)
from pipelines.treatment__transacao_valor_ordem.flow import treatment__transacao_valor_ordem


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run, timeout_seconds=18000)
async def treatment__jae_timestamps_divergentes(
    env: Optional[str] = None,
    datetime_start: Optional[str] = None,
    datetime_end: Optional[str] = None,
):
    deployment_name = runtime.deployment.name

    env = get_run_env(
        env=env,
        deployment_name=deployment_name,
    )

    setup_enviroment = setup_environment(env=env)

    sentry = initialize_sentry(env=env)

    gaps = get_gaps_from_result_table(
        env=env,
        table_ids=constants.GAP_REPAIR_REGISTRY.keys(),
        timestamp_start=datetime_start,
        timestamp_end=datetime_end,
        wait_for=[setup_enviroment, sentry],
    )

    subflow_capture_params = create_recapture_subflows_params(gaps=gaps)

    for param in subflow_capture_params:
        await run_subflow(env=env, flow=param["flow"], parameters=param["params"])

    subflow_materialization_params = create_materialization_subflows_params(gaps=gaps)

    for param in subflow_materialization_params:
        await run_subflow(env=env, flow=param["flow"], parameters=param["params"])

    sql_treatment = run_updates(env=env, gaps=gaps)

    downstream_flows = create_downstream_subflows_params(gaps=gaps)

    for downstream_flow in downstream_flows:
        await run_subflow(env=env, flow=downstream_flow, wait_for=[sql_treatment])

    run_transacao_valor_ordem, params = create_transacao_valor_ordem_params(gaps=gaps)

    if run_transacao_valor_ordem:
        await run_subflow(
            env=env,
            flow=treatment__transacao_valor_ordem,
            parameters=[params],
        )

    verificacao_captura_params = create_verificacao_captura_params(gaps=gaps)

    for param in verificacao_captura_params:
        await run_subflow(
            env=env,
            flow=control__jae_verificacao_captura,
            parameters=[param],
        )
