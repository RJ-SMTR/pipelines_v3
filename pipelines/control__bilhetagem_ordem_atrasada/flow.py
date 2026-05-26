# -*- coding: utf-8 -*-
"""
Flows de tratamento dos dados financeiros
"""

from prefect import runtime

from pipelines.capture.jae.flows import (
    CAPTURA_INTEGRACAO,
    CAPTURA_ORDEM_PAGAMENTO,
    CAPTURA_TRANSACAO_ORDEM,
)
from pipelines.capture__jae_auxiliar import flow
from pipelines.common.tasks import (
    get_run_env,
    get_scheduled_timestamp,
    initialize_sentry,
    run_subflow,
    setup_environment,
)
from pipelines.control__bilhetagem_ordem_atrasada import constants as jae_constants
from pipelines.tasks import (
    parse_timestamp_to_string,
)
from pipelines.treatment.bilhetagem.flows import (
    INTEGRACAO_MATERIALIZACAO,
    TRANSACAO_ORDEM_MATERIALIZACAO,
)
from pipelines.treatment.financeiro.flows import (
    FINANCEIRO_BILHETAGEM_MATERIALIZACAO,
    ordem_pagamento_quality_check,
)

sources = jae_constants.sources


@flow(name="financeiro_bilhetagem: ordem atrasada - captura/tratamento")
async def ordem_atrasada(timestamp: str | None = None, env: str | None = None):
    deployment_name = runtime.deployment.name
    env = get_run_env(env=env, deployment_name=deployment_name)
    sentry = initialize_sentry(env=env)
    setup_env = setup_environment(env=env)

    timestamp = get_scheduled_timestamp(wait_for=[sentry, setup_env])

    run_recapture = await run_subflow(
        flow_name=CAPTURA_ORDEM_PAGAMENTO,
        parameters=[
            {
                "table_id": s.table_id,
                "recapture": True,
            }
            for s in sources
        ],
        maximum_parallelism=3,
    )

    run_capture = await run_subflow(
        flow_name=CAPTURA_ORDEM_PAGAMENTO,
        parameters=[
            {
                "table_id": s.table_id,
                "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "recapture": False,
            }
            for s in sources
        ],
        maximum_parallelism=3,
    )

    run_materializacao_financeiro_bilhetagem = await run_subflow(
        flow_name=FINANCEIRO_BILHETAGEM_MATERIALIZACAO,
    )

    run_ordem_quality_check = await run_subflow(
        flow_name=ordem_pagamento_quality_check,
    )

    integracao_capture_params = create_transacao_ordem_integracao_capture_params(
        timestamp=timestamp,
        table_id=jae_constants.INTEGRACAO_TABLE_ID,
        wait_for=[run_ordem_quality_check],
    )

    run_captura_integracao = await run_subflow(
        flow_name=CAPTURA_INTEGRACAO,
        parameters=integracao_capture_params,
    )

    run_materializacao_integracao = await run_subflow(
        flow_name=INTEGRACAO_MATERIALIZACAO,
        wait_for=[run_captura_integracao],
    )

    transacao_ordem_capture_params = create_transacao_ordem_integracao_capture_params(
        timestamp=timestamp,
        table_id=jae_constants.TRANSACAO_ORDEM_TABLE_ID,
        wait_for=[run_materializacao_integracao],
    )

    run_captura_transacao_ordem = await run_subflow(
        flow_name=CAPTURA_TRANSACAO_ORDEM,
        parameters=transacao_ordem_capture_params,
    )

    run_materializacao_transacao_ordem = await run_subflow(
        flow_name=TRANSACAO_ORDEM_MATERIALIZACAO,
    )
