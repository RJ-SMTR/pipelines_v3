# -*- coding: utf-8 -*-
"""
Flow para os processos manuais de ordem atrasada da bilhetagem
"""

from prefect import runtime

from pipelines.capture__jae_integracao import flow as capture__jae_integracao
from pipelines.capture__jae_ordem_pagamento import constants as ordem_pagamento_constants
from pipelines.capture__jae_ordem_pagamento.flow import capture__jae_ordem_pagamento
from pipelines.capture__jae_transacao_ordem.flow import capture__jae_transacao_ordem
from pipelines.common.capture.jae import constants as jae_constants
from pipelines.common.tasks import (
    get_run_env,
    get_scheduled_timestamp,
    initialize_sentry,
    run_subflow,
    setup_environment,
)
from pipelines.common.utils.prefect import flow, rename_flow_run
from pipelines.control__bilhetagem_ordem_atrasada.tasks import (
    create_transacao_ordem_integracao_capture_params,
)
from pipelines.quality_check__ordem_pagamento.flow import quality_check__ordem_pagamento
from pipelines.treatment__financeiro_bilhetagem.flow import treatment__financeiro_bilhetagem
from pipelines.treatment__integracao.flow import treatment__integracao
from pipelines.treatment__transacao_ordem.flow import treatment__transacao_ordem

sources = ordem_pagamento_constants.JAE_ORDEM_PAGAMENTO_SOURCES


@flow(log_prints=True, flow_run_name=rename_flow_run)
async def control__bilhetagem_ordem_atrasada(env: str | None = None):
    deployment_name = runtime.deployment.name
    env = get_run_env(env=env, deployment_name=deployment_name)
    sentry = initialize_sentry(env=env)
    setup_env = setup_environment(env=env)

    timestamp = get_scheduled_timestamp(wait_for=[sentry, setup_env])

    await run_subflow(
        flow=capture__jae_ordem_pagamento,
    )

    await run_subflow(
        flow=capture__jae_ordem_pagamento,
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

    await run_subflow(
        flow=treatment__financeiro_bilhetagem,
    )

    run_ordem_quality_check = await run_subflow(
        flow=quality_check__ordem_pagamento,
    )

    integracao_capture_params = create_transacao_ordem_integracao_capture_params(
        timestamp=timestamp,
        table_id=jae_constants.INTEGRACAO_TABLE_ID,
        wait_for=[run_ordem_quality_check],
    )

    await run_subflow(
        flow=capture__jae_integracao,
        parameters=integracao_capture_params,
    )

    run_materializacao_integracao = await run_subflow(
        flow=treatment__integracao,
    )

    transacao_ordem_capture_params = create_transacao_ordem_integracao_capture_params(
        timestamp=timestamp,
        table_id=jae_constants.TRANSACAO_ORDEM_TABLE_ID,
        wait_for=[run_materializacao_integracao],
    )

    await run_subflow(
        flow=capture__jae_transacao_ordem,
        parameters=transacao_ordem_capture_params,
    )

    await run_subflow(
        flow=treatment__transacao_ordem,
    )
