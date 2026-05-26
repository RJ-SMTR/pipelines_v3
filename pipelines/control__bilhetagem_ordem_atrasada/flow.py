# -*- coding: utf-8 -*-
"""
Flows de tratamento dos dados financeiros
"""

# from pipelines.capture.jae.flows import (
#     CAPTURA_INTEGRACAO,
#     CAPTURA_ORDEM_PAGAMENTO,
#     CAPTURA_TRANSACAO_ORDEM,
# )

# from pipelines.treatment.financeiro.flows import (
#     FINANCEIRO_BILHETAGEM_MATERIALIZACAO,
#     ordem_pagamento_quality_check,
# )
from prefect import runtime

from pipelines.capture__jae_auxiliar import flow

# from pipelines.capture__jae_integracao.flow import capture__jae_integracao
from pipelines.capture__jae_integracao import flow as capture__jae_integracao
from pipelines.capture__jae_ordem_pagamento import constants as ordem_pagamento_constants

# from pipelines.treatment.bilhetagem.flows import (
#     INTEGRACAO_MATERIALIZACAO,
#     TRANSACAO_ORDEM_MATERIALIZACAO,
# )
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
from pipelines.control__bilhetagem_ordem_atrasada.tasks import (
    create_transacao_ordem_integracao_capture_params,
)
from pipelines.treatment__financeiro_bilhetagem.flow import treatment__financeiro_bilhetagem
from pipelines.treatment__integracao.flow import treatment__integracao
from pipelines.treatment__transacao_ordem.flow import treatment__transacao_ordem

sources = ordem_pagamento_constants.JAE_ORDEM_PAGAMENTO_SOURCES


@flow(name="financeiro_bilhetagem: ordem atrasada - captura/tratamento")
async def ordem_atrasada(timestamp: str | None = None, env: str | None = None):
    deployment_name = runtime.deployment.name
    env = get_run_env(env=env, deployment_name=deployment_name)
    sentry = initialize_sentry(env=env)
    setup_env = setup_environment(env=env)

    timestamp = get_scheduled_timestamp(wait_for=[sentry, setup_env])

    run_recapture = await run_subflow(
        flow=capture__jae_ordem_pagamento,
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

    run_materializacao_financeiro_bilhetagem = await run_subflow(
        flow=treatment__financeiro_bilhetagem,
    )

    run_ordem_quality_check = await run_subflow(
        flow=ordem_pagamento_quality_check,
    )

    integracao_capture_params = create_transacao_ordem_integracao_capture_params(
        timestamp=timestamp,
        table_id=jae_constants.INTEGRACAO_TABLE_ID,
        wait_for=[run_ordem_quality_check],
    )

    run_captura_integracao = await run_subflow(
        flow=capture__jae_integracao,
        parameters=integracao_capture_params,
    )

    run_materializacao_integracao = await run_subflow(
        flow=treatment__integracao,
        wait_for=[run_captura_integracao],
    )

    transacao_ordem_capture_params = create_transacao_ordem_integracao_capture_params(
        timestamp=timestamp,
        table_id=jae_constants.TRANSACAO_ORDEM_TABLE_ID,
        wait_for=[run_materializacao_integracao],
    )

    run_captura_transacao_ordem = await run_subflow(
        flow=capture__jae_transacao_ordem,
        parameters=transacao_ordem_capture_params,
    )

    run_materializacao_transacao_ordem = await run_subflow(
        flow=treatment__transacao_ordem,
    )
