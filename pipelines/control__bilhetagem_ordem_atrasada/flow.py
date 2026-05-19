# -*- coding: utf-8 -*-
"""
Flows de tratamento dos dados financeiros
"""

from typing import Optional

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
    setup_environment,
)
from pipelines.control__bilhetagem_ordem_atrasada import constants as jae_constants
from pipelines.tasks import (
    get_scheduled_timestamp,
    parse_timestamp_to_string,
    run_subflow,
)
from pipelines.treatment.bilhetagem.flows import (
    INTEGRACAO_MATERIALIZACAO,
    TRANSACAO_ORDEM_MATERIALIZACAO,
)
from pipelines.treatment.bilhetagem_processos_manuais.tasks import (
    create_transacao_ordem_integracao_capture_params,
)
from pipelines.treatment.financeiro.flows import (
    FINANCEIRO_BILHETAGEM_MATERIALIZACAO,
    ordem_pagamento_quality_check,
)

sources = jae_constants.sources


@flow(name="financeiro_bilhetagem: ordem atrasada - captura/tratamento")
def ordem_atrasada(timestamp: str | None = None, env: Optional[str] = None):
    env = get_run_env(env=env, deployment_name=runtime.deployment.name)

    sentry = initialize_sentry(env=env)
    setup_env = setup_environment(env=env)

    timestamp = get_scheduled_timestamp(wait_for=[sentry, setup_env])

    run_recapture = run_subflow(
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

    run_capture = run_subflow(
        flow_name=CAPTURA_ORDEM_PAGAMENTO,
        parameters=[
            {
                "table_id": s.table_id,
                "timestamp": parse_timestamp_to_string(
                    timestamp=timestamp, pattern="%Y-%m-%d %H:%M:%S"
                ),
                "recapture": False,
            }
            for s in sources
        ],
        maximum_parallelism=3,
        wait_for=[run_recapture],
    )

    run_materializacao_financeiro_bilhetagem = run_subflow(
        flow_name=FINANCEIRO_BILHETAGEM_MATERIALIZACAO, wait_for=[run_capture]
    )

    run_ordem_quality_check = run_subflow(
        flow_name=ordem_pagamento_quality_check,
        wait_for=[run_materializacao_financeiro_bilhetagem],
    )

    integracao_capture_params = create_transacao_ordem_integracao_capture_params(
        timestamp=timestamp,
        table_id=jae_constants.INTEGRACAO_TABLE_ID,
        wait_for=[run_ordem_quality_check],
    )

    run_captura_integracao = run_subflow(
        flow_name=CAPTURA_INTEGRACAO,
        parameters=integracao_capture_params,
    )

    run_materializacao_integracao = run_subflow(
        flow_name=INTEGRACAO_MATERIALIZACAO,
        wait_for=[run_captura_integracao],
    )

    transacao_ordem_capture_params = create_transacao_ordem_integracao_capture_params(
        timestamp=timestamp,
        table_id=jae_constants.TRANSACAO_ORDEM_TABLE_ID,
        wait_for=[run_materializacao_integracao],
    )

    run_captura_transacao_ordem = run_subflow(
        flow_name=CAPTURA_TRANSACAO_ORDEM,
        parameters=transacao_ordem_capture_params,
    )

    run_materializacao_transacao_ordem = run_subflow(
        flow_name=TRANSACAO_ORDEM_MATERIALIZACAO,
        wait_for=[run_captura_transacao_ordem],
    )
