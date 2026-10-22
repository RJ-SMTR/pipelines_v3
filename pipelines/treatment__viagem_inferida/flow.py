# -*- coding: utf-8 -*-
"""
Flow de materialização de viagem inferida

Executa o selector DBT 'viagem_inferida' para materializar viagens inferidas
a partir do GPS e sua cadeia de validação no modo monitoramento, enquanto as
operadoras ainda não enviaram a viagem informada (prazo de 5 dias).

Schedule:
- A cada 3 horas (horário de São Paulo)
- Depende de dados do Planejamento Diário e do GPS (Conecta e Zirix)

DBT: 2026-07-29
"""

from typing import Optional

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.common.utils.prefect import flow
from pipelines.treatment__viagem_inferida import constants


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__viagem_inferida(  # noqa: PLR0913
    env: Optional[str] = None,
    datetime_start: Optional[str] = None,
    datetime_end: Optional[str] = None,
    flags: Optional[list[str]] = None,
    additional_vars: Optional[dict] = None,
    force_test_run: bool = False,
    skip_source_check: bool = False,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.VIAGEM_INFERIDA_SELECTOR],
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars={**constants.ADDITIONAL_VARS, **(additional_vars or {})},
        force_test_run=force_test_run,
        skip_source_check=skip_source_check,
    )
