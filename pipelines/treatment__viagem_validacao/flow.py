# -*- coding: utf-8 -*-
"""
Flow de materialização de viagem validação

Parâmetros:

- use_partition_dates: se True, reprocessa as partições do intervalo informado
  (datetime_start/datetime_end) em vez das partições modificadas no upstream

DBT: 2026-05-07
"""

from typing import Optional

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.common.utils.prefect import flow
from pipelines.treatment__viagem_validacao import constants


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__viagem_validacao(  # noqa: PLR0913
    env: Optional[str] = None,
    datetime_start: Optional[str] = None,
    datetime_end: Optional[str] = None,
    flags: Optional[list[str]] = None,
    additional_vars: Optional[dict] = None,
    force_test_run: bool = False,
    skip_source_check: bool = False,
    use_partition_dates: bool = False,
):
    if use_partition_dates:
        additional_vars = (additional_vars or {}) | {"use_partition_dates": True}

    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.VIAGEM_VALIDACAO_SELECTOR],
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars=additional_vars,
        force_test_run=force_test_run,
        skip_source_check=skip_source_check,
    )
