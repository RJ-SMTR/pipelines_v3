# -*- coding: utf-8 -*-
"""
Flow de materialização de viagem informada

Executa o selector DBT 'viagem_informada' para materializar dados de viagens
informadas pela Rio Ônibus no BigQuery.

Schedule:
- Diariamente às 7h30 (horário de São Paulo)
- Depende de dados do Planejamento Diário e da Rio Ônibus

DBT: 2026-02-25
"""

from prefect import flow

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.treatment__viagem_informada import constants


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__viagem_informada(  # noqa: PLR0913
    env=None,
    datetime_start=None,
    datetime_end=None,
    flags=None,
    additional_vars=None,
    force_test_run=False,
    skip_source_check=False,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.VIAGEM_INFORMADA_SELECTOR],
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars=additional_vars,
        force_test_run=force_test_run,
        skip_source_check=skip_source_check,
    )
