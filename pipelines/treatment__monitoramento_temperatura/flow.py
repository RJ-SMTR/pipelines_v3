# -*- coding: utf-8 -*-
"""
Flow de materialização de dados de monitoramento de temperatura

Executa o selector DBT 'monitoramento_temperatura' para materializar dados no BigQuery.

Schedule:
- Diariamente às 6h00 (horário de São Paulo)
- Depende de dados do monitoramento_veiculo

DBT: 2026-04-08
"""

from prefect import flow

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.treatment__monitoramento_temperatura import constants


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__monitoramento_temperatura(  # noqa: PLR0913
    env=None,
    datetime_start=None,
    datetime_end=None,
    flags=None,
    additional_vars=None,
    force_test_run=False,
    skip_source_check=False,
):
    additional_vars = additional_vars or {}
    additional_vars["tipo_materializacao"] = "monitoramento"

    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.MONITORAMENTO_TEMPERATURA_SELECTOR],
        snapshot_selector=constants.SNAPSHOT_TEMPERATURA_SELECTOR,
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars=additional_vars,
        force_test_run=force_test_run,
        skip_source_check=skip_source_check,
    )
