# -*- coding: utf-8 -*-
"""
Materialização das tabelas de autuação de trânsito

DBT: 2026-01-27
"""

from prefect import flow

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.treatment__transito_autuacao import constants


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__transito_autuacao(  # noqa: PLR0913
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
        selectors=[constants.TRANSITO_AUTUACAO_SELECTOR],
        snapshot_selector=constants.SNAPSHOT_TRANSITO_SELECTOR,
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars=additional_vars,
        test_scheduled_time=None,
        force_test_run=force_test_run,
        skip_source_check=skip_source_check,
    )
