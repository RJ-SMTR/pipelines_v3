# -*- coding: utf-8 -*-
"""
Flow de materialização da remuneração do Sistema RIO

Executa o selector DBT 'remuneracao_openfisca' para materializar dados no BigQuery
"""

from typing import Optional

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.common.utils.prefect import flow
from pipelines.treatment__remuneracao_openfisca import constants


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__remuneracao_openfisca(  # noqa: PLR0913
    env: Optional[str] = None,
    datetime_start: Optional[str] = None,
    datetime_end: Optional[str] = None,
    flags: Optional[list[str]] = None,
    additional_vars: Optional[dict] = None,
    skip_source_check: bool = False,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.REMUNERACAO_OPENFISCA_SELECTOR],
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars={**constants.ADDITIONAL_VARS, **(additional_vars or {})},
        skip_source_check=skip_source_check,
    )
