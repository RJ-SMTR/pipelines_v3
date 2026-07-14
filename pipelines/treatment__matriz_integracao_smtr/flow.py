# -*- coding: utf-8 -*-
from typing import Optional

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import (
    MaterializationTestConfig,
    rename_treatment_flow_run,
)
from pipelines.common.utils.prefect import flow
from pipelines.treatment__matriz_integracao_smtr import constants


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__matriz_integracao_smtr(  # noqa: PLR0913
    env: Optional[str] = None,
    datetime_start: Optional[str] = None,
    datetime_end: Optional[str] = None,
    flags: Optional[list[str]] = None,
    additional_vars: Optional[dict] = None,
    force_test_run: bool = False,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.MATRIZ_INTEGRACAO_SMTR_SELECTOR],
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars=additional_vars,
        test_config=MaterializationTestConfig(
            force_run=force_test_run,
        ),
    )
