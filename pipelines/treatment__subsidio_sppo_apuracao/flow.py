# -*- coding: utf-8 -*-
"""
Flow de apuração do subsídio SPPO

Migrado de pipelines_rj_smtr (Prefect 1.4) — `subsidio_sppo_apuracao`.
Executa selectors V8/V9/monitoramento conforme range de datas, snapshot final
e pré-checagem de captura de dados da Jaé.

Common: 2026-05-22
"""

from typing import Optional

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import (
    MaterializationTestConfig,
    rename_treatment_flow_run,
    resolve_run_mode,
)
from pipelines.common.utils.prefect import flow
from pipelines.treatment__subsidio_sppo_apuracao import constants


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__subsidio_sppo_apuracao(  # noqa: PLR0913
    env: Optional[str] = None,
    datetime_start: Optional[str] = None,
    datetime_end: Optional[str] = None,
    additional_vars: Optional[dict] = None,
    fallback_run: bool = False,
    skip_source_check: bool = False,
    skip_pre_test: bool = False,
    flags: Optional[list[str]] = None,
    test_only: bool = False,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[
            constants.APURACAO_SUBSIDIO_V8_SELECTOR,
            constants.APURACAO_SUBSIDIO_V9_SELECTOR,
            constants.APURACAO_SUBSIDIO_V14_SELECTOR,
            constants.MONITORAMENTO_SUBSIDIO_SELECTOR,
        ],
        snapshot_selector=constants.SNAPSHOT_SUBSIDIO_SELECTOR,
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        run_mode=resolve_run_mode(skip_pre_test=skip_pre_test, test_only=test_only),
        skip_source_check=skip_source_check,
        additional_vars={**constants.ADDITIONAL_VARS, **(additional_vars or {})},
        test_config=MaterializationTestConfig(webhook_key=constants.WEBHOOK_KEY),
        fallback_run=fallback_run,
        flags=flags,
    )
