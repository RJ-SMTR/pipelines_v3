# -*- coding: utf-8 -*-
"""
Flow de apuração do subsídio SPPO

Migrado de pipelines_rj_smtr (Prefect 1.4) — `subsidio_sppo_apuracao`.
Executa selectors V8/V9/monitoramento conforme range de datas, snapshot final
e pré-checagem de captura de dados da Jaé.

Common: 2026-05-15
"""

from typing import Optional

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.common.utils.prefect import flow
from pipelines.treatment__subsidio_sppo_apuracao import constants
from pipelines.treatment__subsidio_sppo_apuracao.utils import wait_jae_capture_gaps


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__subsidio_sppo_apuracao(  # noqa: PLR0913
    env: Optional[str] = None,
    datetime_start: Optional[str] = None,
    datetime_end: Optional[str] = None,
    additional_vars: Optional[dict] = None,
    fallback_run: bool = False,
    skip_source_check: bool = False,
    skip_pre_test: bool = False,
):
    tasks_wait_for = None

    if not skip_pre_test:
        pre_test_jae = wait_jae_capture_gaps(
            env=env,
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            table_ids=constants.TABLE_IDS_JAE,
        )

        tasks_wait_for = {"pre_tests": [pre_test_jae]}

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
        skip_source_check=skip_source_check,
        additional_vars={**constants.ADDITIONAL_VARS, **(additional_vars or {})},
        test_webhook_key=constants.WEBHOOK_KEY,
        tasks_wait_for=tasks_wait_for,
        fallback_run=fallback_run,
        skip_pre_test=skip_pre_test,
    )
