# -*- coding: utf-8 -*-
"""
Flow de captura de infrações de veículos SPPO

Executa a captura de dados de infrações de veículos via FTP RDO ou GCS,
aplica pré-tratamento e carrega em BigQuery.

Common: 2026-04-16
"""

from prefect import flow

from pipelines.capture__veiculo_infracao import constants
from pipelines.common.capture.default_capture.flow import (
    create_capture_flows_default_tasks,
)
from pipelines.common.capture.default_capture.utils import rename_capture_flow_run
from pipelines.common.capture.veiculo.tasks import create_veiculo_extractor


@flow(log_prints=True, flow_run_name=rename_capture_flow_run)
def capture__veiculo_infracao(
    env=None,
    timestamp=None,
    recapture=True,
    recapture_days=2,
    recapture_timestamps=None,
):
    create_capture_flows_default_tasks(
        env=env,
        sources=constants.SPPO_INFRACAO_SOURCES,
        timestamp=timestamp,
        create_extractor_task=create_veiculo_extractor,
        recapture=recapture,
        recapture_days=recapture_days,
        recapture_timestamps=recapture_timestamps,
        extra_parameters={
            constants.SPPO_INFRACAO_TABLE_ID: {"ftp_path": constants.SPPO_INFRACAO_FTP_PATH}
        },
    )
