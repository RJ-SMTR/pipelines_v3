# -*- coding: utf-8 -*-
"""
Flow de captura de dados do STU

Executa a captura de uma tabela do STU por flow run quando `source_table_id` é
informado. Sem o parâmetro, captura todas as tabelas.

Common: 2026-03-27
"""

from prefect import flow

from pipelines.capture__stu_tabelas import constants
from pipelines.capture__stu_tabelas.tasks import create_stu_extractor
from pipelines.common.capture.default_capture.flow import create_capture_flows_default_tasks
from pipelines.common.capture.default_capture.utils import rename_capture_flow_run


@flow(log_prints=True, flow_run_name=rename_capture_flow_run)
def capture__stu_tabelas(  # noqa: PLR0913
    env=None,
    source_table_ids=None,
    timestamp=None,
    recapture=True,
    recapture_days=5,
    recapture_timestamps=None,
):
    create_capture_flows_default_tasks(
        env=env,
        sources=constants.STU_SOURCES,
        source_table_ids=source_table_ids,
        timestamp=timestamp,
        create_extractor_task=create_stu_extractor,
        recapture=recapture,
        recapture_days=recapture_days,
        recapture_timestamps=recapture_timestamps,
    )
