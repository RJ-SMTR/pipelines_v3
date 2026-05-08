# -*- coding: utf-8 -*-
"""
Flow de captura de dados de fiscalização de veículos

Realiza a captura de dados da planilha de controle de lacre dos veículos.

Common:2026-05-08

"""

from typing import Optional

from pipelines.common.utils.prefect import flow  

from pipelines.capture__veiculo_fiscalizacao_lacre import constants
from pipelines.capture__veiculo_fiscalizacao_lacre.tasks import (
    create_veiculo_fiscalizacao_lacre_extractor,
)
from pipelines.common.capture.default_capture.flow import (
    create_capture_flows_default_tasks,
)
from pipelines.common.capture.default_capture.utils import rename_capture_flow_run


@flow(log_prints=True, flow_run_name=rename_capture_flow_run)
def capture__veiculo_fiscalizacao_lacre(  # noqa: PLR0913
    env: Optional[str] = None,
    source_table_ids: Optional[tuple[str]] = None,
    timestamp: Optional[str] = None,
    recapture: bool = True,
    recapture_days: int = 7,
    recapture_timestamps: Optional[list[str]] = None,
):
    create_capture_flows_default_tasks(
        env=env,
        sources=[constants.VEICULO_LACRE_SOURCE],
        source_table_ids=source_table_ids,
        timestamp=timestamp,
        create_extractor_task=create_veiculo_fiscalizacao_lacre_extractor,
        recapture=recapture,
        recapture_days=recapture_days,
        recapture_timestamps=recapture_timestamps,
    )
