# -*- coding: utf-8 -*-
from prefect import flow

from pipelines.capture__jae_transacao_erro import constants
from pipelines.capture__jae_transacao_erro_historico.tasks import get_transacao_erro_timestamps
from pipelines.common.capture.default_capture.flow import (
    create_capture_flows_default_tasks,
)
from pipelines.common.capture.default_capture.utils import rename_capture_flow_run
from pipelines.common.capture.jae.tasks import create_jae_general_extractor


@flow(log_prints=True, flow_run_name=rename_capture_flow_run)
def capture__jae_transacao_erro_historico(
    env=None,
    timestamp=None,
    recapture=True,
    recapture_days=2,
):
    recapture_timestamps = get_transacao_erro_timestamps(source=constants.JAE_TRANSACAO_ERRO_SOURCE)

    create_capture_flows_default_tasks(
        env=env,
        sources=[constants.JAE_TRANSACAO_ERRO_SOURCE],
        source_table_ids=[constants.JAE_TRANSACAO_ERRO_SOURCE.table_id],
        timestamp=timestamp,
        create_extractor_task=create_jae_general_extractor,
        recapture=recapture,
        recapture_days=recapture_days,
        recapture_timestamps=recapture_timestamps,
        if_exists_upload="pass",
        tasks_wait_for={"env": [recapture_timestamps]},
    )
