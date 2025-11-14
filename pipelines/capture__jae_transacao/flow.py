# -*- coding: utf-8 -*-
from prefect import context, flow, runtime

from pipelines.capture__jae_transacao import constants
from pipelines.capture__jae_transacao.tasks import create_jae_general_extractor
from pipelines.common.capture.default_capture.flow import (
    create_capture_flows_default_tasks,
)

# b


@flow(log_prints=True)
def capture__jae_transacao(
    env=None,
    timestamp=None,
    recapture=False,
    recapture_days=2,
    recapture_timestamps=None,
):
    ctx = context.get_run_context()
    print("deployment_id:", ctx.flow_run.deployment_id)
    print("deployment_name:", runtime.deployment.name)

    create_capture_flows_default_tasks(
        env=env,
        sources=[constants.TRANSACAO_SOURCE],
        timestamp=timestamp,
        create_extractor_task=create_jae_general_extractor,
        recapture=recapture,
        recapture_days=recapture_days,
        recapture_timestamps=recapture_timestamps,
    )
