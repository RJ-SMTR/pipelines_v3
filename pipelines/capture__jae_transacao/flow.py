# -*- coding: utf-8 -*-
import os

from prefect import flow, task

from pipelines.capture__jae_transacao import constants
from pipelines.capture__jae_transacao.tasks import create_jae_general_extractor
from pipelines.common.capture.default_capture.flow import (
    create_capture_flows_default_tasks,
)

# a


@task
def running_in_docker():
    secret_key = bool(os.environ.get("RUNNING_IN_DOCKER", ""))

    # if secret_key:
    print(secret_key)


@flow(log_prints=True)
def capture__jae_transacao(
    env=None,
    timestamp=None,
    recapture=False,
    recapture_days=2,
    recapture_timestamps=None,
):
    running_in_docker()
    create_capture_flows_default_tasks(
        env=env,
        sources=[constants.TRANSACAO_SOURCE],
        timestamp=timestamp,
        create_extractor_task=create_jae_general_extractor,
        recapture=recapture,
        recapture_days=recapture_days,
        recapture_timestamps=recapture_timestamps,
    )
