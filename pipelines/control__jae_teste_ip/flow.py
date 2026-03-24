# -*- coding: utf-8 -*-

from typing import Optional

from prefect import flow, runtime

from pipelines.common.capture.jae import constants
from pipelines.common.tasks import (
    get_run_env,
    initialize_sentry,
    setup_environment,
    task_send_discord_message,
)
from pipelines.control__jae_teste_ip.tasks import (
    create_database_error_discord_message,
    test_jae_databases_connections,
)


@flow(log_prints=True)
def control__jae_teste_ip(env: Optional[str] = None):
    env = get_run_env(env=env, deployment_name=runtime.deployment.name)

    sentry = initialize_sentry(env=env)
    setup_env = setup_environment(env=env)

    success, failed_connections = test_jae_databases_connections(wait_for=[sentry, setup_env])

    if not success:
        message = create_database_error_discord_message(failed_connections=failed_connections)
        task_send_discord_message(
            message=message,
            webhook=constants.ALERT_WEBHOOK,
        )
