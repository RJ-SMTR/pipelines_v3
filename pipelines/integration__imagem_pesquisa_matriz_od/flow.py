# -*- coding: utf-8 -*-
from google.cloud import storage
from prefect import runtime

from pipelines.common import constants as smtr_constants
from pipelines.common.capture.jae import constants as jae_constants
from pipelines.common.tasks import (
    get_run_env,
    initialize_sentry,
    setup_environment,
    task_send_discord_message,
)
from pipelines.common.utils.prefect import flow, handler_notify_failure, rename_flow_run
from pipelines.integration__imagem_pesquisa_matriz_od import constants
from pipelines.integration__imagem_pesquisa_matriz_od.tasks import (
    create_missing_image_discord_message,
    generate_images,
    get_date_range,
    get_journeys,
    save_run_on_redis,
    upload_images_to_gcs,
    wait_materialization,
)


@flow(
    log_prints=True,
    flow_run_name=rename_flow_run,
    on_failure=[handler_notify_failure(webhook="alertas_bilhetagem")],
    on_crashed=[handler_notify_failure(webhook="alertas_bilhetagem")],
)
def integration__imagem_pesquisa_matriz_od(
    env: str | None = None,
    datetime_start: str | None = None,
    datetime_end: str | None = None,
):
    env = get_run_env(env=env, deployment_name=runtime.deployment.name)
    setup_env = setup_environment(env=env)
    initialize_sentry(env)

    datetime_start, datetime_end = get_date_range(
        env=env, datetime_start=datetime_start, datetime_end=datetime_end, wait_for=[setup_env]
    )

    wait = wait_materialization(env=env, datetime_end=datetime_end)

    project_name = smtr_constants.PROJECT_NAME[env]

    journey_filepath = get_journeys(
        project_name=project_name,
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        wait_for=wait,
    )

    bucket = storage.Client(project=project_name).bucket(bucket_name=constants.BUCKET_NAMES[env])

    image_filepaths, no_image_stop_ids = generate_images(
        bucket_obj=bucket, journey_filepath=journey_filepath
    )

    upload_images = upload_images_to_gcs(bucket_obj=bucket, filepaths=image_filepaths)

    save_run_on_redis(
        env=env,
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        wait_for=[upload_images],
    )

    discord_messages = create_missing_image_discord_message(no_image_stop_id=no_image_stop_ids)

    task_send_discord_message(
        message=discord_messages,
        webhook=jae_constants.ALERT_WEBHOOK,
    )
