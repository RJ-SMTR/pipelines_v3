# -*- coding: utf-8 -*-
from typing import Optional

from prefect import runtime

from pipelines.common.tasks import (
    get_run_env,
    get_scheduled_timestamp,
    initialize_sentry,
    setup_environment,
)
from pipelines.common.treatment.default_treatment.tasks import (
    ingest_dbt_artifacts_to_openmetadata,
    install_dbt_packages,
    run_dbt_selector_tests,
    run_dbt_selectors,
    setup_dbt_queries,
)
from pipelines.common.utils.prefect import flow, handler_notify_failure, rename_flow_run
from pipelines.integration__upload_transacao_cct.constants import TABLE_UPLOAD_CONFIGS
from pipelines.integration__upload_transacao_cct.tasks import (
    create_sincronizacao_materialization_context,
    delete_all_files,
    export_data_from_bq_to_gcs,
    get_postgres_modified_dates,
    get_start_datetime,
    save_upload_timestamp_redis,
    upload_files_postgres,
    upload_postgres_modified_data_to_bq,
)
from pipelines.integration__upload_transacao_cct.utils import TableUploadConfig


@flow(log_prints=True, timeout_seconds=18000)
def upload_table_to_cct(  # noqa: PLR0913
    config_name: str,
    env: str,
    timestamp,
    full_refresh: bool,
    data_ordem_start: Optional[str],
    data_ordem_end: Optional[str],
    param_test_dates: Optional[list[str]],
    flags: Optional[list[str]],
):
    config: TableUploadConfig = TABLE_UPLOAD_CONFIGS[config_name]

    if param_test_dates is None:
        start_datetime, full_refresh = get_start_datetime(
            config=config,
            env=env,
            full_refresh=full_refresh,
            data_ordem_start=data_ordem_start,
            data_ordem_end=data_ordem_end,
        )

        delete_files = delete_all_files(config=config, env=env)

        export_bigquery_dates = export_data_from_bq_to_gcs(
            config=config,
            env=env,
            timestamp=timestamp,
            start_datetime=start_datetime,
            full_refresh=full_refresh,
            data_ordem_start=data_ordem_start,
            data_ordem_end=data_ordem_end,
            wait_for=[delete_files],
        )

        upload_postgres = upload_files_postgres(
            config=config,
            env=env,
            full_refresh=full_refresh,
            export_bigquery_dates=export_bigquery_dates,
        )

        test_dates = get_postgres_modified_dates(
            config=config,
            env=env,
            start_datetime=start_datetime,
            full_refresh=full_refresh,
            wait_for=[upload_postgres],
        )
    else:
        test_dates = param_test_dates
        upload_postgres = None

    upload_test_bq = upload_postgres_modified_data_to_bq(
        config=config,
        env=env,
        timestamp=timestamp,
        dates=test_dates,
        full_refresh=full_refresh,
        wait_for=[upload_postgres],
    )

    contexts = create_sincronizacao_materialization_context(
        config=config, env=env, timestamp=timestamp
    )

    run_model = run_dbt_selectors(
        contexts=contexts,
        flags=flags,
        wait_for=[upload_test_bq],
    )

    run_test = run_dbt_selector_tests(
        contexts=contexts,
        mode="post",
        wait_for=[run_model],
    )

    notify_discord = task_dbt_selector_test_notify_discord(
        context=contexts[0],
        mode="post",
        webhook_key="alertas_bilhetagem",
        wait_for=[run_test],
    )

    save_upload_timestamp_redis(
        config=config,
        env=env,
        timestamp=timestamp,
        data_ordem_start=data_ordem_start,
        param_test_dates=param_test_dates,
        wait_for=[upload_postgres, notify_discord],
    )


@flow(
    log_prints=True,
    flow_run_name=rename_flow_run,
    on_failure=[handler_notify_failure(webhook="alertas_bilhetagem")],
    on_crashed=[handler_notify_failure(webhook="alertas_bilhetagem")],
    timeout_seconds=36000,
)
def integration__upload_transacao_cct(  # noqa: PLR0913
    env: Optional[str] = None,
    full_refresh: bool = False,
    data_ordem_start: Optional[str] = None,
    data_ordem_end: Optional[str] = None,
    param_test_dates: Optional[list[str]] = None,
    flags: Optional[list[str]] = None,
):
    env = get_run_env(env=env, deployment_name=runtime.deployment.name)
    sentry = initialize_sentry(env=env)
    setup_env = setup_environment(env=env, wait_for=[sentry])
    queries = setup_dbt_queries(env=env, wait_for=[setup_env])
    install_dbt_packages(wait_for=[queries])

    timestamp = get_scheduled_timestamp()

    for config_name in TABLE_UPLOAD_CONFIGS:
        upload_table_to_cct(
            config_name=config_name,
            env=env,
            timestamp=timestamp,
            full_refresh=full_refresh,
            data_ordem_start=data_ordem_start,
            data_ordem_end=data_ordem_end,
            param_test_dates=param_test_dates,
            flags=flags,
            return_state=True,
        )

    ingest_dbt_artifacts_to_openmetadata(
        env=env,
        deployment_name=runtime.deployment.name,
        flow_run_id=runtime.flow_run.id,
    )
