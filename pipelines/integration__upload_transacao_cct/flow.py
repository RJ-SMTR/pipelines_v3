# -*- coding: utf-8 -*-
from typing import Optional

from prefect import flow, runtime

from pipelines.common.tasks import (
    get_run_env,
    get_scheduled_timestamp,
    initialize_sentry,
    setup_environment,
)
from pipelines.common.treatment.default_treatment.tasks import (
    run_dbt_selector_tests,
    run_dbt_selectors,
    task_dbt_selector_test_notify_discord,
)
from pipelines.common.utils.prefect import rename_flow_run
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


@flow(log_prints=True, flow_run_name=rename_flow_run)
def integration__upload_transacao_cct(
    env: Optional[str] = None,
    full_refresh: bool = False,
    data_ordem_start: Optional[str] = None,
    data_ordem_end: Optional[str] = None,
    param_test_dates: Optional[list[str]] = None,
):
    env = get_run_env(env=env, deployment_name=runtime.deployment.name)
    sentry = initialize_sentry(env=env)
    setup_environment(env=env, wait_for=[sentry])

    timestamp = get_scheduled_timestamp()

    if param_test_dates is None:
        start_datetime, full_refresh = get_start_datetime(
            env=env,
            full_refresh=full_refresh,
            data_ordem_start=data_ordem_start,
            data_ordem_end=data_ordem_end,
        )

        delete_files = delete_all_files(env=env)

        export_bigquery_dates = export_data_from_bq_to_gcs(
            env=env,
            timestamp=timestamp,
            start_datetime=start_datetime,
            full_refresh=full_refresh,
            data_ordem_start=data_ordem_start,
            data_ordem_end=data_ordem_end,
            wait_for=[delete_files],
        )

        upload_postgres = upload_files_postgres(
            env=env,
            full_refresh=full_refresh,
            export_bigquery_dates=export_bigquery_dates,
        )

        test_dates = get_postgres_modified_dates(
            env=env,
            start_datetime=start_datetime,
            full_refresh=full_refresh,
            wait_for=[upload_postgres],
        )
    else:
        test_dates = param_test_dates
        upload_postgres = None

    upload_test_bq = upload_postgres_modified_data_to_bq(
        env=env,
        timestamp=timestamp,
        dates=test_dates,
        full_refresh=full_refresh,
        wait_for=[upload_postgres],
    )

    contexts = create_sincronizacao_materialization_context(env=env)

    run_sincronizacao_model = run_dbt_selectors(
        contexts=contexts,
        flags=None,
        wait_for=[upload_test_bq],
    )

    run_sincronizacao_test = run_dbt_selector_tests(
        contexts=contexts,
        mode="post",
        wait_for=[run_sincronizacao_model],
    )

    notify_discord = task_dbt_selector_test_notify_discord(
        context=contexts[0],
        mode="post",
        webhook_key="alertas_bilhetagem",
        wait_for=[run_sincronizacao_test],
    )

    save_upload_timestamp_redis(
        env=env,
        timestamp=timestamp,
        data_ordem_start=data_ordem_start,
        param_test_dates=param_test_dates,
        wait_for=[upload_postgres, notify_discord],
    )
