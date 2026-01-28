import pandas as pd
from prefect import flow, runtime

from pipelines.common import constants as common_constants
from pipelines.common.capture.default_capture.tasks import (
    create_capture_contexts,
    upload_source_data_to_gcs,
)
from pipelines.common.tasks import (
    async_api_post_request,
    get_run_env,
    get_scheduled_timestamp,
    query_bq,
    save_data_to_file,
    setup_environment,
)
from pipelines.common.treatment.default_treatment.tasks import (
    create_materialization_contexts,
    run_dbt_selectors,
    save_materialization_datetime_redis,
)
from pipelines.integration__previnity_negativacao import constants
from pipelines.integration__previnity_negativacao.tasks import (
    get_previnity_credentials,
    get_previnity_date_range,
    prepare_previnity_payloads,
)


@flow(log_prints=True)
async def integration__previnity_negativacao(  # noqa: PLR0913
    timestamp=None,
    env=None,
    datetime_start=None,
    datetime_end=None,
    flags=None,
    additional_vars=None,
):
    env = get_run_env(env=env, deployment_name=runtime.deployment.name)
    setup_env = setup_environment(env=env)

    previnity_key, previnity_token = get_previnity_credentials(wait_for=[setup_env])

    headers = {
        "PREVKEY": previnity_key,
        "PREVTOKEN": previnity_token,
        "Content-Type": "application/json",
    }

    ts = get_scheduled_timestamp(timestamp=timestamp)

    datetime_start, datetime_end = get_previnity_date_range(
        env=env,
        ts=ts,
        datetime_start=datetime_start,
        datetime_end=datetime_end,
    )

    project_id = common_constants.PROJECT_NAME[env]
    data_list = query_bq(query=constants.QUERY_PF, project_id=project_id)

    contexts = create_capture_contexts(
        env=env,
        sources=constants.PREVINITY_SOURCES,
        source_table_ids=["retorno_negativacao"],
        timestamp=ts,
        recapture=False,
        recapture_days=2,
        recapture_timestamps=None,
    )

    context = contexts[0]

    payloads_with_metadata = prepare_previnity_payloads(
        data=data_list,
        datetime_start=datetime_start,
        datetime_end=datetime_end,
    )

    if not payloads_with_metadata:
        payloads = []
        metadata_list = []
    else:
        payloads, metadata_list = zip(*payloads_with_metadata, strict=True)

    api_results = await async_api_post_request(
        url=constants.API_URL_PF,
        payloads=payloads,
        headers=headers,
        max_concurrent=constants.PREVINITY_RATE_LIMIT,
    )

    response = []
    for result, metadata in zip(api_results, metadata_list, strict=True):
        result.update(metadata)
        response.append(result)

    df_response = pd.DataFrame(response)

    save_data_to_file(
        data=df_response,
        path=context.source_filepath,
        filetype="csv",
    )

    upload_source_future = upload_source_data_to_gcs(context=context)

    materialization_contexts = create_materialization_contexts(
        env=env,
        selectors=[constants.NEGATIVACAO_SELECTOR],
        timestamp=ts,
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        additional_vars=additional_vars,
        test_scheduled_time=None,
        force_test_run=False,
        wait_for=[upload_source_future],
    )

    run_dbt_future = run_dbt_selectors(
        contexts=materialization_contexts,
        flags=flags,
    )

    save_materialization_datetime_redis.map(
        context=materialization_contexts, wait_for=[run_dbt_future]
    )
