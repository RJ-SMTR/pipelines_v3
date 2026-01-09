from prefect import flow, runtime

from pipelines.common import constants as common_constants
from pipelines.common.tasks import (async_api_post_request,
                                    create_local_filepath, get_run_env,
                                    get_scheduled_timestamp, query_bq,
                                    save_data_to_file, setup_environment,
                                    upload_to_gcs)
from pipelines.integration__previnity_negativacao import constants
from pipelines.integration__previnity_negativacao.tasks import (
    get_previnity_credentials, prepare_previnity_payloads)


@flow(log_prints=True)
async def integration__previnity_negativacao():
    env = get_run_env(env=None, deployment_name=runtime.deployment.name)
    setup_env = setup_environment(env=env)

    previnity_key, previnity_token = get_previnity_credentials(wait_for=[setup_env])

    headers = {
        "PREVKEY": previnity_key,
        "PREVTOKEN": previnity_token,
        "Content-Type": "application/json",
    }

    project_id = common_constants.PROJECT_NAME[env]
    data_list = query_bq(query=constants.QUERY_PF, project_id=project_id)

    ts = get_scheduled_timestamp()
    execution_date = ts.date()
    partition = f"data={execution_date}"
    filename = ts.strftime("%Y-%m-%d-%H-%M-%S")
    filepath = create_local_filepath(
        partition=partition,
        dataset_id="previnity",
        table_id="retorno_negativacao",
        filename=filename,
        filetype="csv",
        mode="upload",
    )

    payloads = prepare_previnity_payloads(data=data_list, execution_date=execution_date)

    results = await async_api_post_request(
        url=constants.API_URL_PF,
        payloads=payloads,
        headers=headers,
        max_concurrent=300,
    )

    print(results)

    save_data_to_file(
        data=results,
        path=filepath,
        filetype="csv",
        csv_mode="w",
    )

    upload_to_gcs(
        env=env,
        path=filepath,
        dataset_id="previnity",
        table_id="retorno_negativacao",
        partition=partition,
        create_table=True,
        bucket_names=constants.NEGATIVACAO_PRIVATE_BUCKET_NAMES,
    )
