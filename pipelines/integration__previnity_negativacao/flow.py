from datetime import datetime

from prefect import flow, runtime, unmapped

from pipelines.common import constants as common_constants
from pipelines.common.tasks import (
    async_api_post_request,
    get_run_env,
    get_scheduled_timestamp,
    query_bq,
    setup_environment,
)
from pipelines.common.utils.secret import get_secret
from pipelines.integration__previnity_negativacao import constants
from pipelines.integration__previnity_negativacao.tasks import prepare_previnity_payloads


@flow
async def integration__previnity_negativacao():
    env = get_run_env(env=None, deployment_name=runtime.deployment.name)
    setup_env = setup_environment(env=env)

    secrets = get_secret(secret_path=constants.SECRET_PATH, wait_for=[setup_env])
    prev_key = secrets.get("previnity_api_key")
    prev_token = secrets.get("previnity_api_token")

    if not prev_key or not prev_token:
        raise ValueError("Missing 'prev_key' or 'prev_token' in secrets.")

    headers = {
        "PREVKEY": prev_key,
        "PREVTOKEN": prev_token,
        "Content-Type": "application/json",
    }

    project_id = common_constants.PROJECT_NAME[env]
    data_list = query_bq(query=constants.QUERY_PF, project_id=project_id)

    ts = get_scheduled_timestamp()
    execution_date = ts.date()

    payloads = prepare_previnity_payloads(data=data_list, execution_date=execution_date)

    results = await async_api_post_request(
        url=constants.API_URL_PF,
        payloads=payloads,
        headers=headers,
        max_concurrent=10,
    )

    print(results)
