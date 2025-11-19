# -*- coding: utf-8 -*-
{% if cookiecutter.flow_type == "capture" -%}
from prefect import flow

from pipelines.common.capture.default_capture.flow import (
    create_capture_flows_default_tasks,
)
from pipelines.common.capture.default_capture.utils import rename_capture_flow_run

@flow(log_prints=True, flow_run_name=rename_capture_flow_run)
def {{ cookiecutter.flow_type }}__{{ cookiecutter.pipeline }}(
    env=None,
    timestamp=None,
    recapture=False,
    recapture_days=2,
    recapture_timestamps=None,
) -> list[str]:
    create_capture_flows_default_tasks(
        env=env,
        sources=[],
        timestamp=timestamp,
        create_extractor_task=,
        recapture=recapture,
        recapture_days=recapture_days,
        recapture_timestamps=recapture_timestamps,
    )
{% else -%}
import random

import pandas as pd
from prefect import flow, task


@task
def use_pandas():
    df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
    print(df)


@task
def get_customer_ids() -> list[str]:
    return [f"customer{n}" for n in random.choices(range(100), k=10)]


@task
def process_customer(customer_id: str) -> str:
    return f"Processed {customer_id}"


@flow(log_prints=True)
def {{ cookiecutter.flow_type }}__{{ cookiecutter.pipeline }}() -> list[str]:
    use_pandas()
    customer_ids = get_customer_ids()
    results = process_customer.map(customer_ids)
    return results

{%- endif -%}