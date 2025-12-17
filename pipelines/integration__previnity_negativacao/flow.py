# -*- coding: utf-8 -*-
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
def integration__previnity_negativacao() -> list[str]:
    use_pandas()
    customer_ids = get_customer_ids()
    results = process_customer.map(customer_ids)
    return results