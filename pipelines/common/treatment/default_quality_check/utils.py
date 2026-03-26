# -*- coding: utf-8 -*-
from datetime import datetime
from typing import Optional

from pipelines.common.treatment.default_treatment.utils import (
    DBTTest,
    run_dbt,
)


def run_dbt_tests(
    dbt_test: DBTTest,
    datetime_start: Optional[datetime],
    datetime_end: Optional[datetime],
    partitions: Optional[list[str]],
) -> tuple[str, dict]:
    dbt_vars = dbt_test.get_test_vars(
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        partitions=partitions,
    )
    log = run_dbt(dbt_obj=dbt_test, dbt_vars=dbt_vars, raise_on_failure=False)

    return log, dbt_vars
