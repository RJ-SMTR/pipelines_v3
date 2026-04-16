# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from typing import Optional

from prefect import runtime, task
from prefect.cache_policies import NO_CACHE

from pipelines.common.treatment.default_treatment.tasks import (
    create_materialization_contexts,
)
from pipelines.common.treatment.default_treatment.utils import (
    DBTSelectorMaterializationContext,
)
from pipelines.common.utils.utils import convert_timezone
from pipelines.treatment__sppo_viagens import constants

D0_HOUR_THRESHOLD = 14


@task(cache_policy=NO_CACHE)
def prepare_sppo_viagens_contexts(
    env: Optional[str],
    datetime_start: Optional[str],
    datetime_end: Optional[str],
    additional_vars: Optional[dict],
    timestamp,
) -> list[DBTSelectorMaterializationContext]:
    """Prepara contextos para materialização SPPO com lógica D0."""
    scheduled_time = convert_timezone(runtime.flow_run.scheduled_start_time)
    is_second_run = (
        scheduled_time.hour >= D0_HOUR_THRESHOLD and datetime_start is None and datetime_end is None
    )

    if is_second_run:
        d1 = (scheduled_time + timedelta(days=1)).date()
        datetime_start = f"{d1}T00:00:00"
        datetime_end = f"{d1}T23:59:59"

    selector = (
        constants.VIAGENS_SPPO_D0_SELECTOR if is_second_run else constants.VIAGENS_SPPO_SELECTOR
    )
    snapshot_selector = None if is_second_run else constants.VIAGENS_SPPO_SNAPSHOT_SELECTOR

    if datetime_start and datetime_end:
        start = datetime.fromisoformat(datetime_start).date()
        end = datetime.fromisoformat(datetime_end).date()
        run_dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    else:
        run_dates = [None]

    all_contexts = []

    for run_date in run_dates:
        is_last = run_date == run_dates[-1]

        if run_date is not None:
            run_date_str = run_date.strftime("%Y-%m-%d")
            iter_start = f"{run_date_str}T00:00:00"
            iter_end = f"{run_date_str}T23:59:59"
            iter_vars = {**(additional_vars or {}), "run_date": run_date_str}
        else:
            iter_start = datetime_start
            iter_end = datetime_end
            iter_vars = additional_vars

        contexts = create_materialization_contexts(
            env=env,
            selectors=[selector],
            timestamp=timestamp,
            datetime_start=iter_start,
            datetime_end=iter_end,
            additional_vars=iter_vars,
            test_scheduled_time=None,
            force_test_run=False,
            snapshot_selector=snapshot_selector if is_last else None,
        )

        all_contexts.extend(contexts)

    return all_contexts
