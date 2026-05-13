# -*- coding: utf-8 -*-
"""Flow de apuração de subsídio SPPO

Executa modelos dbt de apuração e verifica qualidade dos dados de subsídio SPPO.
Aplica lógica de versionamento baseada nas datas de início das versões:
- V8: antes de 2024-08-16
- V9: 2024-08-16 a 2025-01-04
- V14: 2025-01-05 em diante
"""

from typing import Optional

from prefect import flow, runtime

from pipelines.common.tasks import (
    get_run_env,
    get_scheduled_timestamp,
    initialize_sentry,
    setup_environment,
)
from pipelines.common.treatment.default_treatment.utils import (
    rename_treatment_flow_run,
    run_dbt,
)
from pipelines.treatment__subsidio_sppo_apuracao import constants
from pipelines.treatment__subsidio_sppo_apuracao.tasks import (
    compute_partitions,
    fetch_repo_version,
    get_apuracao_date_range,
    run_apuracao_selectors,
    run_jae_capture_checks,
    run_pos_tests,
    run_pre_tests,
)


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__subsidio_sppo_apuracao(  # noqa: PLR0913
    env: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    test_only: bool = False,
    skip_pre_test: bool = False,
    table_ids_jae: Optional[list[str]] = None,
):
    if table_ids_jae is None:
        table_ids_jae = []

    raw_start_date = start_date
    raw_end_date = end_date

    initialize_sentry(env=env)
    setup_environment(env=env)

    env_value = get_run_env(env=env, deployment_name=runtime.deployment.name)
    timestamp = get_scheduled_timestamp()

    start_date, end_date = get_apuracao_date_range(start_date=start_date, end_date=end_date)
    partitions = compute_partitions(start_date=start_date, end_date=end_date)
    version = fetch_repo_version()

    dbt_vars = {
        "start_date": start_date,
        "end_date": end_date,
        "date_range_start": f"{start_date}T00:00:00",
        "date_range_end": f"{end_date}T23:59:59",
        "partitions": partitions,
        "version": version,
        "tipo_teste": "subsidio",
    }

    if skip_pre_test:
        missing_timestamps = False
    else:
        missing_timestamps = run_jae_capture_checks(
            env=env_value,
            timestamp=timestamp,
            raw_start_date=raw_start_date,
            raw_end_date=raw_end_date,
            table_ids=table_ids_jae,
        )

    if test_only:
        run_pre_tests(start_date=start_date, end_date=end_date)
        run_pos_tests(start_date=start_date, end_date=end_date)
        return

    if skip_pre_test:
        skip_materialization = False
    else:
        test_failed = run_pre_tests(start_date=start_date, end_date=end_date)
        skip_materialization = missing_timestamps or test_failed

    if not skip_materialization:
        run_apuracao_selectors(start_date=start_date, end_date=end_date, dbt_vars=dbt_vars)
        run_pos_tests(start_date=start_date, end_date=end_date)
        run_dbt(
            dbt_obj=constants.SNAPSHOT_SUBSIDIO_SELECTOR,
            dbt_vars=dbt_vars,
            is_snapshot=True,
        )
