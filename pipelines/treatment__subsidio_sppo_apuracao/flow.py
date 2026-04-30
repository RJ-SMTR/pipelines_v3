# -*- coding: utf-8 -*-
"""Flow de apuração de subsídio SPPO

Executa modelos dbt de apuração e verifica qualidade dos dados de subsídio SPPO.
Aplica lógica de versionamento baseada nas datas de início das versões:
- V8: antes de 2024-08-16
- V9: 2024-08-16 a 2025-01-04
- V14: 2025-01-05 em diante
"""

from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import requests
from prefect import flow, runtime, task
from prefect.cache_policies import NO_CACHE

from pipelines.common import constants as smtr_constants
from pipelines.common.tasks import (
    get_run_env,
    get_scheduled_timestamp,
    initialize_sentry,
    setup_environment,
)
from pipelines.common.treatment.default_treatment.utils import (
    dbt_test_notify_discord,
    parse_dbt_test_output,
    rename_treatment_flow_run,
    run_dbt,
    run_dbt_tests,
)
from pipelines.common.utils.discord import send_discord_message
from pipelines.common.utils.secret import get_env_secret
from pipelines.control__jae_verificacao_captura.tasks import (
    create_capture_check_discord_message,
    get_capture_gaps,
    jae_capture_check_get_ts_range,
)
from pipelines.treatment__subsidio_sppo_apuracao import constants


@task(cache_policy=NO_CACHE)
def get_apuracao_date_range(
    start_date: Optional[str],
    end_date: Optional[str],
) -> tuple[str, str]:
    if start_date is None:
        start_date = (date.today() - timedelta(days=7)).isoformat()
    if end_date is None:
        end_date = (date.today() - timedelta(days=7)).isoformat()
    return start_date, end_date


@task(cache_policy=NO_CACHE)
def compute_partitions(start_date: str, end_date: str) -> str:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    dates = []
    current = start
    while current <= end:
        dates.append(f"date({current.year}, {current.month}, {current.day})")
        current += timedelta(days=1)
    return ", ".join(dates)


@task(cache_policy=NO_CACHE)
def fetch_repo_version() -> str:
    response = requests.get(
        "https://api.github.com/repos/RJ-SMTR/pipelines_v3/commits",
        timeout=60,
    )
    response.raise_for_status()
    return response.json()[0]["sha"]


@task(cache_policy=NO_CACHE)
def run_jae_capture_checks(
    env: str,
    timestamp: datetime,
    raw_start_date: Optional[str],
    raw_end_date: Optional[str],
    table_ids: list[str],
) -> bool:
    end_captura = None
    if raw_end_date is not None:
        end_captura = (date.fromisoformat(raw_end_date) + timedelta(days=7)).isoformat()

    ts_start, ts_end = jae_capture_check_get_ts_range(
        timestamp=timestamp,
        retroactive_days=7,
        timestamp_captura_start=raw_start_date,
        timestamp_captura_end=end_captura,
    )

    webhook_url = get_env_secret(smtr_constants.WEBHOOKS_SECRET_PATH)["subsidio_data_check"]
    has_missing = False

    for table_id in table_ids:
        timestamps = get_capture_gaps(
            env=env,
            table_id=table_id,
            timestamp_captura_start=ts_start,
            timestamp_captura_end=ts_end,
        )
        message = create_capture_check_discord_message(
            table_id=table_id,
            timestamps=timestamps,
            timestamp_captura_start=ts_start,
            timestamp_captura_end=ts_end,
        )
        send_discord_message(message=message, webhook_url=webhook_url)
        if len(timestamps) > 0:
            has_missing = True

    return has_missing


@task(cache_policy=NO_CACHE)
def run_pre_tests(start_date: str, end_date: str) -> bool:
    tz = ZoneInfo(smtr_constants.TIMEZONE)
    dt_start = datetime.fromisoformat(f"{start_date}T00:00:00").replace(tzinfo=tz)
    dt_end = datetime.fromisoformat(f"{end_date}T23:59:59").replace(tzinfo=tz)

    dbt_logs, dbt_vars = run_dbt_tests(
        dbt_test=constants.SUBSIDIO_SPPO_PRE_TEST,
        datetime_start=dt_start,
        datetime_end=dt_end,
    )

    dbt_test_notify_discord(
        dbt_test=constants.SUBSIDIO_SPPO_PRE_TEST,
        dbt_vars=dbt_vars,
        dbt_logs=dbt_logs,
        webhook_key="subsidio_data_check",
        raise_check_error=False,
    )

    results = parse_dbt_test_output(dbt_logs)
    return any(r["result"] != "PASS" for r in results.values())


@task(cache_policy=NO_CACHE)
def run_apuracao_selectors(start_date: str, end_date: str, dbt_vars: dict) -> None:
    v9_inicio = constants.DATA_SUBSIDIO_V9_INICIO

    if end_date < v9_inicio:
        run_dbt(dbt_obj=constants.APURACAO_V8_SELECTOR, dbt_vars=dbt_vars)
    elif start_date >= v9_inicio:
        run_dbt(dbt_obj=constants.APURACAO_V9_SELECTOR, dbt_vars=dbt_vars)
        run_dbt(dbt_obj=constants.MONITORAMENTO_SELECTOR, dbt_vars=dbt_vars)
    else:
        v8_end = (date.fromisoformat(v9_inicio) - timedelta(days=1)).isoformat()
        v8_vars = {
            **dbt_vars,
            "start_date": start_date,
            "end_date": v8_end,
            "date_range_start": f"{start_date}T00:00:00",
            "date_range_end": f"{v8_end}T23:59:59",
        }
        v9_vars = {
            **dbt_vars,
            "start_date": v9_inicio,
            "end_date": end_date,
            "date_range_start": f"{v9_inicio}T00:00:00",
            "date_range_end": f"{end_date}T23:59:59",
        }
        run_dbt(dbt_obj=constants.APURACAO_V8_SELECTOR, dbt_vars=v8_vars)
        run_dbt(dbt_obj=constants.APURACAO_V9_SELECTOR, dbt_vars=v9_vars)
        run_dbt(dbt_obj=constants.MONITORAMENTO_SELECTOR, dbt_vars=v9_vars)


@task(cache_policy=NO_CACHE)
def run_pos_tests(start_date: str, end_date: str) -> None:
    tz = ZoneInfo(smtr_constants.TIMEZONE)
    v9_inicio = constants.DATA_SUBSIDIO_V9_INICIO
    v14_inicio = constants.DATA_SUBSIDIO_V14_INICIO

    def _run_test(dbt_test, s_date, e_date):
        dt_start = datetime.fromisoformat(f"{s_date}T00:00:00").replace(tzinfo=tz)
        dt_end = datetime.fromisoformat(f"{e_date}T23:59:59").replace(tzinfo=tz)
        logs, vars_ = run_dbt_tests(
            dbt_test=dbt_test,
            datetime_start=dt_start,
            datetime_end=dt_end,
        )
        dbt_test_notify_discord(
            dbt_test=dbt_test,
            dbt_vars=vars_,
            dbt_logs=logs,
            webhook_key="subsidio_data_check",
            raise_check_error=False,
        )

    if end_date < v9_inicio:
        _run_test(constants.SUBSIDIO_SPPO_V8_POS_TEST, start_date, end_date)
    elif start_date >= v9_inicio:
        if end_date < v14_inicio:
            _run_test(constants.SUBSIDIO_SPPO_V9_POS_TEST, start_date, end_date)
        elif start_date >= v14_inicio:
            _run_test(constants.SUBSIDIO_SPPO_V14_POS_TEST, start_date, end_date)
        else:
            v13_end = (date.fromisoformat(v14_inicio) - timedelta(days=1)).isoformat()
            _run_test(constants.SUBSIDIO_SPPO_V9_POS_TEST, start_date, v13_end)
            _run_test(constants.SUBSIDIO_SPPO_V14_POS_TEST, v14_inicio, end_date)
    else:
        v8_end = (date.fromisoformat(v9_inicio) - timedelta(days=1)).isoformat()
        _run_test(constants.SUBSIDIO_SPPO_V8_POS_TEST, start_date, v8_end)
        if end_date >= v14_inicio:
            v13_end = (date.fromisoformat(v14_inicio) - timedelta(days=1)).isoformat()
            _run_test(constants.SUBSIDIO_SPPO_V9_POS_TEST, v9_inicio, v13_end)
            _run_test(constants.SUBSIDIO_SPPO_V14_POS_TEST, v14_inicio, end_date)
        else:
            _run_test(constants.SUBSIDIO_SPPO_V9_POS_TEST, v9_inicio, end_date)


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__subsidio_sppo_apuracao(
    env: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    materialize_sppo_veiculo_dia: bool = False,
    test_only: bool = False,
    skip_pre_test: bool = False,
    table_ids_jae: Optional[list[str]] = None,
):
    if table_ids_jae is None:
        table_ids_jae = ["transacao", "transacao_riocard", "gps_validador"]

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

    if materialize_sppo_veiculo_dia:
        print(
            "AVISO: materialização de sppo_veiculo_dia não implementada nesta versão. "
            "Configure o deployment rj-treatment--sppo_veiculo_dia para execução separada."
        )

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
