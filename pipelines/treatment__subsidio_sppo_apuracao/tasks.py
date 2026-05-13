# -*- coding: utf-8 -*-
"""Tasks para o flow de apuração de subsídio SPPO"""

from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import requests
from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import (
    dbt_test_notify_discord,
    parse_dbt_test_output,
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
    """
    Retorna o range de datas para apuração de subsídio SPPO.

    Se as datas não forem fornecidas, usa 7 dias atrás da data atual.

    Args:
        start_date (Optional[str]): Data de início (formato ISO)
        end_date (Optional[str]): Data de fim (formato ISO)

    Returns:
        tuple[str, str]: (start_date, end_date) em formato ISO
    """
    if start_date is None:
        start_date = (date.today() - timedelta(days=7)).isoformat()  # noqa: DTZ011
    if end_date is None:
        end_date = (date.today() - timedelta(days=7)).isoformat()  # noqa: DTZ011
    return start_date, end_date


@task(cache_policy=NO_CACHE)
def compute_partitions(start_date: str, end_date: str) -> str:
    """
    Calcula as partições de data em formato BigQuery.

    Gera uma string com todas as datas entre start_date e end_date
    no formato: date(YYYY, M, D), date(YYYY, M, D), ...

    Args:
        start_date (str): Data de início (formato ISO)
        end_date (str): Data de fim (formato ISO)

    Returns:
        str: String com as partições separadas por vírgula
    """
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
    """
    Busca o SHA do último commit do repositório via GitHub API.

    Returns:
        str: SHA do último commit

    Raises:
        requests.HTTPError: Se a requisição falhar
    """
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
    """
    Executa verificações de captura de dados da Jaé.

    Valida se todos os dados foram capturados para as tabelas especificadas
    e envia notificação no Discord em caso de falhas.

    Args:
        env (str): Ambiente (prod ou dev)
        timestamp (datetime): Timestamp de execução do flow
        raw_start_date (Optional[str]): Data de início para captura (formato ISO)
        raw_end_date (Optional[str]): Data de fim para captura (formato ISO)
        table_ids (list[str]): Lista de IDs de tabelas para verificar

    Returns:
        bool: True se houver timestamps com dados faltantes, False caso contrário
    """
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
    """
    Executa testes PRE-materialization (verificação de dados brutos).

    Args:
        start_date (str): Data de início (formato ISO)
        end_date (str): Data de fim (formato ISO)

    Returns:
        bool: True se houver falhas nos testes, False caso contrário
    """
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
    """
    Executa os seletores dbt de apuração baseado no período e versão.

    Lógica:
    - Se end_date < V9: executa apenas V8
    - Se start_date >= V9: executa V9 + monitoramento
    - Se start_date < V9 <= end_date: executa V8 + V9 + monitoramento com ranges separados

    Args:
        start_date (str): Data de início (formato ISO)
        end_date (str): Data de fim (formato ISO)
        dbt_vars (dict): Variáveis para passar ao dbt
    """
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
    """
    Executa testes POS-materialization (verificação de dados processados).

    Aplica lógica de versão similar a `run_apuracao_selectors`:
    - V8 (antes de 2024-08-16)
    - V9 (2024-08-16 a 2025-01-04)
    - V14 (2025-01-05 em diante)

    Args:
        start_date (str): Data de início (formato ISO)
        end_date (str): Data de fim (formato ISO)
    """
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
