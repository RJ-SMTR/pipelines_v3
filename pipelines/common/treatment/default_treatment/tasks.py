# -*- coding: utf-8 -*-
from datetime import datetime, time, timedelta
from time import sleep
from typing import Optional
from zoneinfo import ZoneInfo

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import (
    DBTSelector,
    DBTSelectorMaterializationContext,
    DBTTest,
    IncompleteDataError,
    dbt_test_notify_discord,
    run_dbt,
    run_dbt_tests,
)
from pipelines.common.utils.cron import cron_get_last_date
from pipelines.common.utils.gcp.bigquery import SourceTable
from pipelines.common.utils.redis import get_redis_client
from pipelines.common.utils.utils import convert_timezone


@task(cache_policy=NO_CACHE)
def create_materialization_contexts(  # noqa: PLR0913
    env: str,
    selectors: list[DBTSelector],
    timestamp: datetime,
    datetime_start: Optional[str],
    datetime_end: Optional[str],
    additional_vars: Optional[dict],
    test_scheduled_time: time,
    force_test_run: bool,
    snapshot_selector: Optional[DBTSelector] = None,
) -> list[DBTSelectorMaterializationContext]:
    """
    Cria os contextos de materialização a partir dos selectors informados.

    Args:
        env (str): prod ou dev.
        selectors (list[DBTSelector]): Lista de selectors do dbt.
        timestamp (datetime): Timestamp de execução do flow.
        datetime_start (Optional[str]): Parâmetro de data e hora de inicio manual da materialização.
        datetime_end (Optional[str]): Parâmetro de data e hora de final manual da materialização.
        additional_vars (Optional[dict]): Variáveis adicionais para o dbt.
        test_scheduled_time (time): Horário agendado para execução dos testes.
        force_test_run (bool): Força a execução dos testes.
        snapshot_selector (Optional[DBTSelector]): Selector para snapshot.

    Returns:
        list[DBTSelectorMaterializationContext]: Lista de contextos de materialização.
    """
    contexts = []
    for s in selectors:
        ctx = DBTSelectorMaterializationContext(
            env=env,
            selector=s,
            timestamp=timestamp,
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            additional_vars=additional_vars,
            test_scheduled_time=test_scheduled_time,
            force_test_run=force_test_run,
            snapshot_selector=snapshot_selector,
        )
        if ctx.should_run:
            contexts.append(ctx)

    return contexts


@task(cache_policy=NO_CACHE)
def wait_data_sources(
    context: DBTSelectorMaterializationContext,
    skip: bool,
):
    """
    Aguarda a completude das fontes de dados associadas ao selector.

    Args:
        context (DBTSelectorMaterializationContext): Contexto de materialização.
        skip (bool): Indica se a verificação de completude deve ser ignorada.
    """
    if skip:
        print("Pulando verificação de completude dos dados")
        return
    count = 0
    wait_limit = 10
    env = context.env
    datetime_start = context.datetime_start
    datetime_end = context.datetime_end
    for ds in context.selector.data_sources:
        print("Checando completude dos dados")
        complete = False
        while not complete:
            if isinstance(ds, SourceTable):
                name = f"{ds.source_name}.{ds.table_id}"
                uncaptured_timestamps = ds.set_env(env=env).get_uncaptured_timestamps(
                    timestamp=datetime_end,
                    retroactive_days=max(2, (datetime_end - datetime_start).days),
                )

                complete = len(uncaptured_timestamps) == 0
            elif isinstance(ds, DBTSelector):
                name = f"{ds.name}"
                complete = ds.is_up_to_date(env=env, timestamp=datetime_end)
            elif isinstance(ds, dict):
                # source dicionário utilizado para compatibilização com flows antigos
                name = ds["redis_key"]
                redis_client = get_redis_client()
                last_materialization = datetime.strptime(
                    redis_client.get(name)[ds["dict_key"]],
                    ds["datetime_format"],
                ).replace(tzinfo=ZoneInfo(smtr_constants.TIMEZONE))
                last_schedule = cron_get_last_date(
                    cron_expr=ds["schedule_cron"],
                    timestamp=datetime_end,
                )
                last_materialization = convert_timezone(timestamp=last_materialization)

                complete = last_materialization >= last_schedule - timedelta(
                    hours=ds.get("delay_hours", 0)
                )

            else:
                raise NotImplementedError(f"Espera por fontes do tipo {type(ds)} não implementada")

            print(f"Checando dados do {type(ds)} {name}")
            if not complete:
                if count < wait_limit:
                    print("Dados incompletos, tentando novamente")
                    sleep(60)
                    count += 1
                else:
                    print("Tempo de espera esgotado")
                    raise IncompleteDataError(f"{type(ds)} {name} incompleto")
            else:
                print("Dados completos")


@task(cache_policy=NO_CACHE)
def run_dbt_selectors(
    contexts: list[DBTSelectorMaterializationContext], flags: Optional[list[str]]
):
    """
    Executa os selectors do dbt para cada contexto de materialização.

    Args:
        contexts (list[DBTSelectorMaterializationContext]): Lista de contextos de materialização.
        flags (Optional[list[str]]): Flags adicionais para execução do dbt.
    """
    for context in contexts:
        run_dbt(dbt_obj=context.selector, dbt_vars=context.dbt_vars, flags=flags)

    return contexts


@task(cache_policy=NO_CACHE)
def run_dbt_snapshots(
    contexts: list[DBTSelectorMaterializationContext], flags: Optional[list[str]]
):
    """
    Executa os snapshots do dbt para cada contexto de materialização que possua
    um snapshot_selector.

    Args:
        contexts (list[DBTSelectorMaterializationContext]): Lista de contextos de materialização.
        flags (Optional[list[str]]): Flags adicionais para execução do dbt.
    """
    for context in contexts:
        if context.snapshot_selector is None:
            continue
        run_dbt(
            dbt_obj=context.snapshot_selector,
            dbt_vars=context.dbt_vars,
            flags=flags,
            is_snapshot=True,
        )

    return contexts


@task(cache_policy=NO_CACHE)
def run_dbt_selector_tests(
    contexts: list[DBTSelectorMaterializationContext],
    mode: str,
):
    """
    Executa os testes do dbt para cada contexto de materialização.

    Args:
        contexts (list[DBTSelectorMaterializationContext]): Lista de contextos de materialização.
        mode (str): Modo de execução do teste (pre ou post).
    """
    for context in contexts:
        if not context[f"should_run_{mode}_test"]:
            continue

        dbt_test: DBTTest = context.selector[f"{mode}_test"]

        if dbt_test is not None:
            log, _ = run_dbt_tests(
                dbt_test=dbt_test,
                datetime_start=context.datetime_start,
                datetime_end=context.datetime_end,
            )
        context[f"{mode}_test_log"] = log

    return contexts


@task(cache_policy=NO_CACHE)
def task_dbt_selector_test_notify_discord(
    context: DBTSelectorMaterializationContext,
    mode: str,
    webhook_key: str = "dataplex",
    raise_check_error: bool = True,
    additional_mentions: Optional[list] = None,
):
    """
    Processa os resultados dos testes do dbt e envia notificações para o Discord.

    Args:
        context (DBTSelectorMaterializationContext): Contexto de materialização.
        mode (str): Modo do teste (pre ou post).
        webhook_key (str): Chave do webhook do Discord.
        raise_check_error (bool): Indica se deve lançar erro em caso de falha nos testes.
        additional_mentions (Optional[list]): Menções adicionais na mensagem.
    """
    test: DBTTest = context.selector[f"{mode}_test"]
    dbt_vars: dict = context[f"{mode}_test_dbt_vars"]
    dbt_logs: str = context[f"{mode}_test_log"]

    dbt_test_notify_discord(
        dbt_test=test,
        dbt_vars=dbt_vars,
        dbt_logs=dbt_logs,
        webhook_key=webhook_key,
        raise_check_error=raise_check_error,
        additional_mentions=additional_mentions,
    )


@task(cache_policy=NO_CACHE)
def save_materialization_datetime_redis(contexts: list[DBTSelectorMaterializationContext]):
    """
    Salva no Redis o datetime da última materialização do selector.

    Args:
        contexts (list[DBTSelectorMaterializationContext]): Contexto de materialização.
    """
    for context in contexts:
        context.selector.set_redis_materialized_datetime(
            env=context.env, timestamp=context.datetime_end
        )
