# -*- coding: utf-8 -*-
from datetime import datetime, time, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from prefect import runtime, task

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import (
    DBTSelector,
    DBTSelectorMaterializationContext,
    DBTTest,
    DBTTestFailedError,
    IncompleteDataError,
    parse_dbt_test_output,
    run_dbt,
)
from pipelines.common.utils.cron import cron_get_last_date
from pipelines.common.utils.discord import format_send_discord_message
from pipelines.common.utils.gcp.bigquery import SourceTable
from pipelines.common.utils.redis import get_redis_client
from pipelines.common.utils.secret import get_secret
from pipelines.common.utils.utils import convert_timezone


@task
def create_materialization_contexts(  # noqa: PLR0913
    env: str,
    selectors: list[DBTSelector],
    timestamp: datetime,
    datetime_start: Optional[str],
    datetime_end: Optional[str],
    additional_vars: Optional[dict],
    test_scheduled_time: time,
    force_test_run: bool,
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
        )
        if ctx.should_run:
            contexts.append(ctx)

    return contexts


@task
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
                    time.sleep(60)
                    count += 1
                else:
                    print("Tempo de espera esgotado")
                    raise IncompleteDataError(f"{type(ds)} {name} incompleto")
            else:
                print("Dados completos")


@task
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


@task
def run_dbt_tests(
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
        dbt_vars = context[f"{mode}_test_dbt_vars"]

        if dbt_test is not None:
            dbt_vars = dbt_test.get_test_vars(
                datetime_start=context.datetime_start,
                datetime_end=context.datetime_end,
            )
            log = run_dbt(dbt_obj=dbt_test, dbt_vars=dbt_vars, raise_on_failure=False)

        context[f"{mode}_test_log"] = log


@task
def dbt_test_notify_discord(  # noqa: PLR0912, PLR0915
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
    if dbt_logs is None:
        return

    test_descriptions = test.test_descriptions

    checks_results = parse_dbt_test_output(dbt_logs)

    webhook_url = get_secret(secret_path=smtr_constants.WEBHOOKS_SECRET_PATH)[webhook_key]
    additional_mentions = additional_mentions or []
    mentions = [*additional_mentions, "dados_smtr"]
    mention_tags = "".join(
        [f" - <@&{smtr_constants.OWNERS_DISCORD_MENTIONS[m]['user_id']}>\n" for m in mentions]
    )

    test_check = all(test["result"] == "PASS" for test in checks_results.values())

    keys = [
        ("date_range_start", "date_range_end"),
        ("start_date", "end_date"),
        ("run_date", None),
        ("data_versao_gtfs", None),
    ]

    start_date = None
    end_date = None

    for start_key, end_key in keys:
        if start_key in dbt_vars and "T" in dbt_vars[start_key]:
            start_date = dbt_vars[start_key].split("T")[0]

            if end_key and end_key in dbt_vars and "T" in dbt_vars[end_key]:
                end_date = dbt_vars[end_key].split("T")[0]

            break
        elif start_key in dbt_vars:
            start_date = dbt_vars[start_key]

            if end_key and end_key in dbt_vars:
                end_date = dbt_vars[end_key]

    date_range = (
        start_date
        if not end_date
        else (start_date if start_date == end_date else f"{start_date} a {end_date}")
    )

    if "(target='dev')" in dbt_logs or "(target='hmg')" in dbt_logs:
        formatted_messages = [
            ":green_circle: " if test_check else ":red_circle: ",
            f"**[DEV] Data Quality Checks - {runtime.flow_run.flow_name} - {date_range}**\n\n",
        ]
    else:
        formatted_messages = [
            ":green_circle: " if test_check else ":red_circle: ",
            f"**Data Quality Checks - {runtime.flow_run.flow_name} - {date_range}**\n\n",
        ]

    table_groups = {}

    for test_id, test_result in checks_results.items():
        parts = test_id.split("__")
        if len(parts) == 2:  # noqa: PLR2004
            table_name = parts[1]
        else:
            table_name = parts[2]

        if table_name not in table_groups:
            table_groups[table_name] = []

        table_groups[table_name].append((test_id, test_result))

    for table_name, tests in table_groups.items():
        formatted_messages.append(f"*{table_name}:*\n")

        for test_id, test_result in tests:
            matched_description = None
            for existing_table_id, test_configs in test_descriptions.items():
                if table_name in existing_table_id:
                    for existing_test_id, test_info in test_configs.items():
                        if existing_test_id in test_id:
                            matched_description = test_info.get("description", test_id).replace(
                                "{column_name}",
                                test_id.split("__")[1] if "__" in test_id else test_id,
                            )
                            break
                    if matched_description:
                        break

            test_id = test_id.replace("_", "\\_")  # noqa: PLW2901
            description = matched_description or f"Teste: {test_id}"

            test_message = (
                f"{':white_check_mark:' if test_result['result'] == 'PASS' else ':x:'} "
                f"{description}\n"
            )
            formatted_messages.append(test_message)

    formatted_messages.append("\n")
    formatted_messages.append(
        ":tada: **Status:** Sucesso"
        if test_check
        else ":warning: **Status:** Testes falharam. Necessidade de revisão dos dados finais!\n"
    )

    if not test_check:
        formatted_messages.append(mention_tags)

    try:
        format_send_discord_message(formatted_messages, webhook_url)
    except Exception as e:
        print(f"Falha ao enviar mensagem para o Discord: {e}", level="error")
        raise

    if not test_check and raise_check_error:
        raise DBTTestFailedError()


@task
def save_materialization_datetime_redis(context: DBTSelectorMaterializationContext):
    """
    Salva no Redis o datetime da última materialização do selector.

    Args:
        context (DBTSelectorMaterializationContext): Contexto de materialização.
    """
    context.selector.set_redis_materialized_datetime(
        env=context.env, timestamp=context.datetime_end
    )
