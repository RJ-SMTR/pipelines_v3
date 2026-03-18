# -*- coding: utf-8 -*-
"""Tasks para o flow de verificação da captura dos dados da Jaé"""

from datetime import datetime, timedelta
from typing import Optional

import pandas_gbq
from prefect import runtime, task
from prefect.cache_policies import NO_CACHE

from pipelines.common import constants as smtr_constants
from pipelines.common.utils.utils import convert_timezone
from pipelines.control__jae_verificacao_captura import constants
from pipelines.control__jae_verificacao_captura.utils import (
    get_jae_timestamp_captura_count,
    save_capture_check_results,
)


@task(cache_policy=NO_CACHE)
def jae_capture_check_get_ts_range(
    timestamp: datetime,
    retroactive_days: int,
    timestamp_captura_start: Optional[str],
    timestamp_captura_end: Optional[str],
) -> tuple[datetime, datetime]:
    """
    Calcula o intervalo de para checagem da captura dos dados da Jaé.

    Args:
        timestamp (datetime): Data e hora de execução do flow
        retroactive_days (int): Número de dias a subtrair de `timestamp` para definir
            o início do intervalo
        timestamp_captura_start (Optional[str]): Parâmetro do flow para definição
            de timestamp inicial de forma manual
        timestamp_captura_end (Optional[str]): Parâmetro do flow para definição
            de timestamp final de forma manual

    Returns:
        tuple[datetime, datetime]: Intervalo com:
            - start (datetime): Início do intervalo
            - end (datetime): Fim do intervalo
    """
    if timestamp_captura_start is not None:
        start = datetime.fromisoformat(timestamp_captura_start)
    else:
        start = (timestamp - timedelta(days=retroactive_days)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

    start = convert_timezone(timestamp=start)

    if timestamp_captura_end is not None:
        end = datetime.fromisoformat(timestamp_captura_end)
    else:
        end = start.replace(hour=23, minute=59, second=59, microsecond=0)

    end = convert_timezone(timestamp=end)

    return start, end


@task(cache_policy=NO_CACHE)
def get_capture_gaps(
    env: str,
    table_id: str,
    timestamp_captura_start: datetime,
    timestamp_captura_end: datetime,
) -> list[str]:
    """
    Identifica timestamps com divergência entre os dados presentes
    na base da Jaé e os dados capturados no datalake.

    Args:
        env (str): prod ou dev
        table_id (str): Nome da tabela no BigQuery
        timestamp_captura_start (datetime): Início do intervalo de verificação
        timestamp_captura_end (datetime): Fim do intervalo de verificação

    Returns:
        list[str]: Lista de strings no formato `%Y-%m-%d %H:%M:%S` representando os timestamps
        com divergência de contagem entre JAE e datalake
    """
    params = constants.CHECK_CAPTURE_PARAMS[table_id]
    timestamp_column = params["timestamp_column"]
    source = params["source"]
    df_jae = get_jae_timestamp_captura_count(
        source=source,
        timestamp_column=timestamp_column,
        timestamp_captura_start=timestamp_captura_start,
        timestamp_captura_end=timestamp_captura_end,
        final_timestamp_exclusive=params["final_timestamp_exclusive"],
    )

    primary_keys = params.get("primary_keys")
    if primary_keys is None:
        primary_keys = "1"
    elif len(primary_keys) == 1:
        primary_keys = f"DISTINCT {primary_keys[0]}"
    else:
        primary_keys = f"DISTINCT TO_JSON_STRING(STRUCT({', '.join(primary_keys)}))"

    query_datalake = f"""
    WITH contagens AS (
        SELECT
            timestamp_captura,
            COUNT({primary_keys}) AS total_datalake
        FROM
            {params["datalake_table"]}
        WHERE
            DATA BETWEEN '{timestamp_captura_start.date().isoformat()}'
            AND '{timestamp_captura_end.date().isoformat()}'
            AND timestamp_captura BETWEEN '{timestamp_captura_start.strftime("%Y-%m-%d %H:%M:%S")}'
            AND '{timestamp_captura_end.strftime("%Y-%m-%d %H:%M:%S")}'
        GROUP BY
            1
    ),
    timestamps_captura AS (
        SELECT
            DATETIME(timestamp_captura) AS timestamp_captura
        FROM
            UNNEST(
                GENERATE_TIMESTAMP_ARRAY(
                    '{timestamp_captura_start.strftime("%Y-%m-%d %H:%M:%S")}',
                    '{timestamp_captura_end.strftime("%Y-%m-%d %H:%M:%S")}',
                    INTERVAL 1 minute
                )
            ) AS timestamp_captura
    )
    SELECT
        timestamp_captura,
        COALESCE(total_datalake, 0) AS total_datalake
    FROM
        timestamps_captura
    LEFT JOIN
        contagens
    USING
        (timestamp_captura)
    """

    print(f"Executando query\n{query_datalake}")

    df_datalake = pandas_gbq.read_gbq(
        query_datalake,
        project_id=smtr_constants.PROJECT_NAME[env],
    )

    df_datalake["timestamp_captura"] = df_datalake["timestamp_captura"].dt.tz_localize(
        smtr_constants.TIMEZONE
    )

    df_merge = df_jae.merge(df_datalake, how="left", on="timestamp_captura")
    df_merge["table_id"] = table_id
    df_merge["total_datalake"] = df_merge["total_datalake"].astype(int)
    df_merge["total_jae"] = df_merge["total_jae"].astype(int)
    df_merge["indicador_captura_correta"] = df_merge["total_datalake"] == df_merge["total_jae"]

    timestamps = (
        df_merge.loc[~df_merge["indicador_captura_correta"]]
        .sort_values(by=["timestamp_captura"])["timestamp_captura"]
        .dt.strftime("%Y-%m-%d %H:%M:%S")
        .tolist()
    )

    save_capture_check_results(env=env, results=df_merge)

    if len(timestamps) > 0:
        ts_log = [f'"{t}",' for t in timestamps]
        print(
            "[{table_id}] Os seguintes timestamps estão divergentes:\n{timestamps_str}".format(
                table_id=table_id, timestamps_str="\n".join(ts_log)
            )
        )
    else:
        print(f"[{table_id}] Todos os dados foram capturados com sucesso!")

    return timestamps


@task(cache_policy=NO_CACHE)
def create_capture_check_discord_message(
    table_id: str,
    timestamps: list[dict],
    timestamp_captura_start: datetime,
    timestamp_captura_end: datetime,
) -> str:
    """
    Cria a mensagem para notificação no Discord com o resultado da verificação de captura de dados

    Args:
        table_id (str): Nome da tabela no BigQuery
        timestamps (list[dict]): Lista de timestamps com falhas na captura
        timestamp_captura_start (datetime): Início do intervalo analisado
        timestamp_captura_end (datetime): Fim do intervalo analisado

    Returns:
        str: Mensagem para ser enviada no Discord
    """
    timestamps_len = len(timestamps)
    message = f"""
Tabela: {table_id}
De {timestamp_captura_start.isoformat()} até {timestamp_captura_end.isoformat()}
Foram encontradas {timestamps_len} timestamps com dados faltantes
"""
    if timestamps_len > 0:
        mentions_tag = f" - <@&{smtr_constants.OWNERS_DISCORD_MENTIONS['dados_smtr']['user_id']}>"
        message = f":red_circle: {message}"
        message += (
            "\n"
            + f"https://prefect.mobilidade.rio/runs/flow-run/{runtime.flow_run.id}"
            + "\n"
            + mentions_tag
        )
    else:
        message = f":green_circle: {message}"
    return message
