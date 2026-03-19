# -*- coding: utf-8 -*-
"""Funções para o flow de verificação da captura dos dados da Jaé"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import pandas_gbq
from croniter import croniter
from google.cloud import bigquery
from prefect import runtime
from pytz import timezone

from pipelines.common import constants as smtr_constants
from pipelines.common.capture.jae import constants as jae_constants
from pipelines.common.utils.database import create_database_url, create_engine
from pipelines.common.utils.gcp.bigquery import SourceTable
from pipelines.common.utils.secret import get_env_secret
from pipelines.common.utils.utils import convert_timezone
from pipelines.control__jae_verificacao_captura import constants


def get_capture_interval_minutes(source: SourceTable) -> int:
    """
    Retorna o intervalo de captura em minutos calculado a partir do cron do SourceTable.

    Args:
        source (SourceTable): Objeto que contém a expressão cron na propriedade `schedule_cron`.

    Returns:
        int: Intervalo entre capturas, em minutos.
    """
    cron_expr = source.schedule_cron
    base_time = datetime.now(tz=ZoneInfo(smtr_constants.TIMEZONE))
    iterador = croniter(cron_expr, base_time)
    next_time = iterador.get_next(datetime)
    prev_time = iterador.get_prev(datetime)

    return (next_time - prev_time).total_seconds() / 60


def get_jae_timestamp_captura_count_query(  # noqa: PLR0913
    engine: str,
    delay_query: str,
    capture_interval_minutes: int,
    capture_query: str,
    timestamp_column: str,
    final_timestamp_exclusive: bool,
) -> str:
    """
    Gera uma query SQL para contar os dados da Jaé adaptada para PostgreSQL ou MySQL.

    Args:
        engine (str): Nome do banco de dados ('postgresql' ou 'mysql').
        delay_query (str): Expressão SQL usada para calcular o delay da captura.
        capture_interval_minutes (int): Intervalo de captura em minutos.
        capture_query (str): Query base que retorna os dados de captura.
        timestamp_column (str): Nome da coluna de timestamp da tabela.

    Returns:
        str: Query SQL completa para contar registros agrupados por timestamp_captura.
    """

    if engine == "postgresql":
        if final_timestamp_exclusive:
            count_query = f"""
                SELECT
                    TO_TIMESTAMP(
                        FLOOR(
                            EXTRACT(
                                EPOCH FROM {timestamp_column}) / ({capture_interval_minutes} * 60
                            )
                        ) * ({capture_interval_minutes} * 60)
                    )  AT TIME ZONE 'UTC' AS datetime_truncado,
                    COUNT(1) AS total_jae
                FROM
                    dados_jae
                GROUP BY
                    1

            """
        else:
            count_query = f"""
                SELECT
                    datetime_truncado,
                    COUNT(1) AS total_jae
                FROM
                (
                    SELECT
                        *,
                        TO_TIMESTAMP(
                            FLOOR(
                                EXTRACT(
                                    EPOCH FROM {timestamp_column}
                                )
                                / ({capture_interval_minutes} * 60)
                            ) * ({capture_interval_minutes} * 60)
                        )  AT TIME ZONE 'UTC' AS datetime_truncado
                    FROM dados_jae

                    UNION ALL

                    SELECT
                        *,
                        TO_TIMESTAMP(
                            FLOOR(
                                EXTRACT(
                                    EPOCH FROM {timestamp_column} - interval '1 second'
                                ) / ({capture_interval_minutes} * 60
                                )
                            ) * ({capture_interval_minutes} * 60)
                        )  AT TIME ZONE 'UTC' AS datetime_truncado
                    FROM
                        dados_jae
                    WHERE
                        TO_TIMESTAMP(
                            FLOOR(
                                EXTRACT(
                                    EPOCH FROM {timestamp_column})
                                    / ({capture_interval_minutes} * 60
                                )
                            ) * ({capture_interval_minutes} * 60)
                        )  AT TIME ZONE 'UTC' = {timestamp_column}
                )
                GROUP BY
                        1
            """
        return f"""
            WITH timestamps_captura AS (
                SELECT timestamp_captura, {delay_query} AS delay
                FROM (SELECT generate_series(
                    timestamp '{{timestamp_captura_start}}',
                    timestamp '{{timestamp_captura_end}}',
                    interval '{capture_interval_minutes} minute'
                ) AS timestamp_captura)
            ),
            dados_jae AS (
                {capture_query}
            ),
            contagens AS (
                {count_query}
            )
            SELECT
                tc.timestamp_captura,
                COALESCE(c.total_jae, 0) AS total_jae
            FROM
                timestamps_captura tc
            LEFT JOIN
                contagens c
            ON
                tc.timestamp_captura = c.datetime_truncado
                + (tc.delay + {capture_interval_minutes} || ' minutes')::interval
        """

    elif engine == "mysql":
        if final_timestamp_exclusive:
            join_condition = f"""
                d.{timestamp_column} >= tc.timestamp_inicial AND
                d.{timestamp_column} < tc.timestamp_final
            """
        else:
            join_condition = f"""
                d.{timestamp_column} BETWEEN tc.timestamp_inicial
                    AND tc.timestamp_final
            """

        return f"""
            WITH RECURSIVE timestamps_captura AS (
                SELECT
                    TIMESTAMP('{{timestamp_captura_start}}') AS timestamp_captura,
                    DATE_SUB(
                        TIMESTAMP('{{timestamp_captura_start}}'),
                        INTERVAL ({delay_query} + {capture_interval_minutes}) MINUTE
                    ) AS timestamp_inicial,
                    DATE_SUB(
                        TIMESTAMP('{{timestamp_captura_start}}'),
                        INTERVAL ({delay_query}) MINUTE
                    ) AS timestamp_final
                UNION ALL
                SELECT
                    timestamp_captura + INTERVAL {capture_interval_minutes} MINUTE,
                    DATE_SUB(
                        timestamp_captura  + INTERVAL {capture_interval_minutes} MINUTE,
                        INTERVAL ({delay_query} + {capture_interval_minutes}) MINUTE
                    ) AS timestamp_inicial,
                    DATE_SUB(
                        timestamp_captura  + INTERVAL {capture_interval_minutes} MINUTE,
                        INTERVAL ({delay_query}) MINUTE
                    ) AS timestamp_final
                FROM timestamps_captura
                WHERE
                    timestamp_captura
                    + INTERVAL {capture_interval_minutes} MINUTE
                    <= TIMESTAMP('{{timestamp_captura_end}}')
            ),
            dados_jae AS (
                {capture_query}
            ),
            jae_timestamp_captura AS (
                SELECT
                    tc.timestamp_captura,
                    d.{timestamp_column} as col
                FROM
                    timestamps_captura tc
                LEFT JOIN
                    dados_jae d
                ON {join_condition}
            )
            SELECT
                timestamp_captura,
                count(col) AS total_jae
            FROM
                jae_timestamp_captura
            GROUP BY
                timestamp_captura
        """

    else:
        raise NotImplementedError(f"Engine {engine} não implementada")


def get_jae_timestamp_captura_count(
    source: SourceTable,
    timestamp_column: str,
    timestamp_captura_start: datetime,
    timestamp_captura_end: datetime,
    final_timestamp_exclusive: bool,
) -> pd.DataFrame:
    """
    Retorna a contagem de registros por timestamp_captura de uma tabela da Jaé.

    Args:
        source (SourceTable): Objeto contendo informações da tabela
        timestamp_column (str): Nome da coluna de timestamp que os dados capturados são filtrados
        timestamp_captura_start (datetime): Data e hora inicial da janela de captura
        timestamp_captura_end (datetime): Data e hora final da janela de captura

    Returns:
        pd.DataFrame: DataFrame com duas colunas:
            - `timestamp_captura` (datetime): Coluna timestamp_captura correspondente.
            - `total_jae` (int): Contagem de registros na base da Jaé.
    """
    table_capture_params = jae_constants.JAE_TABLE_CAPTURE_PARAMS[source.table_id]
    database = table_capture_params["database"]
    credentials = get_env_secret(jae_constants.JAE_SECRET_PATH)
    database_settings = jae_constants.JAE_DATABASE_SETTINGS[database]
    engine = database_settings["engine"]
    url = create_database_url(
        engine=engine,
        host=database_settings["host"],
        user=credentials["user"],
        password=credentials["password"],
        database=database,
    )
    connection = create_engine(url)
    capture_delay_minutes = table_capture_params.get("capture_delay_minutes", {"0": 0})
    capture_delay_timestamps = [a for a in capture_delay_minutes.keys() if a != "0"]

    if len(capture_delay_timestamps) == 0:
        delay_query = f"{capture_delay_minutes['0']}"
    else:
        delay_query = "CASE\n"
        for t in [a for a in capture_delay_timestamps if a != "0"]:
            tc = (
                convert_timezone(timestamp=datetime.fromisoformat(t))
                .astimezone(tz=timezone("UTC"))
                .strftime("%Y-%m-%d %H:%M:%S")
            )
            delay_query += f"WHEN timestamp_captura >= '{tc}' THEN {capture_delay_minutes[t]}\n"

        delay_query += f"ELSE {capture_delay_minutes['0']}\nEND"

    delay = (
        max(*capture_delay_minutes.values())
        if len(capture_delay_timestamps) > 0
        else capture_delay_minutes["0"]
    )

    base_query_jae = get_jae_timestamp_captura_count_query(
        engine=engine,
        delay_query=delay_query,
        capture_interval_minutes=get_capture_interval_minutes(source=source),
        capture_query=table_capture_params["query"],
        timestamp_column=timestamp_column,
        final_timestamp_exclusive=final_timestamp_exclusive,
    )

    jae_start_ts = timestamp_captura_start
    jae_result = []

    while jae_start_ts < timestamp_captura_end:
        jae_end_ts = min(jae_start_ts + timedelta(days=1), timestamp_captura_end)

        jae_start_ts_utc = jae_start_ts.astimezone(tz=timezone("UTC"))
        jae_end_ts_utc = jae_end_ts.astimezone(tz=timezone("UTC"))

        jae_end_ts_utc_format = (
            jae_end_ts_utc - timedelta(minutes=1)
            if jae_end_ts_utc < timestamp_captura_end
            else jae_end_ts_utc
        )

        query = base_query_jae.format(
            timestamp_captura_start=jae_start_ts_utc.strftime("%Y-%m-%d %H:%M:%S"),
            timestamp_captura_end=jae_end_ts_utc_format.strftime("%Y-%m-%d %H:%M:%S"),
            start=(
                jae_start_ts_utc.replace(hour=0, minute=0, second=0, microsecond=0)
                - timedelta(minutes=delay + 1)
            ).strftime("%Y-%m-%d %H:%M:%S"),
            end=(jae_end_ts_utc + timedelta(minutes=delay))
            .replace(hour=23, minute=59, second=59, microsecond=59)
            .strftime("%Y-%m-%d %H:%M:%S"),
            delay=delay,
        )

        print(f"Executando query\n{query}")
        df_count_jae = pd.read_sql(
            sql=query,
            con=connection,
        )

        df_count_jae["timestamp_captura"] = (
            pd.to_datetime(df_count_jae["timestamp_captura"])
            .dt.tz_localize("UTC")
            .dt.tz_convert(smtr_constants.TIMEZONE)
        )

        jae_result.append(df_count_jae)

        jae_start_ts = jae_end_ts

    return pd.concat(jae_result)


def save_capture_check_results(env: str, results: pd.DataFrame):
    """
    Salva os resultados da verificação de captura no BigQuery.

    Args:
        env (str): dev ou prod
        results (pd.DataFrame): DataFrame contendo os resultados da verificação,
            com as seguintes colunas obrigatórias:
              - table_id (str): table_id no BigQuery
              - timestamp_captura (datetime): Data e hora da captura
              - total_datalake (int): Quantidade total de registros no datalake
              - total_jae (int): Quantidade total de registros na Jaé
              - indicador_captura_correta (bool): Se a quantidade de registros é a mesma
    """
    project_id = smtr_constants.PROJECT_NAME[env]
    dataset_id = f"source_{jae_constants.JAE_SOURCE_NAME}"
    table_id = constants.RESULTADO_VERIFICACAO_CAPTURA_TABLE_ID
    results = results[
        [
            "table_id",
            "timestamp_captura",
            "total_datalake",
            "total_jae",
            "indicador_captura_correta",
        ]
    ]

    now = datetime.now(tz=ZoneInfo(smtr_constants.TIMEZONE)).strftime("%Y%m%d%H%M%S")
    tmp_table = f"{dataset_id}.tmp_{table_id}_{now}"

    pandas_gbq.to_gbq(
        results,
        tmp_table,
        project_id=project_id,
        if_exists="replace",
    )

    start_partition = results["timestamp_captura"].min().date().isoformat()
    end_partition = results["timestamp_captura"].max().date().isoformat()

    try:
        pandas_gbq.read_gbq(
            f"""
                MERGE {project_id}.{dataset_id}.{table_id} t
                USING {tmp_table} s
                ON
                    t.data BETWEEN '{start_partition}' AND '{end_partition}'
                    AND t.table_id = s.table_id
                    AND t.timestamp_captura = DATETIME(s.timestamp_captura, 'America/Sao_Paulo')
                WHEN MATCHED THEN
                UPDATE SET
                    total_datalake = s.total_datalake,
                    total_jae = s.total_jae,
                    indicador_captura_correta = s.indicador_captura_correta,
                    datetime_ultima_atualizacao = CURRENT_DATETIME('America/Sao_Paulo')
                WHEN
                    NOT MATCHED
                    AND DATETIME_DIFF(
                        CURRENT_DATETIME('America/Sao_Paulo'),
                        DATETIME(s.timestamp_captura, 'America/Sao_Paulo'),
                        MINUTE
                    ) > 300
                THEN
                INSERT (
                    data,
                    table_id,
                    timestamp_captura,
                    total_datalake,
                    total_jae,
                    indicador_captura_correta,
                    datetime_ultima_atualizacao
                )
                VALUES (
                    DATE(DATETIME(timestamp_captura, 'America/Sao_Paulo')),
                    table_id,
                    DATETIME(timestamp_captura, 'America/Sao_Paulo'),
                    total_datalake,
                    total_jae,
                    indicador_captura_correta,
                    CURRENT_DATETIME('America/Sao_Paulo')
                )
            """,
            project_id=project_id,
        )

    finally:
        bigquery.Client(project=project_id).delete_table(
            tmp_table,
            not_found_ok=True,
        )


def rename_capture_check_flow_run() -> str:
    """
    Renomeia a execução do flow de checagem da captura da Jaé
    """
    scheduled_start_time = convert_timezone(runtime.flow_run.scheduled_start_time)
    timestamp_captura_start = runtime.flow_run.parameters["timestamp_captura_start"]
    timestamp_captura_end = runtime.flow_run.parameters["timestamp_captura_end"]
    retroactive_days = runtime.flow_run.parameters["retroactive_days"]

    run_name = f"[{scheduled_start_time.strftime('%Y-%m-%d %H-%M-%S')}] Verifica Captura Jaé: "

    if timestamp_captura_start is None and timestamp_captura_end is None:
        start = scheduled_start_time - timedelta(days=retroactive_days)
        timestamp_captura_start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        timestamp_captura_end = start.replace(hour=23, minute=59, second=59, microsecond=0)

    run_name += f"From {timestamp_captura_start} To {timestamp_captura_end}"

    return run_name
