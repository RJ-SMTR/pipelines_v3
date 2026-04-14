# -*- coding: utf-8 -*-
"""Tasks para exportação das transações do BQ para o Postgres"""

import gzip
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas_gbq
from google.cloud import bigquery
from prefect import task
from prefect.cache_policies import NO_CACHE
from pytz import timezone

from pipelines.capture__cct_pagamento.constants import CCT_PRIVATE_BUCKET_NAMES
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import (
    DBTSelectorMaterializationContext,
)
from pipelines.common.utils.fs import get_data_folder_path
from pipelines.common.utils.gcp.storage import Storage
from pipelines.common.utils.redis import get_redis_client
from pipelines.common.utils.utils import convert_timezone
from pipelines.integration__upload_transacao_cct import constants
from pipelines.integration__upload_transacao_cct.utils import (
    create_log_trigger,
    create_postgres_connection,
    create_temp_table,
    get_modified_partitions,
    get_partition_using_data_ordem,
    merge_final_data,
)


@task(cache_policy=NO_CACHE)
def get_start_datetime(
    env: str,
    full_refresh: bool,
    data_ordem_start: Optional[str],
    data_ordem_end: Optional[str],
) -> tuple[Optional[datetime], bool]:
    """
    Obtém o timestamp da última execução a partir do Redis, ajustando o modo de execução.

    Args:
        env (str): dev ou prod.
        full_refresh (bool): Indica se a execução é um carregamento completo.
        data_ordem_start (Optional[str]): Data inicial do filtro de ordem.
        data_ordem_end (Optional[str]): Data final do filtro de ordem.

    Returns:
        tuple[Optional[datetime], bool]: Timestamp inicial e flag indicando se é full refresh.
    """

    if (data_ordem_start is None) != (data_ordem_end is None):
        raise ValueError("Filtro de data ordem com inicio ou fim nulo")

    start_datetime = None
    if not full_refresh and data_ordem_start is None:
        redis_client = get_redis_client()
        content = redis_client.get(f"{env}.{constants.REDIS_KEY}")
        if content is None:
            full_refresh = True
        else:
            start_datetime = convert_timezone(
                timestamp=datetime.fromisoformat(content[constants.LAST_UPLOAD_TIMESTAMP_KEY_NAME])
            )

    return start_datetime, full_refresh


@task(cache_policy=NO_CACHE)
def delete_all_files(env: str):
    """
    Remove todos os arquivos exportados do GCS.

    Args:
        env (str): dev ou prod.
    """

    print("Deletando todos os arquivos do GCS")
    storage = Storage(
        env=env,
        dataset_id=constants.TRANSACAO_CCT_FOLDER,
        bucket_names=CCT_PRIVATE_BUCKET_NAMES,
    )

    for b in storage.bucket.list_blobs(prefix=f"{constants.EXPORT_GCS_PREFIX}/"):
        b.delete()


@task(cache_policy=NO_CACHE)
def export_data_from_bq_to_gcs(  # noqa: PLR0913
    env: str,
    timestamp: datetime,
    start_datetime: datetime,
    full_refresh: bool,
    data_ordem_start: Optional[str],
    data_ordem_end: Optional[str],
) -> list[str]:
    """
    Exporta dados do BigQuery para o GCS aplicando filtros conforme o tipo de execução.

    Args:
        env (str): dev ou prod.
        timestamp (datetime): Timestamp de referência da execução.
        start_datetime (datetime): Timestamp inicial para filtragem incremental.
        full_refresh (bool): Indica se o carregamento é completo.
        data_ordem_start (Optional[str]): Data inicial do filtro de ordem.
        data_ordem_end (Optional[str]): Data final do filtro de ordem.

    Returns:
        list[str]: Lista com as datas exportadas (vazia em caso de full refresh).
    """
    project_id = {"prod": "rj-smtr", "dev": "rj-smtr-dev"}[env]

    file_name = f"{timestamp.strftime('%Y-%m-%d-%H-%M-%S')}-*.csv"

    export_filter = "true"

    end_ts = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    partitions = []

    table_full_name = f"{project_id}.{constants.TRANSACAO_CCT_VIEW_FULL_NAME}"
    if full_refresh:
        export_filter = f"datetime_ultima_atualizacao <= DATETIME('{end_ts}')"
        partitions = pandas_gbq.read_gbq(
            f"""
                SELECT DISTINCT CONCAT("'", data, "'") AS particao
                FROM {table_full_name}
                WHERE {export_filter}
            """,
            project_id=project_id,
        )["particao"].tolist()
    elif data_ordem_start is None:
        start_ts = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
        partitions = get_modified_partitions(
            project_id=project_id,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        export_filter = f"""
            data IN ({", ".join(partitions)})
            AND datetime_ultima_atualizacao
                BETWEEN DATETIME("{start_ts}")
                AND DATETIME("{end_ts}")
            """
    else:
        partitions = get_partition_using_data_ordem(
            project_id=project_id,
            data_ordem_start=data_ordem_start,
            data_ordem_end=data_ordem_end,
        )
        export_filter = f"""
            data IN ({", ".join(partitions)})
            AND data_ordem
                BETWEEN "{data_ordem_start}"
                AND "{data_ordem_end}"
            """

    transacao_select = f"""
        SELECT
            {{cols}}
        FROM
            {table_full_name}
        WHERE
            {export_filter}
        LIMIT {{count}}
    """

    sql = transacao_select.format(cols="count(1) as ct", count=1)
    print(f"Executando query:\n{sql}")
    limit = pandas_gbq.read_gbq(sql, project_id=project_id)["ct"].max()
    export_uri = f"gs://{CCT_PRIVATE_BUCKET_NAMES[env]}/{constants.EXPORT_GCS_PREFIX}/{file_name}"
    cols = "*, current_datetime('America/Sao_Paulo') as datetime_export"
    sql = f"""
        EXPORT DATA
        OPTIONS(uri='{export_uri}',
        format='CSV',
        OVERWRITE=TRUE,
        header=TRUE) AS

        {transacao_select.format(cols=cols, count=limit)}
    """
    print(f"Executando query:\n{sql}")
    pandas_gbq.read_gbq(sql, project_id=project_id)

    return partitions


@task(cache_policy=NO_CACHE)
def upload_files_postgres(
    env: str,
    full_refresh: bool,
    export_bigquery_dates: list[str],
):
    """
    Carrega os arquivos CSV das transações no GCS para o PostgreSQL

    Args:
        env (str): dev ou prod.
        full_refresh (bool): Indica se o carregamento é completo (truncate + reload) ou incremental.
        export_bigquery_dates (list[str]): Lista de datas das transações exportadas do BigQuery
    """
    table_name = constants.TRANSACAO_POSTGRES_TABLE_NAME

    blobs = list(
        Storage(
            env=env,
            dataset_id="transacao_cct",
            bucket_names=CCT_PRIVATE_BUCKET_NAMES,
        ).bucket.list_blobs(prefix=f"{constants.EXPORT_GCS_PREFIX}/")
    )

    with create_postgres_connection(env=env)() as conn:
        conn.autocommit = False

        with conn.cursor() as cur:
            sql = f"DROP INDEX IF EXISTS public.{constants.FINAL_TABLE_ID_ORDEM_INDEX_NAME}"
            print("Deletando índice ordem pagamento da tabela final")
            cur.execute(sql)
            print("Índice deletado")

            sql = f"DROP INDEX IF EXISTS public.{constants.FINAL_TABLE_DATA_INDEX_NAME}"
            print("Deletando índice data da tabela final")
            cur.execute(sql)
            print("Índice deletado")

            sql = f"""
                DROP TRIGGER IF EXISTS {constants.LOG_TRIGGER_NAME}
                ON public.{table_name}
            """
            print("Deletando trigger de log da tabela final")
            cur.execute(sql)
            print("Trigger deletado")

            if full_refresh:
                print("Truncando tabela final")
                cur.execute(f"TRUNCATE TABLE public.{table_name}")

            for blob in blobs:
                create_temp_table(cur=cur, blob=blob, full_refresh=full_refresh)

                merge_final_data(
                    cur=cur,
                    blob=blob,
                    full_refresh=full_refresh,
                    export_bigquery_dates=export_bigquery_dates,
                )

            sql = f"""
                CREATE INDEX
                {constants.FINAL_TABLE_ID_ORDEM_INDEX_NAME}
                ON public.{table_name} (id_ordem_pagamento_consorcio_operador_dia)
            """
            print("Recriando índice ordem pagamento da tabela final")
            cur.execute(sql)
            print("Índice recriado")

            sql = f"""
                CREATE INDEX
                {constants.FINAL_TABLE_DATA_INDEX_NAME}
                ON public.{table_name} (data)
            """
            print("Recriando índice data da tabela final")
            cur.execute(sql)
            print("Índice recriado")

            create_log_trigger(cur=cur)

            conn.commit()

            sql = f"""
                ANALYZE public.{table_name}
            """
            cur.execute(sql)

        conn.commit()

    for blob in blobs:
        print("Deletando arquivo do GCS")
        blob.delete()
        print("Arquivo do GCS deletado")


@task(cache_policy=NO_CACHE)
def get_postgres_modified_dates(
    env: str, start_datetime: datetime, full_refresh: bool
) -> list[str]:
    """
    Retorna as datas modificadas no Postgres desde um timestamp inicial.

    Args:
        env (str): dev ou prod.
        start_datetime (datetime): Data e hora inicial para filtrar modificações.
        full_refresh (bool): Indica se o carregamento é completo ou incremental.

    Returns:
        list[str]: Lista de datas modificadas formatadas como strings SQL.
    """
    if full_refresh:
        return []
    start_ts_str = start_datetime.astimezone(tz=timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S")
    with create_postgres_connection(env=env)() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                    SELECT data
                    FROM {constants.LOG_TABLE_NAME}
                    WHERE datetime_alteracao
                    BETWEEN '{start_ts_str}'
                    AND NOW()
                """
            )

            rows = cur.fetchall()

    return [f"'{r[0]!s}'" for r in rows]


@task(cache_policy=NO_CACHE)
def save_upload_timestamp_redis(
    env: str,
    timestamp: datetime,
    param_test_dates: Optional[list[str]],
    data_ordem_start: Optional[str],
):
    """
    Atualiza o timestamp da execução no Redis.

    Args:
        env (str): dev ou prod.
        timestamp (datetime): Timestamp de referência da execução.
        data_ordem_start (Optional[str]): Data inicial do parâmetro de filtro de ordem.
    """

    if data_ordem_start is not None or param_test_dates is not None:
        return

    value = timestamp.isoformat()
    redis_key = f"{env}.{constants.REDIS_KEY}"
    redis_client = get_redis_client()
    content = redis_client.get(redis_key)

    print(f"Salvando timestamp {value} na chave: {redis_key}")
    if not content:
        content = {constants.LAST_UPLOAD_TIMESTAMP_KEY_NAME: value}
        print("Timestamp salva no Redis")
        redis_client.set(redis_key, content)
    elif (
        convert_timezone(datetime.fromisoformat(content[constants.LAST_UPLOAD_TIMESTAMP_KEY_NAME]))
        < timestamp
    ):
        content[constants.LAST_UPLOAD_TIMESTAMP_KEY_NAME] = value
        print("Timestamp salva no Redis")
        redis_client.set(redis_key, content)


@task(cache_policy=NO_CACHE)
def upload_postgres_modified_data_to_bq(
    env: str,
    timestamp: datetime,
    dates: list[str],
    full_refresh: bool,
):
    """
    Exporta dados modificados do Postgres e faz upload para o BigQuery.

    Args:
        env (str): dev ou prod.
        timestamp (datetime): Data e hora de referência para nomear o arquivo exportado.
        dates (list[str]): Lista de datas a serem filtradas na exportação.
        full_refresh (bool): Indica se o carregamento é completo ou incremental.
    """
    where = "1=1" if full_refresh else f"data IN ({', '.join(dates)})"
    sql = f"""
        SELECT
            *,
            NOW() AS datetime_extracao_teste
        FROM
            public.{constants.TRANSACAO_POSTGRES_TABLE_NAME}
        WHERE
            {where}
    """

    table_name = constants.TESTE_SINCRONIZACAO_TABLE_NAME

    filepath = Path(
        Path(get_data_folder_path())
        / "upload"
        / table_name
        / f"{timestamp.strftime('%Y-%m-%d-%H-%M-%S')}.csv"
    )

    filepath.parent.mkdir(parents=True, exist_ok=True)

    with create_postgres_connection(env=env)() as conn:
        with conn.cursor() as cur, filepath.open("w", encoding="utf-8") as f:
            print(f"exportando dados para arquivo {filepath}")
            cur.copy_expert(f"COPY ({sql}) TO STDOUT WITH CSV HEADER", f)
            print("arquivo exportado")

    project_id = smtr_constants.PROJECT_NAME[env]

    bq = bigquery.Client()
    bq_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
    )
    filepath_zip = Path(str(filepath) + ".gz")

    with filepath.open("rb") as f_in, gzip.open(filepath_zip, "wb") as f_out:
        print(f"Criando arquivo compactado: {filepath_zip}")
        shutil.copyfileobj(f_in, f_out)

    with filepath_zip.open("rb") as f:
        bq.load_table_from_file(
            f,
            f"{project_id}.source_cct.{table_name}",
            job_config=bq_config,
        ).result()


@task(cache_policy=NO_CACHE)
def create_sincronizacao_materialization_context(
    env: str,
) -> list[DBTSelectorMaterializationContext]:
    return [
        DBTSelectorMaterializationContext(
            env=env,
            selector=constants.TESTE_SINCRONIZACAO_SELECTOR,
        )
    ]
