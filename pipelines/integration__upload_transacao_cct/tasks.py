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
from pipelines.integration__upload_transacao_cct.constants import (
    LAST_UPLOAD_TIMESTAMP_KEY_NAME,
)
from pipelines.integration__upload_transacao_cct.utils import (
    TableUploadConfig,
    create_log_trigger,
    create_postgres_connection,
    create_temp_table,
    get_modified_partitions,
    get_partition_using_data_ordem,
    merge_final_data,
)


@task(cache_policy=NO_CACHE)
def get_start_datetime(
    config: TableUploadConfig,
    env: str,
    full_refresh: bool,
    data_ordem_start: Optional[str],
    data_ordem_end: Optional[str],
) -> tuple[Optional[datetime], bool]:
    if (data_ordem_start is None) != (data_ordem_end is None):
        raise ValueError("Filtro de data ordem com inicio ou fim nulo")

    start_datetime = None
    if not full_refresh and data_ordem_start is None:
        redis_client = get_redis_client()
        content = redis_client.get(f"{env}.{config.redis_key}")
        if content is None:
            full_refresh = True
        else:
            start_datetime = convert_timezone(
                timestamp=datetime.fromisoformat(content[LAST_UPLOAD_TIMESTAMP_KEY_NAME])
            )

    return start_datetime, full_refresh


@task(cache_policy=NO_CACHE)
def delete_all_files(config: TableUploadConfig, env: str):
    print(f"[{config.table_name}] Deletando todos os arquivos do GCS")
    storage = Storage(env=env, dataset_id=config.folder, bucket_names=CCT_PRIVATE_BUCKET_NAMES)
    for b in storage.bucket.list_blobs(prefix=f"{config.gcs_prefix}/"):
        b.delete()


@task(cache_policy=NO_CACHE)
def export_data_from_bq_to_gcs(  # noqa: PLR0913
    config: TableUploadConfig,
    env: str,
    timestamp: datetime,
    start_datetime: Optional[datetime],
    full_refresh: bool,
    data_ordem_start: Optional[str],
    data_ordem_end: Optional[str],
) -> list[str]:
    project_id = {"prod": "rj-smtr", "dev": "rj-smtr-dev"}[env]
    file_name = f"{timestamp.strftime('%Y-%m-%d-%H-%M-%S')}-*.csv"
    end_ts = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    pf = config.partition_field
    table_full_name = f"{project_id}.{config.view_full_name}"
    partitions = []

    if full_refresh:
        current_date_expr = (
            "current_datetime('America/Sao_Paulo')"
            if pf == "data"
            else "current_date('America/Sao_Paulo')"
        )
        export_filter = (
            f"datetime_ultima_atualizacao <= DATETIME('{end_ts}') and {pf} <= {current_date_expr}"
        )
        partitions = pandas_gbq.read_gbq(
            f"""
                SELECT DISTINCT CONCAT("'", {pf}, "'") AS particao
                FROM {table_full_name}
                WHERE {export_filter}
            """,
            project_id=project_id,
        )["particao"].tolist()
    elif data_ordem_start is None:
        start_ts = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
        partitions = get_modified_partitions(
            config=config, project_id=project_id, start_ts=start_ts, end_ts=end_ts
        )
        export_filter = f"""
            {pf} IN ({", ".join(partitions)})
            AND datetime_ultima_atualizacao
                BETWEEN DATETIME("{start_ts}")
                AND DATETIME("{end_ts}")
            """
    else:
        partitions = get_partition_using_data_ordem(
            config=config,
            project_id=project_id,
            data_ordem_start=data_ordem_start,
            data_ordem_end=data_ordem_end,
        )
        if config.use_data_ordem_transacao_join:
            export_filter = f"""
                {pf} IN ({", ".join(partitions)})
                AND data_ordem
                    BETWEEN "{data_ordem_start}"
                    AND "{data_ordem_end}"
                """
        else:
            export_filter = f"{pf} IN ({', '.join(partitions)})"

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
    export_uri = f"gs://{CCT_PRIVATE_BUCKET_NAMES[env]}/{config.gcs_prefix}/{file_name}"
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
    config: TableUploadConfig,
    env: str,
    full_refresh: bool,
    export_bigquery_dates: list[str],
):
    blobs = list(
        Storage(
            env=env, dataset_id=config.folder, bucket_names=CCT_PRIVATE_BUCKET_NAMES
        ).bucket.list_blobs(prefix=f"{config.gcs_prefix}/")
    )

    with create_postgres_connection(env=env)() as conn:
        conn.autocommit = False

        with conn.cursor() as cur:
            print(f"[{config.table_name}] Deletando índices e trigger da tabela final")
            cur.execute(f"DROP INDEX IF EXISTS public.{config.id_ordem_index_name}")
            cur.execute(f"DROP INDEX IF EXISTS public.{config.data_index_name}")
            cur.execute(
                f"DROP TRIGGER IF EXISTS {config.log_trigger_name} ON public.{config.table_name}"
            )

            if full_refresh:
                print(f"[{config.table_name}] Truncando tabela final")
                cur.execute(f"TRUNCATE TABLE public.{config.table_name}")

            for blob in blobs:
                create_temp_table(config=config, cur=cur, blob=blob, full_refresh=full_refresh)
                merge_final_data(
                    config=config,
                    cur=cur,
                    blob=blob,
                    full_refresh=full_refresh,
                    export_bigquery_dates=export_bigquery_dates,
                )

            cur.execute(
                f"""
                CREATE INDEX {config.id_ordem_index_name}
                ON public.{config.table_name} (id_ordem_pagamento_consorcio_operador_dia)
                """
            )
            print(f"[{config.table_name}] Índice ordem pagamento recriado")

            cur.execute(
                f"""
                CREATE INDEX {config.data_index_name}
                ON public.{config.table_name} ({config.partition_field})
                """
            )
            print(f"[{config.table_name}] Índice {config.partition_field} recriado")

            create_log_trigger(config=config, cur=cur)
            conn.commit()
            cur.execute(f"ANALYZE public.{config.table_name}")

        conn.commit()

    for blob in blobs:
        blob.delete()
    print(f"[{config.table_name}] Arquivos do GCS deletados")


@task(cache_policy=NO_CACHE)
def get_postgres_modified_dates(
    config: TableUploadConfig,
    env: str,
    start_datetime: Optional[datetime],
    full_refresh: bool,
) -> list[str]:
    if full_refresh:
        return []
    start_ts_str = start_datetime.astimezone(tz=timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S")
    with create_postgres_connection(env=env)() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                    SELECT {config.partition_field}
                    FROM {config.log_table_name}
                    WHERE datetime_alteracao
                    BETWEEN '{start_ts_str}'
                    AND NOW()
                """
            )
            rows = cur.fetchall()

    return [f"'{r[0]!s}'" for r in rows]


@task(cache_policy=NO_CACHE)
def save_upload_timestamp_redis(
    config: TableUploadConfig,
    env: str,
    timestamp: datetime,
    param_test_dates: Optional[list[str]],
    data_ordem_start: Optional[str],
):
    if data_ordem_start is not None or param_test_dates is not None:
        return

    value = timestamp.isoformat()
    redis_key = f"{env}.{config.redis_key}"
    redis_client = get_redis_client()
    content = redis_client.get(redis_key)

    print(f"[{config.table_name}] Salvando timestamp {value} na chave: {redis_key}")
    if not content:
        redis_client.set(redis_key, {LAST_UPLOAD_TIMESTAMP_KEY_NAME: value})
    elif (
        convert_timezone(datetime.fromisoformat(content[LAST_UPLOAD_TIMESTAMP_KEY_NAME]))
        < timestamp
    ):
        content[LAST_UPLOAD_TIMESTAMP_KEY_NAME] = value
        redis_client.set(redis_key, content)


@task(cache_policy=NO_CACHE)
def upload_postgres_modified_data_to_bq(
    config: TableUploadConfig,
    env: str,
    timestamp: datetime,
    dates: list[str],
    full_refresh: bool,
):
    pf = config.partition_field
    where = "1=1" if full_refresh else f"{pf} IN ({', '.join(dates)})"
    sql = f"""
        SELECT
            *,
            NOW() AS datetime_extracao_teste
        FROM
            public.{config.table_name}
        WHERE
            {where}
    """

    table_name = config.teste_sincronizacao_table_name
    filepath = Path(
        Path(get_data_folder_path())
        / "upload"
        / table_name
        / f"{timestamp.strftime('%Y-%m-%d-%H-%M-%S')}.csv"
    )
    filepath.parent.mkdir(parents=True, exist_ok=True)

    with create_postgres_connection(env=env)() as conn:
        with conn.cursor() as cur, filepath.open("w", encoding="utf-8") as f:
            print(f"[{config.table_name}] Exportando dados para arquivo {filepath}")
            cur.copy_expert(f"COPY ({sql}) TO STDOUT WITH CSV HEADER", f)

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
        shutil.copyfileobj(f_in, f_out)

    with filepath_zip.open("rb") as f:
        bq.load_table_from_file(
            f,
            f"{project_id}.source_cct.{table_name}",
            job_config=bq_config,
        ).result()


@task(cache_policy=NO_CACHE)
def create_sincronizacao_materialization_context(
    config: TableUploadConfig, env: str, timestamp: datetime
) -> list[DBTSelectorMaterializationContext]:
    return [
        DBTSelectorMaterializationContext(
            env=env,
            selector=config.teste_sincronizacao_selector,
            timestamp=timestamp,
            datetime_start=None,
            datetime_end=None,
            additional_vars=None,
            test_scheduled_time=None,
            force_test_run=False,
        )
    ]
