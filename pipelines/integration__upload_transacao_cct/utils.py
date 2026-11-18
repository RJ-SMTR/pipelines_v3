# -*- coding: utf-8 -*-
"""Funções para exportação das transações do BQ para o Postgres"""

from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import Callable

import pandas_gbq
import psycopg2
from google.cloud.storage import Blob
from psycopg2._psycopg import connection, cursor

from pipelines.capture__cct_pagamento import constants as cct_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector
from pipelines.common.utils.secret import get_env_secret


@dataclass
class TableUploadConfig:
    table_name: str
    tmp_table_name: str
    tmp_table_ddl: str
    tmp_index_name: str
    tmp_index_columns: str
    unique_key_fields: list[str]
    id_transacao_index_name: str
    id_ordem_index_name: str
    data_index_name: str
    log_table_name: str
    log_trigger_name: str
    log_function_name: str
    redis_key: str
    folder: str
    gcs_prefix: str
    view_full_name: str
    bq_source_table: str
    partition_field: str
    use_data_ordem_transacao_join: bool
    teste_sincronizacao_table_name: str
    teste_sincronizacao_selector: DBTSelector


def create_postgres_connection(env: str) -> Callable[[], connection]:
    credentials = (
        get_env_secret(cct_constants.CCT_SECRET_PATH)
        if env == "prod"
        else get_env_secret(cct_constants.CCT_HMG_SECRET_PATH)
    )
    return partial(
        psycopg2.connect,
        host=credentials["host"],
        user=credentials["user"],
        password=credentials["password"],
        database=credentials["dbname"],
    )


def get_modified_partitions(
    config: TableUploadConfig,
    project_id: str,
    start_ts: str,
    end_ts: str,
) -> list[str]:
    sql = f"""
    SELECT
        CONCAT("'", PARSE_DATE("%Y%m%d", partition_id), "'") AS particao
    FROM
        `rj-smtr.bilhetagem.INFORMATION_SCHEMA.PARTITIONS`
    WHERE
        table_name = "{config.bq_source_table}"
        AND partition_id != "__NULL__"
        AND DATETIME(last_modified_time, "America/Sao_Paulo")
            BETWEEN DATETIME("{start_ts}")
            AND DATETIME("{end_ts}")
    """

    print(f"Executando query:\n{sql}")

    modified_partitions = pandas_gbq.read_gbq(sql, project_id=project_id)["particao"].to_list()

    if not modified_partitions:
        return []

    pf = config.partition_field
    sql = f"""
        SELECT
            DISTINCT CONCAT("'", {pf}, "'") AS particao
        FROM
            {project_id}.{config.view_full_name}
        WHERE
            {pf} IN ({", ".join(modified_partitions)})
            AND datetime_ultima_atualizacao
                BETWEEN DATETIME("{start_ts}")
                AND DATETIME("{end_ts}")
    """

    print(f"Executando query:\n{sql}")
    return pandas_gbq.read_gbq(sql, project_id=project_id)["particao"].to_list()


def get_partition_using_data_ordem(
    config: TableUploadConfig,
    project_id: str,
    data_ordem_start: str,
    data_ordem_end: str,
) -> list[str]:
    try:
        datetime.strptime(data_ordem_start, "%Y-%m-%d")  # noqa: DTZ007
        datetime.strptime(data_ordem_end, "%Y-%m-%d")  # noqa: DTZ007
    except ValueError as e:
        raise ValueError(f"Formato de data_ordem inválido: {e}") from e

    if config.use_data_ordem_transacao_join:
        sql = f"""
        SELECT
            DISTINCT CONCAT("'", data_transacao, "'") AS particao
        FROM
            `rj-smtr.bilhetagem_interno.data_ordem_transacao`
        WHERE
            data_ordem BETWEEN "{data_ordem_start}" AND "{data_ordem_end}"
        """
        print(f"Executando query:\n{sql}")
        partitions = pandas_gbq.read_gbq(sql, project_id=project_id)["particao"].to_list()

        pf = config.partition_field
        sql = f"""
            SELECT
                DISTINCT CONCAT("'", {pf}, "'") AS particao
            FROM
                {project_id}.{config.view_full_name}
            WHERE
                {pf} IN ({", ".join(partitions)})
                AND data_ordem BETWEEN "{data_ordem_start}" AND "{data_ordem_end}"
        """
    else:
        pf = config.partition_field
        sql = f"""
        SELECT
            DISTINCT CONCAT("'", {pf}, "'") AS particao
        FROM
            {project_id}.{config.view_full_name}
        WHERE
            {pf} BETWEEN "{data_ordem_start}" AND "{data_ordem_end}"
        """

    print(f"Executando query:\n{sql}")
    return pandas_gbq.read_gbq(sql, project_id=project_id)["particao"].to_list()


def create_temp_table(config: TableUploadConfig, cur: cursor, blob: Blob, full_refresh: bool):
    if not full_refresh:
        sql = f"""
            CREATE TABLE IF NOT EXISTS public.{config.tmp_table_name}
            (
                {config.tmp_table_ddl}
            )
        """
        print("Criando tabela temporária")
        cur.execute(sql)
        print("Tabela temporária criada")

        print("Deletando índice da tabela temporária")
        cur.execute(f"DROP INDEX IF EXISTS public.{config.tmp_index_name}")

        print(f"Copiando arquivo {blob.name} para a tabela temporária")
        sql = f"COPY public.{config.tmp_table_name} FROM STDIN WITH CSV HEADER"
        with blob.open("r") as f:
            cur.copy_expert(sql, f)
        print("Cópia completa")

        sql = f"""
            CREATE INDEX {config.tmp_index_name}
            ON public.{config.tmp_table_name} ({config.tmp_index_columns})
        """
        print("Criando índice tabela temporária")
        cur.execute(sql)


def merge_final_data(
    config: TableUploadConfig,
    cur: cursor,
    blob: Blob,
    full_refresh: bool,
    export_bigquery_dates: list[str],
):
    if not full_refresh:
        join_condition = " AND ".join(
            [f"t.{f} = s.{f}" for f in config.unique_key_fields]
        )
        sql = f"""
            DELETE FROM public.{config.table_name} t
            USING public.{config.tmp_table_name} s
            WHERE {join_condition}
        """
        print("Deletando registros da tabela final")
        cur.execute(sql)
        print(f"{cur.rowcount} linhas deletadas")

    print("Deletando tabela temporária")
    cur.execute(f"DROP TABLE IF EXISTS public.{config.tmp_table_name}")
    print("Tabela temporária deletada")

    print("Deletando índice id_transacao da tabela final")
    cur.execute(f"DROP INDEX IF EXISTS public.{config.id_transacao_index_name}")

    print(f"Copiando arquivo {blob.name} para a tabela final")
    with blob.open("r") as f:
        cur.copy_expert(f"COPY public.{config.table_name} FROM STDIN WITH CSV HEADER", f)
    print("Cópia completa")

    sql = f"""
        CREATE INDEX {config.id_transacao_index_name}
        ON public.{config.table_name} ({config.tmp_index_columns})
    """
    print("Recriando índice id_transacao da tabela final")
    cur.execute(sql)

    pf = config.partition_field
    date_values = [f"(DATE({d}))" for d in export_bigquery_dates]
    sql = f"""
        MERGE INTO public.{config.log_table_name} AS t
        USING (
            VALUES
            {",".join(date_values)}
        ) AS s({pf})
        ON t.{pf} = s.{pf}
        WHEN MATCHED THEN
            UPDATE SET datetime_alteracao = now()
        WHEN NOT MATCHED THEN
            INSERT ({pf}, datetime_alteracao)
            VALUES (s.{pf}, now());
    """
    print("Atualizando tabela de log de modificações")
    cur.execute(sql)


def create_log_trigger(config: TableUploadConfig, cur: cursor):
    print("Recriando Trigger na tabela final")

    pf = config.partition_field
    log = config.log_table_name

    sql = f"""
        CREATE OR REPLACE FUNCTION public.{config.log_function_name}()
        RETURNS TRIGGER
        SECURITY DEFINER
        AS $$
        DECLARE
            v_now timestamptz := now();
        BEGIN
            IF TG_OP = 'DELETE' THEN
                MERGE INTO public.{log} AS t
                USING (SELECT OLD.{pf} AS {pf}, v_now AS datetime_alteracao) AS s
                ON (t.{pf} = s.{pf})
                WHEN MATCHED THEN
                    UPDATE SET datetime_alteracao = s.datetime_alteracao
                WHEN NOT MATCHED THEN
                    INSERT ({pf}, datetime_alteracao) VALUES (s.{pf}, s.datetime_alteracao);

            ELSIF TG_OP = 'INSERT' THEN
                MERGE INTO public.{log} AS t
                USING (SELECT NEW.{pf} AS {pf}, v_now AS datetime_alteracao) AS s
                ON (t.{pf} = s.{pf})
                WHEN MATCHED THEN
                    UPDATE SET datetime_alteracao = s.datetime_alteracao
                WHEN NOT MATCHED THEN
                    INSERT ({pf}, datetime_alteracao) VALUES (s.{pf}, s.datetime_alteracao);

            ELSIF TG_OP = 'UPDATE' THEN
                IF OLD.{pf} IS DISTINCT FROM NEW.{pf} THEN
                    MERGE INTO public.{log} AS t
                    USING (SELECT OLD.{pf} AS {pf}, v_now AS datetime_alteracao) AS s
                    ON (t.{pf} = s.{pf})
                    WHEN MATCHED THEN
                        UPDATE SET datetime_alteracao = s.datetime_alteracao
                    WHEN NOT MATCHED THEN
                        INSERT ({pf}, datetime_alteracao) VALUES (s.{pf}, s.datetime_alteracao);

                    MERGE INTO public.{log} AS t
                    USING (SELECT NEW.{pf} AS {pf}, v_now AS datetime_alteracao) AS s
                    ON (t.{pf} = s.{pf})
                    WHEN MATCHED THEN
                        UPDATE SET datetime_alteracao = s.datetime_alteracao
                    WHEN NOT MATCHED THEN
                        INSERT ({pf}, datetime_alteracao) VALUES (s.{pf}, s.datetime_alteracao);
                ELSE
                    MERGE INTO public.{log} AS t
                    USING (SELECT NEW.{pf} AS {pf}, v_now AS datetime_alteracao) AS s
                    ON (t.{pf} = s.{pf})
                    WHEN MATCHED THEN
                        UPDATE SET datetime_alteracao = s.datetime_alteracao
                    WHEN NOT MATCHED THEN
                        INSERT ({pf}, datetime_alteracao) VALUES (s.{pf}, s.datetime_alteracao);
                END IF;
            END IF;

            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """

    cur.execute(sql)

    sql = f"""
        CREATE TRIGGER {config.log_trigger_name}
        AFTER INSERT OR UPDATE OR DELETE
        ON public.{config.table_name}
        FOR EACH ROW
        EXECUTE FUNCTION public.{config.log_function_name}();
    """

    cur.execute(sql)

    print("Trigger criado")
