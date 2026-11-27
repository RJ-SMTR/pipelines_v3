# -*- coding: utf-8 -*-
"""Funções para exportação de transacao_valor_ordem do BQ para o Postgres"""

from datetime import datetime
from functools import partial
from typing import Callable

import pandas_gbq
import psycopg2
from google.cloud.storage import Blob
from psycopg2._psycopg import connection, cursor

from pipelines.capture__cct_pagamento import constants as cct_constants
from pipelines.common.utils.secret import get_env_secret
from pipelines.integration__upload_transacao_valor_ordem import constants


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
        table_name = "transacao_valor_ordem"
        AND partition_id != "__NULL__"
        AND DATETIME(last_modified_time, "America/Sao_Paulo")
            BETWEEN DATETIME("{start_ts}")
            AND DATETIME("{end_ts}")
    """

    print(f"Executando query:\n{sql}")

    modified_partitions = pandas_gbq.read_gbq(
        sql,
        project_id=project_id,
    )["particao"].to_list()

    if not modified_partitions:
        return []

    sql = f"""
        SELECT
            DISTINCT CONCAT("'", data_ordem, "'") AS particao
        FROM
            {project_id}.{constants.TRANSACAO_VALOR_ORDEM_VIEW_FULL_NAME}
        WHERE
        data_ordem IN ({", ".join(modified_partitions)})
        AND datetime_ultima_atualizacao
            BETWEEN DATETIME("{start_ts}")
            AND DATETIME("{end_ts}")
    """

    print(f"Executando query:\n{sql}")
    return pandas_gbq.read_gbq(
        sql,
        project_id=project_id,
    )["particao"].to_list()


def get_partition_using_data_ordem(
    project_id: str,
    data_ordem_start: str,
    data_ordem_end: str,
) -> list[str]:
    try:
        datetime.strptime(data_ordem_start, "%Y-%m-%d")  # noqa: DTZ007
        datetime.strptime(data_ordem_end, "%Y-%m-%d")  # noqa: DTZ007
    except ValueError as e:
        raise ValueError(f"Formato de data_ordem inválido: {e}") from e

    sql = f"""
    SELECT
        DISTINCT CONCAT("'", data_ordem, "'") AS particao
    FROM
        {project_id}.{constants.TRANSACAO_VALOR_ORDEM_VIEW_FULL_NAME}
    WHERE
        data_ordem BETWEEN "{data_ordem_start}" AND "{data_ordem_end}"
    """

    print(f"Executando query:\n{sql}")

    return pandas_gbq.read_gbq(
        sql,
        project_id=project_id,
    )["particao"].to_list()


def create_temp_table(cur: cursor, blob: Blob, full_refresh: bool):
    if not full_refresh:
        tmp_table_name = constants.TRANSACAO_VALOR_ORDEM_POSTGRES_TMP_TABLE_NAME
        sql = f"""
            CREATE TABLE IF NOT EXISTS public.{tmp_table_name}
            (
                data_ordem date,
                data_transacao date,
                id_transacao character varying(60),
                id_operadora character varying(60),
                valor_transacao_rateio numeric(13,5),
                id_ordem_pagamento integer,
                id_ordem_pagamento_consorcio_dia integer,
                id_ordem_pagamento_consorcio_operador_dia integer,
                datetime_ultima_atualizacao timestamp,
                datetime_export timestamp
            )
        """
        print("Criando tabela temporária")
        cur.execute(sql)
        print("Tabela temporária criada")

        sql = f"DROP INDEX IF EXISTS public.{constants.TMP_TABLE_INDEX_NAME}"
        print("Deletando índice da tabela temporária")
        cur.execute(sql)

        print(f"Copiando arquivo {blob.name} para a tabela temporária")
        sql = f"""
            COPY public.{tmp_table_name}
            FROM STDIN WITH CSV HEADER
        """

        with blob.open("r") as f:
            cur.copy_expert(sql, f)
        print("Cópia completa")

        sql = f"""
            CREATE INDEX {constants.TMP_TABLE_INDEX_NAME}
            ON public.{tmp_table_name} (id_transacao, data_ordem)
        """
        print("Criando índice tabela temporária")
        cur.execute(sql)


def merge_final_data(
    cur: cursor,
    blob: Blob,
    full_refresh: bool,
    export_bigquery_dates: list[str],
):
    table_name = constants.TRANSACAO_VALOR_ORDEM_POSTGRES_TABLE_NAME
    tmp_table_name = constants.TRANSACAO_VALOR_ORDEM_POSTGRES_TMP_TABLE_NAME

    if not full_refresh:
        sql = f"""
            DELETE FROM public.{table_name} t
            USING public.{tmp_table_name} s
            WHERE t.id_transacao = s.id_transacao
                AND t.data_ordem = s.data_ordem
        """
        print("Deletando registros da tabela final")
        cur.execute(sql)
        print(f"{cur.rowcount} linhas deletadas")

    print("Deletando tabela temporária")
    cur.execute(f"DROP TABLE IF EXISTS public.{tmp_table_name}")
    print("Tabela temporária deletada")

    sql = f"DROP INDEX IF EXISTS public.{constants.FINAL_TABLE_ID_TRANSACAO_INDEX_NAME}"
    print("Deletando índice id_transacao da tabela final")
    cur.execute(sql)

    print(f"Copiando arquivo {blob.name} para a tabela final")
    sql = f"""
        COPY public.{table_name}
        FROM STDIN WITH CSV HEADER
    """
    with blob.open("r") as f:
        cur.copy_expert(sql, f)
    print("Cópia completa")

    sql = f"""
        CREATE INDEX {constants.FINAL_TABLE_ID_TRANSACAO_INDEX_NAME}
        ON public.{table_name} (id_transacao, data_ordem)
    """
    print("Recriando índice id_transacao da tabela final")
    cur.execute(sql)

    date_values = [f"(DATE({d}))" for d in export_bigquery_dates]
    sql = f"""
        MERGE INTO public.{constants.LOG_TABLE_NAME} AS t
        USING (
            VALUES
            {",".join(date_values)}
        ) AS s(data_ordem)
        ON t.data_ordem = s.data_ordem
        WHEN MATCHED THEN
            UPDATE SET datetime_alteracao = now()
        WHEN NOT MATCHED THEN
            INSERT (data_ordem, datetime_alteracao)
            VALUES (s.data_ordem, now());
    """
    print("Atualizando tabela de log de modificações")
    cur.execute(sql)


def create_log_trigger(cur: cursor):
    print("Recriando Trigger na tabela final")

    log_table_name = constants.LOG_TABLE_NAME
    sql = f"""
        CREATE OR REPLACE FUNCTION public.{constants.LOG_FUNCTION_NAME}()
        RETURNS TRIGGER
        SECURITY DEFINER
        AS $$
        DECLARE
            v_now timestamptz := now();
        BEGIN
            IF TG_OP = 'DELETE' THEN
                MERGE INTO public.{log_table_name} AS t
                USING (SELECT OLD.data_ordem AS data_ordem, v_now AS datetime_alteracao) AS s
                ON (t.data_ordem = s.data_ordem)
                WHEN MATCHED THEN
                    UPDATE SET datetime_alteracao = s.datetime_alteracao
                WHEN NOT MATCHED THEN
                    INSERT (data_ordem, datetime_alteracao)
                    VALUES (s.data_ordem, s.datetime_alteracao);

            ELSIF TG_OP = 'INSERT' THEN
                MERGE INTO public.{log_table_name} AS t
                USING (SELECT NEW.data_ordem AS data_ordem, v_now AS datetime_alteracao) AS s
                ON (t.data_ordem = s.data_ordem)
                WHEN MATCHED THEN
                    UPDATE SET datetime_alteracao = s.datetime_alteracao
                WHEN NOT MATCHED THEN
                    INSERT (data_ordem, datetime_alteracao)
                    VALUES (s.data_ordem, s.datetime_alteracao);

            ELSIF TG_OP = 'UPDATE' THEN
                IF OLD.data_ordem IS DISTINCT FROM NEW.data_ordem THEN
                    MERGE INTO public.{log_table_name} AS t
                    USING (SELECT OLD.data_ordem AS data_ordem, v_now AS datetime_alteracao) AS s
                    ON (t.data_ordem = s.data_ordem)
                    WHEN MATCHED THEN
                        UPDATE SET datetime_alteracao = s.datetime_alteracao
                    WHEN NOT MATCHED THEN
                        INSERT (data_ordem, datetime_alteracao)
                        VALUES (s.data_ordem, s.datetime_alteracao);

                    MERGE INTO public.{log_table_name} AS t
                    USING (SELECT NEW.data_ordem AS data_ordem, v_now AS datetime_alteracao) AS s
                    ON (t.data_ordem = s.data_ordem)
                    WHEN MATCHED THEN
                        UPDATE SET datetime_alteracao = s.datetime_alteracao
                    WHEN NOT MATCHED THEN
                        INSERT (data_ordem, datetime_alteracao)
                        VALUES (s.data_ordem, s.datetime_alteracao);
                ELSE
                    MERGE INTO public.{log_table_name} AS t
                    USING (SELECT NEW.data_ordem AS data_ordem, v_now AS datetime_alteracao) AS s
                    ON (t.data_ordem = s.data_ordem)
                    WHEN MATCHED THEN
                        UPDATE SET datetime_alteracao = s.datetime_alteracao
                    WHEN NOT MATCHED THEN
                        INSERT (data_ordem, datetime_alteracao)
                        VALUES (s.data_ordem, s.datetime_alteracao);
                END IF;
            END IF;

            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """

    cur.execute(sql)

    sql = f"""
        CREATE TRIGGER {constants.LOG_TRIGGER_NAME}
        AFTER INSERT OR UPDATE OR DELETE
        ON public.{constants.TRANSACAO_VALOR_ORDEM_POSTGRES_TABLE_NAME}
        FOR EACH ROW
        EXECUTE FUNCTION public.{constants.LOG_FUNCTION_NAME}();
    """

    cur.execute(sql)

    print("Trigger criado")
