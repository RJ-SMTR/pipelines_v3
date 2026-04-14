# -*- coding: utf-8 -*-
"""Funções para exportação das transações do BQ para o Postgres"""

from datetime import datetime
from functools import partial
from typing import Callable

import pandas_gbq
import psycopg2
from google.cloud.storage import Blob
from psycopg2._psycopg import connection, cursor

from pipelines.capture__cct_pagamento import constants as cct_constants
from pipelines.common.utils.secret import get_env_secret
from pipelines.integration__upload_transacao_cct import constants


def create_postgres_connection(env: str) -> Callable[[], connection]:
    """
    Cria uma função de conexão com o banco Postgres para o ambiente especificado.

    Args:
        env (str): dev ou prod.

    Returns:
        Callable[[], connection]: Função que, ao ser chamada, retorna aconexão com o Postgres.
    """
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
    """
    Retorna as partições da tabela `rj-smtr.bilhetagem.transacao`
    que foram modificadas entre dois timestamps.

    Args:
        project_id (str): ID do projeto no BigQuery.
        start_ts (str): Timestamp inicial no formato compatível com BigQuery.
        end_ts (str): Timestamp final no formato compatível com BigQuery.

    Returns:
        list[str]: Lista de partições modificadas no intervalo informado.
    """

    sql = f"""
    SELECT
        CONCAT("'", PARSE_DATE("%Y%m%d", partition_id), "'") AS particao
    FROM
        `rj-smtr.bilhetagem.INFORMATION_SCHEMA.PARTITIONS`
    WHERE
        table_name = "transacao"
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

    sql = f"""
        SELECT
            DISTINCT CONCAT("'", data, "'") AS particao
        FROM
            {project_id}.{constants.TRANSACAO_CCT_VIEW_FULL_NAME}
        WHERE
        data IN ({", ".join(modified_partitions)})
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
    """
    Retorna as partições da tabela `rj-smtr.bilhetagem.transacao`
    associadas a um intervalo de datas de ordem.

    Args:
        project_id (str): ID do projeto no BigQuery.
        data_ordem_start (str): Data da ordem de pagamento inicial para filtrar as partições.
        data_ordem_end (str): Data da ordem de pagamento final para filtrar as partições.

    Returns:
        list[str]: Lista de partições correspondentes ao intervalo de datas de ordem.
    """

    try:
        datetime.strptime(data_ordem_start, "%Y-%m-%d")  # noqa: DTZ007
        datetime.strptime(data_ordem_end, "%Y-%m-%d")  # noqa: DTZ007
    except ValueError as e:
        raise ValueError(f"Formato de data_ordem inválido: {e}") from e

    sql = f"""
    SELECT
        DISTINCT CONCAT("'", data_transacao, "'") AS particao
    FROM
        `rj-smtr.bilhetagem_interno.data_ordem_transacao`
    WHERE
        data_ordem BETWEEN "{data_ordem_start}" AND "{data_ordem_end}"
    """

    print(f"Executando query:\n{sql}")

    partitions = pandas_gbq.read_gbq(
        sql,
        project_id=project_id,
    )["particao"].to_list()

    sql = f"""
        SELECT
            DISTINCT CONCAT("'", data, "'") AS particao
        FROM
            {project_id}.{constants.TRANSACAO_CCT_VIEW_FULL_NAME}

        WHERE
        data IN ({", ".join(partitions)})
        AND data_ordem BETWEEN "{data_ordem_start}" AND "{data_ordem_end}"
    """

    print(f"Executando query:\n{sql}")
    return pandas_gbq.read_gbq(
        sql,
        project_id=project_id,
    )["particao"].to_list()


def create_temp_table(cur: cursor, blob: Blob, full_refresh: bool):
    """
    Cria e popula a tabela temporária no PostgreSQL com os dados de um arquivo CSV no GCS.

    Args:
        cur (cursor): Cursor ativo da conexão PostgreSQL.
        blob (Blob): Objeto Blob que representa o arquivo CSV no GCS.
        full_refresh (bool): Indica se o carregamento é completo (True) ou incremental (False).
    """

    if not full_refresh:
        tmp_table_name = constants.TRANSACAO_POSTGRES_TMP_TABLE_NAME
        sql = f"""
            CREATE TABLE IF NOT EXISTS public.{tmp_table_name}
            (
                id_transacao character varying(60),
                data date,
                datetime_transacao timestamp,
                consorcio character varying(20),
                tipo_transacao character varying(50),
                valor_pagamento numeric(13,5),
                data_ordem date,
                id_ordem_pagamento integer,
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
            ON public.{tmp_table_name} (id_transacao)
        """
        print("Criando índice tabela temporária")
        cur.execute(sql)


def merge_final_data(
    cur: cursor,
    blob: Blob,
    full_refresh: bool,
    export_bigquery_dates: list[str],
):
    """
    Atualiza a tabela final no PostgreSQL com base em um arquivo CSV no GCS,
    substituindo registros existentes.

    Args:
        cur (cursor): Cursor ativo da conexão PostgreSQL.
        blob (Blob): Objeto Blob que representa o arquivo CSV no GCS.
        full_refresh (bool): Indica se o carregamento é completo (True) ou incremental (False).
        export_bigquery_dates (list[str]): Lista de datas das transações exportadas do BigQuery
    """

    table_name = constants.TRANSACAO_POSTGRES_TABLE_NAME
    tmp_table_name = constants.TRANSACAO_POSTGRES_TMP_TABLE_NAME

    if not full_refresh:
        sql = f"""
            DELETE FROM public.{table_name} t
            USING public.{tmp_table_name} s
            WHERE t.id_transacao = s.id_transacao
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
        ON public.{table_name} (id_transacao)
    """
    print("Recriando índice id_transacao da tabela final")
    cur.execute(sql)

    date_values = [f"(DATE({d}))" for d in export_bigquery_dates]
    sql = f"""
        MERGE INTO public.{constants.LOG_TABLE_NAME} AS t
        USING (
            VALUES
            {",".join(date_values)}
        ) AS s(data)
        ON t.data = s.data
        WHEN MATCHED THEN
            UPDATE SET datetime_alteracao = now()
        WHEN NOT MATCHED THEN
            INSERT (data, datetime_alteracao)
            VALUES (s.data, now());
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
                USING (SELECT OLD.data AS data, v_now AS datetime_alteracao) AS s
                ON (t.data = s.data)
                WHEN MATCHED THEN
                    UPDATE SET datetime_alteracao = s.datetime_alteracao
                WHEN NOT MATCHED THEN
                    INSERT (data, datetime_alteracao) VALUES (s.data, s.datetime_alteracao);

            ELSIF TG_OP = 'INSERT' THEN
                -- Atualiza/inclui a data nova
                MERGE INTO public.{log_table_name} AS t
                USING (SELECT NEW.data AS data, v_now AS datetime_alteracao) AS s
                ON (t.data = s.data)
                WHEN MATCHED THEN
                    UPDATE SET datetime_alteracao = s.datetime_alteracao
                WHEN NOT MATCHED THEN
                    INSERT (data, datetime_alteracao) VALUES (s.data, s.datetime_alteracao);

            ELSIF TG_OP = 'UPDATE' THEN
                -- Caso a data tenha mudado, atualiza as duas (antiga e nova)
                IF OLD.data IS DISTINCT FROM NEW.data THEN
                    -- Atualiza a data antiga
                    MERGE INTO public.{log_table_name} AS t
                    USING (SELECT OLD.data AS data, v_now AS datetime_alteracao) AS s
                    ON (t.data = s.data)
                    WHEN MATCHED THEN
                        UPDATE SET datetime_alteracao = s.datetime_alteracao
                    WHEN NOT MATCHED THEN
                        INSERT (data, datetime_alteracao) VALUES (s.data, s.datetime_alteracao);

                    -- Atualiza a data nova
                    MERGE INTO public.{log_table_name} AS t
                    USING (SELECT NEW.data AS data, v_now AS datetime_alteracao) AS s
                    ON (t.data = s.data)
                    WHEN MATCHED THEN
                        UPDATE SET datetime_alteracao = s.datetime_alteracao
                    WHEN NOT MATCHED THEN
                        INSERT (data, datetime_alteracao) VALUES (s.data, s.datetime_alteracao);
                ELSE
                    -- Caso a data não tenha mudado, só atualiza a nova
                    MERGE INTO public.{log_table_name} AS t
                    USING (SELECT NEW.data AS data, v_now AS datetime_alteracao) AS s
                    ON (t.data = s.data)
                    WHEN MATCHED THEN
                        UPDATE SET datetime_alteracao = s.datetime_alteracao
                    WHEN NOT MATCHED THEN
                        INSERT (data, datetime_alteracao) VALUES (s.data, s.datetime_alteracao);
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
        ON public.{constants.TRANSACAO_POSTGRES_TABLE_NAME}
        FOR EACH ROW
        EXECUTE FUNCTION public.{constants.LOG_FUNCTION_NAME}();
    """

    cur.execute(sql)

    print("Trigger criado")
