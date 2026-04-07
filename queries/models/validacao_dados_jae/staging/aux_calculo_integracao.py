# -*- coding: utf-8 -*-
from datetime import datetime

from pyspark.sql.types import IntegerType, StringType, StructField, StructType


def model(dbt, session):  # noqa: ARG001, PLR0915
    dbt.config(
        materialized="table",
    )
    df_transacao = dbt.ref("aux_transacao_filtro_integracao_calculada")

    df_matriz = dbt.ref("matriz_integracao").toPandas()

    df_matriz["modo_integracao_origem"] = df_matriz.modo_origem.combine_first(
        df_matriz.integracao_origem
    )

    df_matriz_integracao = df_matriz[df_matriz["tipo_integracao"] == "Integração"]
    df_matriz_transferencia = df_matriz[df_matriz["tipo_integracao"] == "Transferência"]

    def is_integracao(  # noqa: PLR0913
        integracao_origem,
        modo_destino,
        servico_origem,
        servico_destino,
        data_transacao,
        datetime_inicio_integracao,
        datetime_transacao,
    ):
        tempo_integracao = (
            datetime.fromisoformat(datetime_transacao)
            - datetime.fromisoformat(datetime_inicio_integracao)
        ).total_seconds() / 60

        return not (
            df_matriz_integracao[
                (df_matriz_integracao["data_inicio"] <= data_transacao)
                & (
                    (df_matriz_integracao["data_fim"] >= data_transacao)
                    | (df_matriz_integracao["data_fim"].isna())
                )
                & (df_matriz_integracao["modo_integracao_origem"] == integracao_origem)
                & (df_matriz_integracao["modo_destino"] == modo_destino)
                & (
                    (df_matriz_integracao["id_servico_jae_origem"] == servico_origem)
                    | (df_matriz_integracao["id_servico_jae_origem"].isna())
                )
                & (
                    (df_matriz_integracao["id_servico_jae_destino"] == servico_destino)
                    | (df_matriz_integracao["id_servico_jae_destino"].isna())
                )
                & (df_matriz_integracao["tempo_integracao_minutos"] >= tempo_integracao)
                & (df_matriz_integracao["indicador_integracao"])
            ].empty
        )

    def is_transferencia(  # noqa: PLR0913
        modo_origem,
        modo_destino,
        servico_origem,
        servico_destino,
        datetime_inicio_transferencia,
        data_transacao,
        datetime_transacao,
        datetime_transacao_anterior,
    ):
        if datetime_inicio_transferencia is None:
            datetime_inicio_transferencia = datetime_transacao_anterior

        tempo_transferencia = (
            datetime.fromisoformat(datetime_transacao)
            - datetime.fromisoformat(datetime_inicio_transferencia)
        ).total_seconds() / 60

        return servico_origem != servico_destino and not (
            df_matriz_transferencia[
                (df_matriz_transferencia["data_inicio"] <= data_transacao)
                & (
                    (df_matriz_transferencia["data_fim"] >= data_transacao)
                    | (df_matriz_transferencia["data_fim"].isna())
                )
                & (df_matriz_transferencia["modo_origem"] == modo_origem)
                & (df_matriz_transferencia["modo_destino"] == modo_destino)
                & (
                    (df_matriz_transferencia["id_servico_jae_origem"] == servico_origem)
                    | (df_matriz_transferencia["id_servico_jae_origem"].isna())
                )
                & (
                    (df_matriz_transferencia["id_servico_jae_destino"] == servico_destino)
                    | (df_matriz_transferencia["id_servico_jae_destino"].isna())
                )
                & (df_matriz_transferencia["tempo_integracao_minutos"] >= tempo_transferencia)
                & (df_matriz_transferencia["indicador_integracao"])
            ].empty
        )

    def itera_transacao(partition):  # noqa: PLR0915
        integracao_origem = ""
        modo_origem = ""
        servico_origem = ""
        id_integracao = ""
        sequencia_integracao_origem = 0
        datetime_transacao_anterior = None
        datetime_inicio_integracao = None
        datetime_inicio_transferencia = None
        flag_transferencia = False
        for row in partition:
            novo_row = list(row)
            if integracao_origem == "":
                id_integracao = row.id_transacao
                sequencia_integracao = sequencia_integracao_origem + 1
                datetime_inicio_integracao = row.datetime_transacao

                novo_row.append(id_integracao)
                novo_row.append(sequencia_integracao)
                novo_row.append("Primeira perna")
                novo_row.append(datetime_inicio_integracao)

                integracao_origem = row.modo_join

            elif is_transferencia(
                modo_origem=modo_origem,
                modo_destino=row.modo_join,
                servico_origem=servico_origem,
                servico_destino=row.id_servico_jae,
                data_transacao=row.data,
                datetime_inicio_transferencia=datetime_inicio_transferencia,
                datetime_transacao=row.datetime_transacao,
                datetime_transacao_anterior=datetime_transacao_anterior,
            ):
                if not flag_transferencia:
                    datetime_inicio_transferencia = row.datetime_transacao

                sequencia_integracao = sequencia_integracao_origem + 1
                novo_row.append(id_integracao)
                novo_row.append(sequencia_integracao)
                novo_row.append("Transferência")
                novo_row.append(datetime_inicio_integracao)

                flag_transferencia = True

            elif is_integracao(
                integracao_origem=integracao_origem,
                modo_destino=row.modo_join,
                servico_origem=servico_origem,
                servico_destino=row.id_servico_jae,
                data_transacao=row.data,
                datetime_inicio_integracao=datetime_inicio_integracao,
                datetime_transacao=row.datetime_transacao,
            ):
                sequencia_integracao = sequencia_integracao_origem + 1
                novo_row.append(id_integracao)
                novo_row.append(sequencia_integracao)
                novo_row.append("Integração")
                novo_row.append(datetime_inicio_integracao)

                integracao_origem += f"-{row.modo_join}"

                flag_transferencia = False
                datetime_inicio_transferencia = None

            else:
                sequencia_integracao = 1
                id_integracao = row.id_transacao
                datetime_inicio_integracao = row.datetime_transacao
                integracao_origem = row.modo_join
                novo_row.append(id_integracao)
                novo_row.append(sequencia_integracao)
                novo_row.append("Primeira perna")
                novo_row.append(datetime_inicio_integracao)

                flag_transferencia = False
                datetime_inicio_transferencia = None

            sequencia_integracao_origem = sequencia_integracao
            modo_origem = row.modo_join
            servico_origem = row.id_servico_jae
            datetime_transacao_anterior = row.datetime_transacao

            yield tuple(novo_row)

    df_transacao_cliente = df_transacao.rdd.map(lambda row: (row.cliente_cartao, row)).groupByKey()

    def process_cliente(rows):
        sorted_rows = sorted(rows, key=lambda r: r.datetime_transacao)
        return itera_transacao(sorted_rows)

    rdd_integracao = df_transacao_cliente.flatMap(lambda kv: process_cliente(kv[1]))

    schema_integracao = StructType(
        df_transacao.schema.fields  # noqa: RUF005
        + [
            StructField("id_integracao", StringType(), True),
            StructField("sequencia_integracao", IntegerType(), True),
            StructField("tipo_integracao", StringType(), True),
            StructField("datetime_inicio_integracao", StringType(), True),
        ]
    )

    df_integracao = spark.createDataFrame(rdd_integracao, schema=schema_integracao)  # noqa

    return df_integracao
