# -*- coding: utf-8 -*-
"""Viagens apuradas via ``openfisca_smtr.apurar`` (grão viagem).

Lê ``viagem_classificacao_validacao`` (fatos) e ``servico_oferta_faixa``
(POR por faixa, todas as faixas do recorte) e persiste a saída do OF.

Nota: não usar ``from __future__`` — o Dataproc/dbt injeta código antes
deste arquivo.
"""

from uuid import uuid4

from openfisca_smtr import apurar
from pyspark.sql.functions import col
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Contrato BQ. Coerção de tipos / faixa / lote fica no openfisca_smtr.apurar.
SCHEMA_VIAGENS_APURADAS = StructType(
    [
        StructField("id_viagem", StringType(), False),
        StructField("data", DateType(), False),
        StructField("datetime_partida", TimestampType(), False),
        StructField("datetime_chegada", TimestampType(), True),
        StructField("indicador_viagem_completa", BooleanType(), False),
        StructField("indicador_viagem_valida", BooleanType(), False),
        StructField("indicador_viagem_conforme", BooleanType(), False),
        StructField("km_programada", DoubleType(), False),
        StructField("km_percorrida", DoubleType(), False),
        StructField("lote", StringType(), False),
        StructField("id_veiculo", StringType(), False),
        StructField("placa", StringType(), True),
        StructField("servico", StringType(), False),
        StructField("sentido", StringType(), False),
        StructField("faixa_horaria_inicio", StringType(), False),
        StructField("faixa_horaria_fim", StringType(), True),
        StructField("servico_tecnologia", StringType(), True),
        StructField("tecnologia_fcf", StringType(), True),
        StructField("tipo_dia", StringType(), True),
        StructField("period", StringType(), False),
        StructField("hora_partida", IntegerType(), False),
        StructField("day_of_week", IntegerType(), False),
        StructField("indicador_quilometragem_pagamento", BooleanType(), False),
        StructField("indicador_percentual_atendimento", BooleanType(), False),
        StructField("indicador_dentro_do_teto_programado", BooleanType(), False),
        StructField("km_remuneravel", DoubleType(), False),
        StructField("indicador_pico_manha", BooleanType(), False),
        StructField("indicador_pico_tarde", BooleanType(), False),
        StructField("indicador_completa_pico_manha", BooleanType(), False),
        StructField("indicador_completa_pico_tarde", BooleanType(), False),
        StructField("indicador_dia_util", BooleanType(), False),
        StructField("frota_operante", DoubleType(), False),
        StructField("frota_pico_manha", DoubleType(), False),
        StructField("frota_pico_tarde", DoubleType(), False),
        StructField("viagens_atendimento_faixa", IntegerType(), False),
        StructField("viagens_programadas_faixa", IntegerType(), False),
        StructField("percentual_atendimento", DoubleType(), False),
        StructField("ipa", DoubleType(), False),
        StructField("desconto_operacao_precaria", DoubleType(), False),
        StructField("qc_km_faixa", DoubleType(), False),
        StructField("qc_km_ponderada_ipa", DoubleType(), False),
        StructField("km_ponderada_ipa_viagem", DoubleType(), False),
        StructField("remuneracao_opex_viagem", DoubleType(), False),
        StructField("tarifa_remuneracao", DoubleType(), False),
        StructField("alpha", DoubleType(), False),
        StructField("beta", DoubleType(), False),
        StructField("versao_regra", StringType(), False),
        StructField("id_execucao", StringType(), True),
    ]
)

COLUNAS_VIAGEM = [
    "data",
    "id_viagem",
    "datetime_partida",
    "datetime_chegada",
    "indicador_viagem_completa",
    "indicador_viagem_valida",
    "indicador_viagem_conforme",
    "km_programada",
    "km_percorrida",
    "id_veiculo",
    "placa",
    "servico",
    "sentido",
    "faixa_horaria_inicio",
    "faixa_horaria_fim",
    "servico_tecnologia",
    "tecnologia_fcf",
    "tipo_dia",
]

COLUNAS_PLANEJAMENTO = [
    "data",
    "servico",
    "sentido",
    "faixa_horaria_inicio",
    "lote",
    "viagens_programadas",
]


def _linhas_saida(resultado, id_execucao):
    versao_regra = str(resultado["versao_regra"])
    nomes = [field.name for field in SCHEMA_VIAGENS_APURADAS]
    linhas = []
    for viagem in resultado["viagens"]:
        linha = dict(viagem)
        linha["versao_regra"] = versao_regra
        linha["id_execucao"] = id_execucao
        linhas.append({nome: linha.get(nome) for nome in nomes})
    return linhas


def model(dbt, session):
    dbt.config(
        materialized="table",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        tags=["remuneracao", "openfisca", "wip"],
    )

    viagens_df = dbt.ref("viagem_classificacao_validacao").select(*COLUNAS_VIAGEM)
    planejamento_df = dbt.ref("servico_oferta_faixa").select(*COLUNAS_PLANEJAMENTO)

    limites = viagens_df.agg(
        spark_min("data").alias("data_inicio"),
        spark_max("data").alias("data_fim"),
    ).collect()
    if not limites or limites[0]["data_inicio"] is None:
        return session.createDataFrame([], SCHEMA_VIAGENS_APURADAS)

    planejamento_df = planejamento_df.filter(
        (col("data") >= limites[0]["data_inicio"]) & (col("data") <= limites[0]["data_fim"])
    )

    id_execucao = f"dbt-{uuid4()}"
    resultado = apurar(
        viagens=[row.asDict(recursive=True) for row in viagens_df.collect()],
        planejamento=[row.asDict(recursive=True) for row in planejamento_df.collect()],
        id_key="id_viagem",
        id_execucao=id_execucao,
    )
    return session.createDataFrame(
        _linhas_saida(resultado, id_execucao),
        SCHEMA_VIAGENS_APURADAS,
    )
