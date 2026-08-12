# -*- coding: utf-8 -*-
"""Viagens apuradas via ``openfisca_smtr.apurar`` (grão viagem).

Lê ``viagem_classificacao_validacao``, chama OpenFisca e persiste Tab. 2,
picos, frota diária, IPA/precária e OPEX por viagem.

Frota diária e IPA passam a sair daqui — ``aux_frota_operante_dia_lote``
fica depreciado.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any
from uuid import uuid4

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

from openfisca_smtr import apurar

# Campos ecoados do placeholder + calculados pelo apurar (schema estável BQ).
SCHEMA_VIAGENS_APURADAS = StructType(
    [
        StructField("id_apuracao", StringType(), False),
        StructField("id_viagem", StringType(), True),
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
        StructField("servico_viagens_programadas", IntegerType(), False),
        StructField("lote_frota_estimada", DoubleType(), True),
        StructField("lote_frota_determinada", DoubleType(), True),
        StructField("lote_qr_mensal", DoubleType(), True),
        StructField("lote_km_referencia", DoubleType(), True),
        StructField("servico_tipo", StringType(), True),
        StructField("servico_tecnologia", StringType(), True),
        StructField("tecnologia_fcf", StringType(), True),
        StructField("tipo_dia", StringType(), True),
        StructField("period", StringType(), False),
        StructField("hora_partida", IntegerType(), False),
        StructField("day_of_week", IntegerType(), False),
        StructField("indicador_quilometragem_pagamento", BooleanType(), False),
        StructField("indicador_percentual_atendimento", BooleanType(), False),
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

COLUNAS_ENTRADA = [
    "data",
    "id_viagem",
    "datetime_partida",
    "datetime_chegada",
    "indicador_viagem_completa",
    "indicador_viagem_valida",
    "indicador_viagem_conforme",
    "km_programada",
    "km_percorrida",
    "lote",
    "id_veiculo",
    "placa",
    "servico",
    "sentido",
    "faixa_horaria_inicio",
    "faixa_horaria_fim",
    "servico_viagens_programadas",
    "lote_frota_estimada",
    "lote_frota_determinada",
    "lote_qr_mensal",
    "lote_km_referencia",
    "servico_tipo",
    "servico_tecnologia",
    "tecnologia_fcf",
    "tipo_dia",
]


def _para_python(valor: Any) -> Any:
    if valor is None:
        return None
    if isinstance(valor, datetime):
        return valor
    if isinstance(valor, date):
        return valor
    return valor


def _montar_id_apuracao(row: dict[str, Any]) -> str:
    data = row["data"]
    data_str = data.isoformat() if isinstance(data, date) else str(data)[:10]
    partida = row["datetime_partida"]
    if isinstance(partida, datetime):
        partida_str = partida.isoformat(sep="T", timespec="seconds")
    else:
        partida_str = str(partida)
    return "|".join(
        [
            data_str,
            str(row["lote"]),
            str(row["id_veiculo"]),
            partida_str,
        ]
    )


def _linha_entrada(row: dict[str, Any]) -> dict[str, Any]:
    limpa = {chave: _para_python(row.get(chave)) for chave in COLUNAS_ENTRADA}
    limpa["id_apuracao"] = _montar_id_apuracao(limpa)
    limpa["servico_viagens_programadas"] = int(limpa["servico_viagens_programadas"])
    limpa["km_programada"] = float(limpa["km_programada"])
    limpa["km_percorrida"] = float(limpa["km_percorrida"])
    return limpa


def _opt_float(valor: Any) -> float | None:
    if valor is None:
        return None
    return float(valor)


def _como_date(valor: Any) -> date:
    if isinstance(valor, datetime):
        return valor.date()
    if isinstance(valor, date):
        return valor
    return date.fromisoformat(str(valor)[:10])


def _como_datetime(valor: Any) -> datetime | None:
    if valor is None:
        return None
    if isinstance(valor, datetime):
        return valor
    if isinstance(valor, date):
        return datetime.combine(valor, datetime.min.time())
    texto = str(valor).strip().replace("Z", "+00:00").replace(" ", "T", 1)
    return datetime.fromisoformat(texto)


def _tupla_saida(viagem: dict[str, Any], *, versao_regra: str, id_execucao: str | None) -> tuple:
    return (
        str(viagem["id_apuracao"]),
        viagem.get("id_viagem"),
        _como_date(viagem.get("data") or viagem["period"]),
        _como_datetime(viagem["datetime_partida"]),
        _como_datetime(viagem.get("datetime_chegada")),
        bool(viagem["indicador_viagem_completa"]),
        bool(viagem["indicador_viagem_valida"]),
        bool(viagem["indicador_viagem_conforme"]),
        float(viagem["km_programada"]),
        float(viagem["km_percorrida"]),
        str(viagem["lote"]),
        str(viagem["id_veiculo"]),
        viagem.get("placa"),
        str(viagem["servico"]),
        str(viagem["sentido"]),
        str(viagem["faixa_horaria_inicio"]),
        viagem.get("faixa_horaria_fim"),
        int(viagem["servico_viagens_programadas"]),
        _opt_float(viagem.get("lote_frota_estimada")),
        _opt_float(viagem.get("lote_frota_determinada")),
        _opt_float(viagem.get("lote_qr_mensal")),
        _opt_float(viagem.get("lote_km_referencia")),
        viagem.get("servico_tipo"),
        viagem.get("servico_tecnologia"),
        viagem.get("tecnologia_fcf"),
        viagem.get("tipo_dia"),
        str(viagem["period"]),
        int(viagem["hora_partida"]),
        int(viagem["day_of_week"]),
        bool(viagem["indicador_quilometragem_pagamento"]),
        bool(viagem["indicador_percentual_atendimento"]),
        float(viagem["km_remuneravel"]),
        bool(viagem["indicador_pico_manha"]),
        bool(viagem["indicador_pico_tarde"]),
        bool(viagem["indicador_completa_pico_manha"]),
        bool(viagem["indicador_completa_pico_tarde"]),
        bool(viagem["indicador_dia_util"]),
        float(viagem["frota_operante"]),
        float(viagem["frota_pico_manha"]),
        float(viagem["frota_pico_tarde"]),
        int(viagem["viagens_atendimento_faixa"]),
        int(viagem["viagens_programadas_faixa"]),
        float(viagem["percentual_atendimento"]),
        float(viagem["ipa"]),
        float(viagem["desconto_operacao_precaria"]),
        float(viagem["qc_km_faixa"]),
        float(viagem["qc_km_ponderada_ipa"]),
        float(viagem["km_ponderada_ipa_viagem"]),
        float(viagem["remuneracao_opex_viagem"]),
        float(viagem["tarifa_remuneracao"]),
        float(viagem["alpha"]),
        float(viagem["beta"]),
        versao_regra,
        id_execucao,
    )


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

    viagens_df = dbt.ref("viagem_classificacao_validacao").select(*COLUNAS_ENTRADA)
    linhas_brutas = [row.asDict(recursive=True) for row in viagens_df.collect()]
    id_execucao = f"dbt-{uuid4()}"

    if not linhas_brutas:
        return session.createDataFrame([], SCHEMA_VIAGENS_APURADAS)

    entradas = [_linha_entrada(row) for row in linhas_brutas]
    resultado = apurar(viagens=entradas, id_execucao=id_execucao)
    versao_regra = str(resultado["versao_regra"])
    tuplas = [
        _tupla_saida(viagem, versao_regra=versao_regra, id_execucao=id_execucao)
        for viagem in resultado["viagens"]
    ]
    return session.createDataFrame(tuplas, SCHEMA_VIAGENS_APURADAS)
