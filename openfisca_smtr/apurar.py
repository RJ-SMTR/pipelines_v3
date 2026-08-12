# -*- coding: utf-8 -*-
"""Fachada unica de apuracao no grao viagem.

Uso::

    from openfisca_smtr import apurar

    resultado = apurar(
        viagens=[
            {
                "id_apuracao": "v1",
                "datetime_partida": "2026-08-14T07:00:00",
                "indicador_viagem_completa": True,
                "indicador_viagem_valida": True,
                "indicador_viagem_conforme": True,
                "km_programada": 10.0,
                "km_percorrida": 0.0,
                "lote": "B2",
                "id_veiculo": "ABC1D23",
                "servico": "731",
                "sentido": "I",
                "faixa_horaria_inicio": "05:00",
                "servico_viagens_programadas": 2,
            }
        ],
    )
    # frota_operante = MAX(picos) por lote×data (0 se nao dia util).
    # FCF/QR/CAPEX na quinzena (dbt). OPEX por viagem no OF.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from typing import Any

from openfisca_smtr.simulation import simulate
from openfisca_smtr.system import SubsidioRemuneracaoTaxBenefitSystem
from openfisca_smtr.version import get_versao_regra

Periodo = str
Linha = Mapping[str, Any]
LinhaMutavel = dict[str, Any]

_ISO_DATE_LEN = 10

FAIXA_KEYS = ("data", "lote", "servico", "sentido", "faixa_horaria_inicio")

VIAGEM_OUTPUTS = [
    "indicador_quilometragem_pagamento",
    "indicador_percentual_atendimento",
    "km_remuneravel",
    "indicador_pico_manha",
    "indicador_pico_tarde",
    "indicador_completa_pico_manha",
    "indicador_completa_pico_tarde",
    "indicador_dia_util",
    "tarifa_remuneracao",
    "alpha",
    "beta",
]

IPA_OUTPUTS = [
    "percentual_atendimento",
    "ipa",
    "desconto_operacao_precaria",
]

VIAGEM_INPUT_KEYS = {
    "indicador_viagem_completa",
    "indicador_viagem_valida",
    "indicador_viagem_conforme",
    "km_programada",
    "km_percorrida",
    "lote",
}

VIAGEM_REQUIRED_KEYS = (
    "datetime_partida",
    "indicador_viagem_completa",
    "indicador_viagem_valida",
    "indicador_viagem_conforme",
    "km_programada",
    "km_percorrida",
    "lote",
    "id_veiculo",
    "servico",
    "sentido",
    "faixa_horaria_inicio",
    "servico_viagens_programadas",
)


def period_from_value(value: Any) -> Periodo:
    """Converte data/datetime/str em period OpenFisca diario ``YYYY-MM-DD``."""
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    texto = str(value).strip()
    if len(texto) < _ISO_DATE_LEN:
        msg = f"Valor de data invalido para period: {value!r}"
        raise ValueError(msg)
    dia = texto[:_ISO_DATE_LEN]
    date.fromisoformat(dia)
    return dia


def period_from_row(row: Mapping[str, Any]) -> Periodo:
    """Deriva o period da row a partir de ``datetime_partida``."""
    if row.get("datetime_partida") is not None:
        return period_from_value(row["datetime_partida"])
    msg = "Row precisa de 'datetime_partida' para derivar o period OpenFisca."
    raise ValueError(msg)


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime(value.year, value.month, value.day)
    texto = str(value).strip().replace("Z", "+00:00")
    if "T" in texto or " " in texto:
        return datetime.fromisoformat(texto.replace(" ", "T", 1))
    dia = date.fromisoformat(texto[:_ISO_DATE_LEN])
    return datetime(dia.year, dia.month, dia.day)


def spark_day_of_week(dia: date) -> int:
    """Spark: 1=domingo … 7=sabado (Python weekday: 0=segunda … 6=domingo)."""
    return (dia.weekday() + 1) % 7 + 1


def _enrich_hora_e_dow(row: LinhaMutavel) -> None:
    """Sempre define ``hora_partida`` e ``day_of_week`` a partir de ``datetime_partida``."""
    dt = _parse_datetime(row["datetime_partida"])
    row["hora_partida"] = dt.hour
    row["day_of_week"] = spark_day_of_week(dt.date())


def _campos_faltantes(row: Mapping[str, Any], id_key: str) -> list[str]:
    faltantes: list[str] = []
    if row.get(id_key) is None:
        faltantes.append(id_key)
    for key in VIAGEM_REQUIRED_KEYS:
        if row.get(key) is None:
            faltantes.append(key)
    return faltantes


def _validar_e_enriquecer_viagens(
    viagens: Sequence[LinhaMutavel],
    *,
    id_key: str,
) -> None:
    """Exige campos obrigatorios e deriva hora/dow. Sem calculo parcial."""
    erros: list[str] = []
    for indice, row in enumerate(viagens):
        faltantes = _campos_faltantes(row, id_key)
        if faltantes:
            ref = row.get(id_key, f"indice={indice}")
            erros.append(f"{ref}: faltam {', '.join(faltantes)}")
            continue
        _enrich_hora_e_dow(row)
    if erros:
        msg = "Viagens incompletas para apurar (sem calculo parcial): " + "; ".join(erros)
        raise ValueError(msg)


def _filter_inputs(row: Mapping[str, Any], allowed: set[str], id_key: str) -> LinhaMutavel:
    filtrado: LinhaMutavel = {id_key: row[id_key]}
    for key in allowed:
        if key in row and row[key] is not None:
            filtrado[key] = row[key]
    filtrado["hora_partida"] = row["hora_partida"]
    filtrado["day_of_week"] = row["day_of_week"]
    return filtrado


def _simulate_por_period(
    rows: Sequence[LinhaMutavel],
    *,
    period_por_id: Mapping[str, Periodo],
    outputs: Sequence[str],
    id_key: str,
    system: SubsidioRemuneracaoTaxBenefitSystem,
) -> list[LinhaMutavel]:
    """Roda ``simulate`` agrupando rows pelo period derivado."""
    grupos: dict[Periodo, list[LinhaMutavel]] = defaultdict(list)
    for row in rows:
        grupos[period_por_id[str(row[id_key])]].append(row)

    saida: list[LinhaMutavel] = []
    for periodo, grupo in sorted(grupos.items()):
        calculado = simulate(
            grupo,
            period=periodo,
            outputs=list(outputs),
            id_key=id_key,
            system=system,
            keep_inputs=False,
        )
        for item in calculado:
            linha = dict(item)
            linha["period"] = periodo
            saida.append(linha)
    return saida


def _frota_operante_por_lote_periodo(
    viagens: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, Periodo], dict[str, float]]:
    """Frota operante diaria (item 4): MAX(pico manha, pico tarde) por lote×data.

    Em dia nao util → frota_operante = 0 (nao entra na media quinzenal no dbt).
    """
    por_chave: dict[tuple[str, Periodo], list[Mapping[str, Any]]] = defaultdict(list)
    for viagem in viagens:
        chave = (str(viagem.get("lote", "")), str(viagem["period"]))
        por_chave[chave].append(viagem)

    resultado: dict[tuple[str, Periodo], dict[str, float]] = {}
    for chave, itens in por_chave.items():
        dia_util = any(bool(item.get("indicador_dia_util")) for item in itens)
        veiculos_manha: set[str] = set()
        veiculos_tarde: set[str] = set()
        for item in itens:
            id_veiculo = item.get("id_veiculo")
            if id_veiculo is None:
                continue
            vid = str(id_veiculo)
            if item.get("indicador_completa_pico_manha"):
                veiculos_manha.add(vid)
            if item.get("indicador_completa_pico_tarde"):
                veiculos_tarde.add(vid)
        n_manha = float(len(veiculos_manha))
        n_tarde = float(len(veiculos_tarde))
        resultado[chave] = {
            "frota_pico_manha": n_manha,
            "frota_pico_tarde": n_tarde,
            "frota_operante": max(n_manha, n_tarde) if dia_util else 0.0,
        }
    return resultado


def _faixa_id(viagem: Mapping[str, Any]) -> str:
    return "|".join(str(viagem.get(key, "")) for key in FAIXA_KEYS)


def _agregar_faixas(viagens: Sequence[LinhaMutavel]) -> list[LinhaMutavel]:
    grupos: dict[str, list[LinhaMutavel]] = defaultdict(list)
    for viagem in viagens:
        grupos[_faixa_id(viagem)].append(viagem)

    faixas: list[LinhaMutavel] = []
    for indice, (_chave, itens) in enumerate(grupos.items()):
        primeira = itens[0]
        atendimento = sum(1 for item in itens if item.get("indicador_percentual_atendimento"))
        programadas = int(primeira["servico_viagens_programadas"])
        qc_km = sum(float(item.get("km_remuneravel") or 0.0) for item in itens)
        faixas.append(
            {
                "id_apuracao": f"faixa-{indice}",
                "faixa_chave": _faixa_id(primeira),
                "data": primeira.get("data") or primeira.get("period"),
                "lote": primeira.get("lote"),
                "servico": primeira.get("servico"),
                "sentido": primeira.get("sentido"),
                "faixa_horaria_inicio": primeira.get("faixa_horaria_inicio"),
                "viagens_programadas": programadas,
                "viagens_atendimento": int(atendimento),
                "qc_km": qc_km,
                "ids_viagem": [str(item["id_apuracao"]) for item in itens],
            }
        )
    return faixas


def _aplicar_ipa_nas_viagens(
    viagens: Sequence[LinhaMutavel],
    *,
    id_key: str,
    system: SubsidioRemuneracaoTaxBenefitSystem,
) -> None:
    """Agrega faixa, calcula IPA/precária no OF e faz broadcast nas viagens."""
    faixas = _agregar_faixas(viagens)
    if not faixas:
        return

    period_por_faixa = {
        str(faixa["id_apuracao"]): period_from_value(faixa["data"]) for faixa in faixas
    }
    rows_faixa = [
        {
            "id_apuracao": faixa["id_apuracao"],
            "viagens_programadas": faixa["viagens_programadas"],
            "viagens_atendimento": faixa["viagens_atendimento"],
        }
        for faixa in faixas
    ]
    calculado = _simulate_por_period(
        rows_faixa,
        period_por_id=period_por_faixa,
        outputs=IPA_OUTPUTS,
        id_key="id_apuracao",
        system=system,
    )
    por_faixa = {str(item["id_apuracao"]): item for item in calculado}
    por_viagem: dict[str, LinhaMutavel] = {str(v[id_key]): v for v in viagens}

    for faixa in faixas:
        calc = por_faixa[str(faixa["id_apuracao"])]
        ipa_valor = float(calc["ipa"])
        qc_km = float(faixa["qc_km"])
        payload = {
            "viagens_atendimento_faixa": faixa["viagens_atendimento"],
            "viagens_programadas_faixa": faixa["viagens_programadas"],
            "percentual_atendimento": float(calc["percentual_atendimento"]),
            "ipa": ipa_valor,
            "desconto_operacao_precaria": float(calc["desconto_operacao_precaria"]),
            "qc_km_faixa": qc_km,
            "qc_km_ponderada_ipa": qc_km * ipa_valor,
        }
        for id_viagem in faixa["ids_viagem"]:
            por_viagem[id_viagem].update(payload)


def _aplicar_opex_nas_viagens(viagens: Sequence[LinhaMutavel]) -> None:
    """OPEX por viagem: TR * beta * km_remuneravel * ipa (prd=0)."""
    for viagem in viagens:
        km = float(viagem.get("km_remuneravel") or 0.0)
        ipa_valor = float(viagem.get("ipa") or 0.0)
        tr = float(viagem.get("tarifa_remuneracao") or 0.0)
        beta_valor = float(viagem.get("beta") or 0.0)
        viagem["km_ponderada_ipa_viagem"] = km * ipa_valor
        viagem["remuneracao_opex_viagem"] = tr * beta_valor * km * ipa_valor


def apurar(
    viagens: Sequence[Linha] | None = None,
    *,
    id_key: str = "id_apuracao",
    system: SubsidioRemuneracaoTaxBenefitSystem | None = None,
    id_execucao: str | None = None,
) -> dict[str, Any]:
    """Executa a apuracao no grao viagem.

    Calcula Tab. 2, picos, ``frota_operante`` diaria (MAX picos, 0 se nao
    dia util), IPA/precária e ``remuneracao_opex_viagem``. FCF/QR/CAPEX
    ficam na quinzena (dbt).
    """
    tax_system = system or SubsidioRemuneracaoTaxBenefitSystem()
    viagens_in = [dict(row) for row in (viagens or [])]
    periods_usados: set[Periodo] = set()

    viagens_out: list[LinhaMutavel] = []
    if viagens_in:
        _validar_e_enriquecer_viagens(viagens_in, id_key=id_key)

        period_por_viagem: dict[str, Periodo] = {}
        for row in viagens_in:
            periodo_row = period_from_row(row)
            period_por_viagem[str(row[id_key])] = periodo_row
            periods_usados.add(periodo_row)

        rows_of = [_filter_inputs(row, VIAGEM_INPUT_KEYS, id_key) for row in viagens_in]
        calculado = _simulate_por_period(
            rows_of,
            period_por_id=period_por_viagem,
            outputs=VIAGEM_OUTPUTS,
            id_key=id_key,
            system=tax_system,
        )
        por_id = {str(item[id_key]): item for item in calculado}
        for row in viagens_in:
            merged = dict(row)
            merged.update(por_id[str(row[id_key])])
            periodo_row = period_por_viagem[str(row[id_key])]
            merged["period"] = periodo_row
            if "data" not in merged:
                merged["data"] = periodo_row
            viagens_out.append(merged)

        frota_por_chave = _frota_operante_por_lote_periodo(viagens_out)
        for viagem in viagens_out:
            chave = (str(viagem.get("lote", "")), str(viagem["period"]))
            frota = frota_por_chave.get(
                chave,
                {"frota_operante": 0.0, "frota_pico_manha": 0.0, "frota_pico_tarde": 0.0},
            )
            viagem["frota_operante"] = frota["frota_operante"]
            viagem["frota_pico_manha"] = frota["frota_pico_manha"]
            viagem["frota_pico_tarde"] = frota["frota_pico_tarde"]

        _aplicar_ipa_nas_viagens(viagens_out, id_key=id_key, system=tax_system)
        _aplicar_opex_nas_viagens(viagens_out)

    periods_ordenados = sorted(periods_usados)
    resultado: dict[str, Any] = {
        "periods": periods_ordenados,
        "period": periods_ordenados[0] if len(periods_ordenados) == 1 else periods_ordenados,
        "versao_regra": get_versao_regra(),
        "viagens": viagens_out,
    }
    if id_execucao is not None:
        resultado["id_execucao"] = id_execucao
    return resultado
