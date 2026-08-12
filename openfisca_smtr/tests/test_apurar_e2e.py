# -*- coding: utf-8 -*-
from datetime import date

import pytest
from openfisca_smtr import apurar, get_versao_regra
from openfisca_smtr.apurar import period_from_row, spark_day_of_week


def _viagem_base(**overrides):
    base = {
        "id_apuracao": "v1",
        "datetime_partida": "2026-08-14T07:00:00",
        "lote": "B2",
        "id_veiculo": "VEIC-1",
        "indicador_viagem_completa": True,
        "indicador_viagem_valida": True,
        "indicador_viagem_conforme": True,
        "km_programada": 10.0,
        "km_percorrida": 0.0,
        "servico": "731",
        "sentido": "I",
        "faixa_horaria_inicio": "05:00",
        "servico_viagens_programadas": 2,
    }
    base.update(overrides)
    return base


def test_period_from_datetime_partida():
    assert period_from_row({"datetime_partida": "2026-08-14T07:30:00"}) == "2026-08-14"


def test_spark_day_of_week():
    assert spark_day_of_week(date(2026, 8, 14)) == 6
    assert spark_day_of_week(date(2026, 8, 16)) == 1


def test_apurar_deriva_hora_e_day_of_week():
    resultado = apurar(viagens=[_viagem_base(hora_partida=99, day_of_week=1)])
    v1 = resultado["viagens"][0]
    assert v1["hora_partida"] == 7
    assert v1["day_of_week"] == 6
    assert v1["indicador_completa_pico_manha"] is True
    assert v1["indicador_dia_util"] is True


def test_apurar_rejeita_viagem_incompleta():
    incompleta = _viagem_base()
    del incompleta["servico_viagens_programadas"]
    del incompleta["id_veiculo"]
    with pytest.raises(ValueError, match="faltam"):
        apurar(viagens=[incompleta])


def test_apurar_exige_datetime_partida():
    with pytest.raises(ValueError, match="datetime_partida"):
        apurar(viagens=[_viagem_base(datetime_partida=None, data="2026-08-14")])


def test_apurar_e2e_viagens_com_frota_ipa_e_opex():
    resultado = apurar(
        viagens=[
            _viagem_base(
                id_apuracao="v1",
                datetime_partida="2026-08-14T07:00:00",
                id_veiculo="VEIC-1",
                servico_viagens_programadas=2,
            ),
            _viagem_base(
                id_apuracao="v2",
                datetime_partida="2026-08-14T12:00:00",
                id_veiculo="VEIC-2",
                indicador_viagem_conforme=False,
                faixa_horaria_inicio="05:00",
                servico_viagens_programadas=2,
            ),
            _viagem_base(
                id_apuracao="v3",
                datetime_partida="2026-08-14T07:30:00",
                id_veiculo="VEIC-1",
                km_programada=8.0,
                sentido="V",
                faixa_horaria_inicio="05:00",
                servico_viagens_programadas=1,
            ),
        ],
        id_execucao="teste-e2e-1",
    )

    assert resultado["period"] == "2026-08-14"
    assert resultado["versao_regra"] == get_versao_regra()
    assert "faixas" not in resultado

    v1, v2, v3 = resultado["viagens"]
    assert v1["km_remuneravel"] == 10.0
    assert v2["km_remuneravel"] == 0.0
    assert v2["indicador_percentual_atendimento"] is True
    assert v3["km_remuneravel"] == 8.0

    # faixa I: v1+v2 — ambas contam %; programadas=2 → 100% → ipa=1
    assert v1["viagens_atendimento_faixa"] == 2
    assert v1["viagens_programadas_faixa"] == 2
    assert v1["percentual_atendimento"] == 1.0
    assert v1["ipa"] == 1.0
    assert v1["desconto_operacao_precaria"] == 0.0
    assert v1["qc_km_faixa"] == 10.0
    assert v1["qc_km_ponderada_ipa"] == 10.0
    assert v2["ipa"] == 1.0

    # faixa V: só v3 — programadas=1, atendimento=1 → ipa=1
    assert v3["ipa"] == 1.0
    assert v3["qc_km_faixa"] == 8.0

    # lote×data: MAX(pico manha=1 VEIC-1, pico tarde=0) = 1
    assert v1["frota_pico_manha"] == 1.0
    assert v1["frota_pico_tarde"] == 0.0
    assert v1["frota_operante"] == 1.0
    assert "fcf" not in v1

    # OPEX: TR × β × km × ipa (prd=0)
    assert abs(v1["remuneracao_opex_viagem"] - v1["tarifa_remuneracao"] * v1["beta"] * 10.0) < 1e-4
    assert abs(v3["remuneracao_opex_viagem"] - v3["tarifa_remuneracao"] * v3["beta"] * 8.0) < 1e-4
    assert v2["remuneracao_opex_viagem"] == 0.0
    assert round(v1["tarifa_remuneracao"], 2) == 11.53
    assert round(v1["beta"], 2) == 0.78


def test_apurar_ipa_operacao_precaria():
    # 1 de 3 programadas → ~33% → ipa=0, desconto grave 1200
    resultado = apurar(
        viagens=[
            _viagem_base(
                id_apuracao="v1",
                servico_viagens_programadas=3,
                indicador_viagem_conforme=True,
            ),
            _viagem_base(
                id_apuracao="v2",
                datetime_partida="2026-08-14T08:00:00",
                id_veiculo="VEIC-2",
                indicador_viagem_valida=False,
                indicador_viagem_conforme=False,
                servico_viagens_programadas=3,
            ),
            _viagem_base(
                id_apuracao="v3",
                datetime_partida="2026-08-14T08:30:00",
                id_veiculo="VEIC-3",
                indicador_viagem_valida=False,
                indicador_viagem_conforme=False,
                servico_viagens_programadas=3,
            ),
        ],
    )
    v1 = resultado["viagens"][0]
    assert v1["viagens_atendimento_faixa"] == 1
    assert abs(v1["percentual_atendimento"] - 1 / 3) < 1e-6
    assert v1["ipa"] == 0.0
    assert v1["desconto_operacao_precaria"] == 1200.0
    assert v1["remuneracao_opex_viagem"] == 0.0


def test_apurar_frota_max_picos_e_zero_fim_de_semana():
    resultado = apurar(
        viagens=[
            _viagem_base(
                id_apuracao="v1",
                datetime_partida="2026-08-14T07:00:00",
                id_veiculo="VEIC-1",
                servico_viagens_programadas=1,
            ),
            _viagem_base(
                id_apuracao="v2",
                datetime_partida="2026-08-14T17:00:00",
                id_veiculo="VEIC-2",
                servico_viagens_programadas=1,
            ),
            _viagem_base(
                id_apuracao="v3",
                datetime_partida="2026-08-16T07:00:00",
                id_veiculo="VEIC-3",
                km_programada=5.0,
                servico_viagens_programadas=1,
            ),
        ],
    )
    assert resultado["periods"] == ["2026-08-14", "2026-08-16"]
    por_id = {v["id_apuracao"]: v for v in resultado["viagens"]}
    # sexta: MAX(manha=1, tarde=1) = 1
    assert por_id["v1"]["frota_pico_manha"] == 1.0
    assert por_id["v1"]["frota_pico_tarde"] == 1.0
    assert por_id["v1"]["frota_operante"] == 1.0
    assert por_id["v2"]["frota_operante"] == 1.0
    # domingo: dia nao util → 0
    assert por_id["v3"]["indicador_dia_util"] is False
    assert por_id["v3"]["frota_operante"] == 0.0
    assert all(v["ipa"] == 1.0 for v in resultado["viagens"])
