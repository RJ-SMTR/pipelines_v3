# openfisca_smtr

Pacote OpenFisca com regras versionadas da SMTR **no grão viagem**.

A simulação é independente do dbt. A fachada **`apurar`** devolve só **viagens**
(Tab. 2, picos, frota diária, IPA/precária, OPEX). FCF quinzena, CAPEX, QR, RQ e
subsídio ficam no dbt.

## API recomendada — `apurar`

Period diário (`YYYY-MM-DD`), `hora_partida` e `day_of_week` sempre derivados de `datetime_partida`.

```python
from openfisca_smtr import apurar

resultado = apurar(
    viagens=[
        {
            "id_apuracao": "v1",
            "datetime_partida": "2026-08-14T07:00:00",
            "servico": "731",
            "sentido": "I",
            "faixa_horaria_inicio": "05:00",
            "indicador_viagem_completa": True,
            "indicador_viagem_valida": True,
            "indicador_viagem_conforme": True,
            "km_programada": 10.0,
            "km_percorrida": 0.0,
            "lote": "B2",
            "id_veiculo": "ABC1D23",
            "servico_viagens_programadas": 8,
        }
    ],
)

resultado["viagens"]      # Tab. 2, km, picos, frota_operante, ipa, opex, …
resultado["periods"]
resultado["versao_regra"]
```

CLI:

```bash
python -m openfisca_smtr apurar --viagens viagens.json

python -m openfisca_smtr simulate --period 2026-08-14 \
  --output km_remuneravel --input fatos.json
```

## Docs

- Plano do zero: [PLANO_IMPLEMENTACAO.md](PLANO_IMPLEMENTACAO.md)
- Arquitetura do pacote: [ARQUITETURA_PACOTE.md](ARQUITETURA_PACOTE.md)
- Contrato dbt/DAG: [arquitetura_funcoes.md](arquitetura_funcoes.md)
