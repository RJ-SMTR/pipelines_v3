# Changelog - treatment__remuneracao_openfisca

## [Unreleased]

### Alterado

- `incremental_delay_hours` de 48h para `5 * 24` (5 dias) no selector
  `remuneracao_openfisca` e no wait `viagens_sppo`
- Modelos do selector `remuneracao_openfisca` passam a materializar no
  dataset `sistemario` (antes em `subsidio`)
- WIP/teste: `data_sources` aguarda `viagens_sppo` (`viagem_completa`) —
  `viagem_valida` ainda sem dados

## [0.1.0] - 2026-08-03

### Adicionado

- Flow Prefect para o selector dbt `remuneracao_openfisca` (cadeia
  `viagem_valida_*` → `viagens_apuradas` → FCF/RQ)
- Instala `openfisca_smtr` na imagem Docker (modelo Python `viagens_apuradas`)
- `data_sources`: `viagem_validacao` e `planejamento_diario`
