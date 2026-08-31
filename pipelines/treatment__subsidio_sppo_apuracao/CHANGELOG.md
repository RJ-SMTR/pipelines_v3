# Changelog - treatment__subsidio_sppo_apuracao

## [1.0.5] - 2026-08-28

### Adicionado

- Adiciona descrições dos testes `unique__snapshot_key__viagem_planejada`, `dbt_utils__mutually_exclusive_ranges__id_veiculo__viagem_completa`, `check_viagem_completa__viagens_remuneradas`, `dbt_utils__not_constant__status__veiculo_dia` e `subsidio_viagens_atualizadas__sumario_servico_dia_historico` para notificação no Discord. (https://github.com/RJ-SMTR/pipelines_v3/pull/588)

## [1.0.4] - 2026-08-20

### Adicionado

- Adiciona descrições dos testes de `viagem_completa` no `PRE_CHECKS_LIST` para notificação no Discord. (https://github.com/RJ-SMTR/pipelines_v3/pull/522)

## [1.0.3] - 2026-07-28

### Adicionado

- Adiciona descrições dos testes `sumario_servico_dia_tipo_soma_km__km_apurada__sumario_servico_dia_tipo` e `sumario_servico_dia_tipo_soma_km__km_apurada_dia__sumario_servico_dia_pagamento`. (https://github.com/RJ-SMTR/pipelines_v3/pull/443)

## [1.0.2] - 2026-06-02

### Adicionado

- Adiciona parâmetro `test_only` (https://github.com/RJ-SMTR/pipelines_v3/pull/223)

## [1.0.1] - 2026-05-22

### Corrigido

- Ajusta horários de `DATA_SUBSIDIO_V9_INICIO`, `DATA_SUBSIDIO_V14_INICIO` e `SUBSIDIO_INITIAL_DATETIME` para coincidir com o horário do schedule (https://github.com/RJ-SMTR/pipelines_v3/pull/209)

## [1.0.0] - 2026-05-14

### Adicionado

- Cria flow `treatment__subsidio_sppo_apuracao` (https://github.com/RJ-SMTR/pipelines_v3/pull/185)
