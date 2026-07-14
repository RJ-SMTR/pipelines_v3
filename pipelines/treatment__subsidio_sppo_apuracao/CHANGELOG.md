# Changelog - treatment__subsidio_sppo_apuracao

## [1.0.3] - 2026-07-14

### Alterado

- Adapta a chamada de `create_materialization_flows_default_tasks` ao novo contrato de `run_mode` e `MaterializationTestConfig`

## [1.0.2] - 2026-06-02

### Adicionado

- Adiciona parâmetro `test_only` (https://github.com/RJ-SMTR/pipelines_v3/pull/223)

## [1.0.1] - 2026-05-22

### Corrigido

- Ajusta horários de `DATA_SUBSIDIO_V9_INICIO`, `DATA_SUBSIDIO_V14_INICIO` e `SUBSIDIO_INITIAL_DATETIME` para coincidir com o horário do schedule (https://github.com/RJ-SMTR/pipelines_v3/pull/209)

## [1.0.0] - 2026-05-14

### Adicionado

- Cria flow `treatment__subsidio_sppo_apuracao` (https://github.com/RJ-SMTR/pipelines_v3/pull/185)
