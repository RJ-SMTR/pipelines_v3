# Changelog - monitoramento_interno

## [1.0.3] - 2026-06-30

### Adicionado

- Cria views `view_metricas_testes_dbt`, `view_sumario_testes_dbt_dia` e `view_ultimo_resultado_testes_dbt` para acompanhamento dos resultados de testes dbt gravados pelo Elementary (https://github.com/RJ-SMTR/pipelines_v3/pull/321)

## [1.0.2] - 2026-06-25

### Adicionado

- Adiciona a coluna `consorcio` nos modelos `aux_monitoramento_registros_status_trajeto` e `viagem_inferida` (https://github.com/RJ-SMTR/pipelines_v3/pull/311)

## [1.0.1] - 2025-11-17

### Adicionado

- Adiciona os modelos `monitoramento_sumario_servico_dia_historico`e `monitoramento_sumario_dia_tipo_viagem_historico`ao monitoramento interno e adiciona em staging os modelos auxiliares `monitoramento_servico_dia_tipo_viagem_v2`,`monitoramento_servico_dia_tipo_viagem` `monitoramento_servico_dia` e `monitoramento_servico_dia_v2` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1024)

## [1.0.0] - 2025-03-25

### Adicionado

- Cria modelos para monitoramento de viagens: `aux_monitoramento_registros_status_trajeto`, `registros_status_viagem_inferida` e `viagem_inferida` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/458)
