# Changelog - monitoramento_interno

## [1.0.5] - 2026-08-06

### Alterado

- Amplia `view_viagem_monitoramento` com `servico_informado`, `servico_realizado`, `tipo_dia`, `vista`, `inicio_periodo`, `fim_periodo` e `tempo_viagem` para painéis internos; no pós-V25, obtém `tipo_dia` de `calendario` e `vista` de `ordem_servico_trips_shapes_gtfs` por `feed_start_date` e `shape_id`, mantendo os períodos nulos e os dois novos campos de serviço alinhados a `servico`

## [1.0.4] - 2026-07-31

### Adicionado

- Cria `view_viagem_monitoramento`, consolidando `viagem_completa` e `viagem_inferida` em uma interface histórica para painéis internos (https://github.com/RJ-SMTR/pipelines_v3/pull/438)

## [1.0.3] - 2026-07-28

### Adicionado

- Adiciona as colunas `id_viagem_planejada`, `fonte_gps` e `id_execucao_dbt` no modelo `viagem_inferida` (https://github.com/RJ-SMTR/pipelines_v3/pull/444)

### Alterado

- Substitui `view_gps_sppo_completo` por `view_gps_onibus` em `aux_monitoramento_registros_status_trajeto` (https://github.com/RJ-SMTR/pipelines_v3/pull/444)
- Renomeia `timestamp_gps` para `datetime_gps` na cadeia `aux_monitoramento_registros_status_trajeto` → `viagem_inferida` → `registros_status_viagem_inferida` (https://github.com/RJ-SMTR/pipelines_v3/pull/444)

## [1.0.2] - 2026-06-25

### Adicionado

- Adiciona a coluna `consorcio` nos modelos `aux_monitoramento_registros_status_trajeto` e `viagem_inferida` (https://github.com/RJ-SMTR/pipelines_v3/pull/311)

## [1.0.1] - 2025-11-17

### Adicionado

- Adiciona os modelos `monitoramento_sumario_servico_dia_historico`e `monitoramento_sumario_dia_tipo_viagem_historico`ao monitoramento interno e adiciona em staging os modelos auxiliares `monitoramento_servico_dia_tipo_viagem_v2`,`monitoramento_servico_dia_tipo_viagem` `monitoramento_servico_dia` e `monitoramento_servico_dia_v2` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1024)

## [1.0.0] - 2025-03-25

### Adicionado

- Cria modelos para monitoramento de viagens: `aux_monitoramento_registros_status_trajeto`, `registros_status_viagem_inferida` e `viagem_inferida` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/458)
