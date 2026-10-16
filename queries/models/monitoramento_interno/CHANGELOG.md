# Changelog - monitoramento_interno

## [1.0.9] - 2026-10-08

### Adicionado

- Cria o modelo ephemeral `aux_gps_viagem_inferida`, unindo GPS de bordo (ônibus e BRT) e GPS do validador Jaé para a cadeia de inferência. Alinha `id_veiculo` SPPO (prefixo de bordo vs ordem da Jaé) na janela de GPS, sem join por `data`. (https://github.com/RJ-SMTR/pipelines_v3/pull/677)

### Alterado

- Em `aux_monitoramento_registros_status_trajeto`, deixa de classificar o GPS em `start`/`end`/`middle`/`out` contra o shape e os pontos extremos. Passa a expor só `indicador_segmento_inicio` e `indicador_segmento_fim` (`st_intersects` com o buffer do primeiro e do último segmento). Lê GPS de `aux_gps_viagem_inferida` no lugar de `view_gps_onibus`. (https://github.com/RJ-SMTR/pipelines_v3/pull/677)
- Em `viagem_inferida`, emparelha partida e chegada pelos extremos exclusivos: partida é GPS só no primeiro segmento, chegada é GPS só no último. Ponto nos dois buffers (terminal circular) é ignorado para não abrir viagem a cada ping. A partida considerada é o último GPS só no início; a chegada é o primeiro GPS só no fim depois de um intervalo fora dele (`lag` + `last_value`). (https://github.com/RJ-SMTR/pipelines_v3/pull/677)

## [1.0.8] - 2026-09-16

### Alterado

- Ajusta `view_viagem_monitoramento` para expor os campos necessários aos painéis, mantendo `servico`, `vista` e `tempo_viagem`, e seguindo a ordem de colunas de `viagem_valida` (https://github.com/RJ-SMTR/pipelines_v3/pull/610)

## [1.0.7] - 2026-09-03

### Alterado

- Altera `monitoramento_servico_dia_v2` para obter `vista` de `aux_viagem_planejada_planejamento_dia_unnested` a partir de `DATA_SUBSIDIO_V25_INICIO`. Mantém `viagem_planejada` no período anterior. (https://github.com/RJ-SMTR/pipelines_v3/pull/657)

## [1.0.6] - 2026-08-28

### Adicionado

- Adiciona a coluna `sistema` ao modelo `viagem_inferida` (https://github.com/RJ-SMTR/pipelines_v3/pull/577)

### Alterado

- Usa `modo` e `sistema` provenientes do serviço planejado, remove `id_empresa` e o join com `routes_gtfs` da cadeia de inferência e reordena as colunas conforme a ontologia (https://github.com/RJ-SMTR/pipelines_v3/pull/577)

## [1.0.5] - 2026-08-25

### Alterado

- Atualiza `view_viagem_monitoramento` para usar `viagem_valida` a partir de `DATA_SUBSIDIO_V25_INICIO` e expor `sistema` na nova linhagem (https://github.com/RJ-SMTR/pipelines_v3/pull/552)

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
