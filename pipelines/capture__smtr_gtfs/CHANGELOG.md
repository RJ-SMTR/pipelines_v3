# Changelog - capture__smtr_gtfs

## [1.0.2] - 2026-10-21

### Adicionado

- Valida os contratos de dados dos arquivos brutos de `trips`, `shapes`, `stop_times`, `stops`, `routes`, `frequencies` e `ordem_servico_faixa_horaria_sentido` antes do upload ao GCS, incluindo o formato do evento de trajeto alternativo em `trip_headsign` e a extensão positiva dos serviços na faixa horária (https://github.com/RJ-SMTR/pipelines_v3/pull/781)
- Verifica os relationships entre os arquivos de uma mesma captura: `shape_id` de `trips` em `shapes`, `tipo_os` entre `ordem_servico_trajeto_alternativo_sentido` e `ordem_servico_faixa_horaria_sentido`, nas duas direções, `servico` do trajeto alternativo na faixa horária, `trip_id` e `stop_id` de `stop_times` em `trips` e `stops`, `route_id` e `service_id` de `trips` em `routes` e `calendar`, e `trip_id` de `frequencies` em `trips` (https://github.com/RJ-SMTR/pipelines_v3/pull/781)

### Alterado

- As falhas do contrato de dados enviadas ao Google Chat mostram a partição capturada de cada tabela (`data_versao`) (https://github.com/RJ-SMTR/pipelines_v3/pull/786)
- Colunas que só aparecem em algumas versões do GTFS ficam fora da validação do contrato: `block_id`, `wheelchair_accessible` e `bikes_allowed` em `trips`; `pickup_type`, `drop_off_type`, `continuous_pickup` e `continuous_drop_off` em `stop_times`; `tts_stop_name` e `level_id` em `stops`; e `route_url`, `route_sort_order`, `continuous_pickup`, `continuous_drop_off` e `network_id` em `routes` (https://github.com/RJ-SMTR/pipelines_v3/pull/781)
- Os arquivos brutos são validados a partir de uma cópia em Parquet, o que reduz o tempo da validação de `ordem_servico_faixa_horaria_sentido`; os arquivos enviados ao GCS continuam em CSV (https://github.com/RJ-SMTR/pipelines_v3/pull/781)

## [1.0.1] - 2026-10-19

### Adicionado

- Configura o webhook `google_chat_planejamento` para notificar no Google Chat as falhas de validação dos contratos de dados em produção (https://github.com/RJ-SMTR/pipelines_v3/pull/773)

## [1.0.0] - 2026-10-08

### Adicionado

- Cria o flow de captura do GTFS sobre o flow genérico de captura, substituindo o `capture__gtfs` (https://github.com/RJ-SMTR/pipelines_v3/pull/736)
