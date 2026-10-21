# Changelog - capture__smtr_gtfs

## [1.0.2] - 2026-10-21

### Adicionado

- Valida os contratos de dados dos arquivos brutos de `trips`, `shapes` e `ordem_servico_faixa_horaria_sentido` antes do upload ao GCS (https://github.com/RJ-SMTR/pipelines_v3/pull/781)
- Verifica os relationships entre os arquivos de uma mesma captura: `shape_id` de `trips` em `shapes` e `tipo_os` entre `ordem_servico_trajeto_alternativo_sentido` e `ordem_servico_faixa_horaria_sentido`, nas duas direções (https://github.com/RJ-SMTR/pipelines_v3/pull/781)

### Alterado

- As falhas do contrato de dados enviadas ao Google Chat mostram a partição capturada de cada tabela (`data_versao`) (https://github.com/RJ-SMTR/pipelines_v3/pull/786)
- Em `trips`, `block_id`, `wheelchair_accessible` e `bikes_allowed` ficam fora da validação do contrato, pois só aparecem em algumas versões do GTFS (https://github.com/RJ-SMTR/pipelines_v3/pull/781)
- Os arquivos brutos são validados a partir de uma cópia em Parquet, o que reduz o tempo da validação de `ordem_servico_faixa_horaria_sentido`; os arquivos enviados ao GCS continuam em CSV (https://github.com/RJ-SMTR/pipelines_v3/pull/781)

## [1.0.1] - 2026-10-19

### Adicionado

- Configura o webhook `google_chat_planejamento` para notificar no Google Chat as falhas de validação dos contratos de dados em produção (https://github.com/RJ-SMTR/pipelines_v3/pull/773)

## [1.0.0] - 2026-10-08

### Adicionado

- Cria o flow de captura do GTFS sobre o flow genérico de captura, substituindo o `capture__gtfs` (https://github.com/RJ-SMTR/pipelines_v3/pull/736)
