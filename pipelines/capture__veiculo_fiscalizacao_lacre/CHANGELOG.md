# Changelog - capture__veiculo_fiscalizacao_lacre

## [1.1.0] - 2026-07-13

### Alterado

- Substitui a definição manual de `SourceTable` e a task de extração local pela fábrica
  `create_google_sheet_capture_params` e pela task compartilhada
  `create_google_sheet_extractor` de `pipelines.common.capture.google_sheets`

## [1.0.0] - 2026-05-07

### Adicionado

- Cria flow `capture__veiculo_fiscalizacao_lacre` (https://github.com/RJ-SMTR/pipelines_v3/pull/165)
