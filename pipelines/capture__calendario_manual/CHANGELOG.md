# Changelog - capture__calendario_manual

## [1.0.0] - 2026-06-23

### Adicionado

- Cria flow `capture__calendario_manual` para ingestão da aba "Dias Atípicos" da planilha do calendário manual, com detecção de mudança pelo índice e hash da última linha submetida, e disparo da materialização do `treatment__planejamento_diario`. (https://github.com/RJ-SMTR/pipelines_v3/pull/306)
