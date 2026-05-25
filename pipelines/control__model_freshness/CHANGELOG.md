# Changelog

## [1.0.1] - 2026-05-21

### Alterado

- Substitui `datetime.now()` por `get_scheduled_timestamp()` e amplia janela de teste de 1h para 2h para evitar race condition com captura na virada do dia (https://github.com/RJ-SMTR/pipelines_v3/pull/202)

## [1.0.0] - 2026-05-13

### Adicionado

- Cria flow `control__model_freshness` (https://github.com/RJ-SMTR/pipelines_v3/pull/181)
