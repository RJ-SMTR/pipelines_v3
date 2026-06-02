# Changelog - treatment__gps_sppo

## [1.0.2] - 2026-06-02

### Alterado

- Exclui teste de freshness `dbt_expectations.expect_row_values_to_have_recent_data__datetime_gps__gps` do flow de materialização, passando a rodar apenas no flow `control__model_freshness` (https://github.com/RJ-SMTR/pipelines_v3/pull/224)

## [1.0.1] - 2026-06-01

### Adicionado

- Adiciona `collision_strategy: CANCEL_NEW` (https://github.com/RJ-SMTR/pipelines_v3/pull/221)

## [1.0.0] - 2026-04-16

### Adicionado

- Cria flow `treatment__gps_sppo` (https://github.com/RJ-SMTR/pipelines_v3/pull/119)
