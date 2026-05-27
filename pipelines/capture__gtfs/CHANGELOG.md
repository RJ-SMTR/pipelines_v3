# Changelog - capture__gtfs

## [1.0.2] - 2026-05-27

### Adicionado

- Adiciona o teste `dbt_expectations.expect_column_values_to_be_between__distancia_planejada__ordem_servico_trips_shapes_gtfs` ao flow `capture__gtfs` (https://github.com/RJ-SMTR/pipelines_v3/pull/206)
### Alterado

- Altera o teste `dbt_expectations.expect_table_aggregation_to_equal_other_table__ordem_servico_trajeto_alternativo_sentido` do flow `capture__gtfs` para rodar apenas após `2025-12-21` (https://github.com/RJ-SMTR/pipelines_v3/pull/206)

## [1.0.1] - 2026-05-04

### Alterado

- Altera o flow `capture__gtfs` para utilizar o parâmetro `concurrency_limit` (https://github.com/RJ-SMTR/pipelines_v3/pull/146)
- Altera o flow `capture__gtfs` para utilizar o parâmetro `collision_strategy` (https://github.com/RJ-SMTR/pipelines_v3/pull/147)

## [1.0.0] - 2026-04-29

### Adicionado

- Migra flow `capture__gtfs` do Prefect 1.4 para Prefect 3 (https://github.com/RJ-SMTR/pipelines_v3/pull/127)
