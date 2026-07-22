# Changelog - capture__gtfs

## [1.2.1] - 2026-07-22

### Adicionado

- Adiciona o teste `test_check_trajeto_alternativo__trips_gtfs` ao flow `capture__gtfs`
- Adiciona o teste `test_formato_evento__ordem_servico_trajeto_alternativo_sentido` ao flow `capture__gtfs`(https://github.com/RJ-SMTR/pipelines_v3/pull/411)

## [1.2.0] - 2026-06-23

### Adicionado

- Dispara a materialização do `treatment__planejamento_diario` após a captura e materialização do GTFS, usando janela derivada da vigência do feed capturado. (https://github.com/RJ-SMTR/pipelines_v3/pull/292)
- Adiciona pré-materialização com `--empty` para relações dbt ausentes em ambiente dev. (https://github.com/RJ-SMTR/pipelines_v3/pull/292)

### Alterado

- Prefixa mensagens de Discord do GTFS com `[DEV]` em execuções de desenvolvimento. (https://github.com/RJ-SMTR/pipelines_v3/pull/292)

## [1.1.0] - 2026-05-28

### Adicionado

- Adiciona tasks `setup_dbt_queries` e `install_dbt_packages` para download da pasta `queries/` e instalação de pacotes dbt em runtime (https://github.com/RJ-SMTR/pipelines_v3/pull/214)

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
