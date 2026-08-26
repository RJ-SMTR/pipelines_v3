# Changelog - capture__gtfs

## [1.3.1] - 2026-08-26

### Adicionado

- Adiciona ao pós-teste do GTFS o teste `dbt_utils__unique_combination_of_columns__ordem_servico_faixa_horaria_sentido`. (https://github.com/RJ-SMTR/pipelines_v3/pull/574)

## [1.3.0] - 2026-08-17

### Adicionado

- Adiciona ao pós-teste do GTFS a validação de trajetos alternativos de `viagem_planejada_planejamento`. (https://github.com/RJ-SMTR/pipelines_v3/pull/478)
- Adiciona ao pós-teste do GTFS o teste `test_consistencia_servicos_ordem_servico_gtfs`, que valida a existência de trips e horários para os serviços da Ordem de Serviço. (https://github.com/RJ-SMTR/pipelines_v3/pull/478)

## [1.2.2] - 2026-07-31

### Alterado

- Altera a descrição do teste `dbt_expectations.expect_table_aggregation_to_equal_other_table__servico_sentido__ordem_servico_trajeto_alternativo_sentido` no flow `capture__gtfs`(https://github.com/RJ-SMTR/pipelines_v3/pull/470)

## [1.2.1] - 2026-07-30

### Adicionado

- Adiciona o teste `dbt_utils.relationships_where__servico_evento__ordem_servico_trajeto_alternativo_sentido` ao flow `capture__gtfs` (https://github.com/RJ-SMTR/pipelines_v3/pull/411)
- Adiciona o teste `dbt_expectations.expect_column_values_to_match_regex__evento__ordem_servico_trajeto_alternativo_sentido` ao flow `capture__gtfs` (https://github.com/RJ-SMTR/pipelines_v3/pull/411)

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
