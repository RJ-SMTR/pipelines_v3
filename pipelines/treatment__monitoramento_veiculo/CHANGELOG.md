# Changelog - treatment__monitoramento_veiculo

## [1.0.1] - 2026-09-09

### Alterado

- Altera o schedule do flow de 5h45 para 6h45 (https://github.com/RJ-SMTR/pipelines_v3/pull/658)

### Adicionado

- Adiciona pós-teste `dbt_expectations.expect_row_values_to_have_data_for_every_n_datepart` no arquivo de infração (`dbt_expectations__expect_row_values_to_have_data_for_every_n_datepart__staging_infracao`, severity `warn`) (https://github.com/RJ-SMTR/pipelines_v3/pull/658)

## [1.0.0] - 2026-04-29

### Adicionado

- Cria flow `treatment__monitoramento_veiculo`(https://github.com/RJ-SMTR/pipelines_v3/pull/118)
