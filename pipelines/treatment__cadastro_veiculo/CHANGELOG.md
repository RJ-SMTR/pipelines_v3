# Changelog - treatment__cadastro_veiculo

## [1.0.1] - 2026-09-09

### Alterado

- Alterado o schedule do flow de 6h para 7h (https://github.com/RJ-SMTR/pipelines_v3/pull/658)

### Adicionado

- Adiciona pós-teste `dbt_expectations.expect_row_values_to_have_data_for_every_n_datepart` no arquivo de licenciamento STU (`dbt_expectations__expect_row_values_to_have_data_for_every_n_datepart__staging_licenciamento_stu`, severity `warn`) (https://github.com/RJ-SMTR/pipelines_v3/pull/658)

## [1.0.0] - 2026-05-05

### Adicionado

- Cria flow `treatment__cadastro_veiculo` (https://github.com/RJ-SMTR/pipelines_v3/pull/135)