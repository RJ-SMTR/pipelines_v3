# Changelog - treatment__cadastro_veiculo

## [1.0.1] - 2026-09-09

### Alterado

- Alterado o schedule do flow de 6h para 7h (America/Sao_Paulo), após a captura de licenciamento

### Adicionado

- Adiciona pós-teste `dbt_utils.recency` no arquivo de licenciamento STU (`dbt_utils__recency__data__staging_licenciamento_stu`, severity `warn`)

## [1.0.0] - 2026-05-05

### Adicionado

- Cria flow `treatment__cadastro_veiculo` (https://github.com/RJ-SMTR/pipelines_v3/pull/135)