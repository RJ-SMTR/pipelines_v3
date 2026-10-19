# Changelog - capture__riorotativo_credenciados

## [1.0.4] - 2026-10-15

### Adicionado

- Ativa captura dos bloqueios (https://github.com/RJ-SMTR/pipelines_v3/pull/768)

## [1.0.3] - 2026-10-14

### Adicionado
- Envia as falhas da validação do contrato de dados ao Google Chat (`google_chat_riorotativo`) em prod (https://github.com/RJ-SMTR/pipelines_v3/pull/748)

### Alterado

- Valida cada entidade credenciadora com o contrato do seu staging (`staging_guardador_veiculo_<cnpj>_riorotativo`) (https://github.com/RJ-SMTR/pipelines_v3/pull/748)
- O contrato de dados passa a incluir os testes dbt `not_null`, `unique` e `accepted_values` do staging, com as descrições dos testes (https://github.com/RJ-SMTR/pipelines_v3/pull/748)
- Alertas do contrato de dados não bloqueiam mais o upload ao GCS; apenas falhas (https://github.com/RJ-SMTR/pipelines_v3/pull/748)
- Log da validação do contrato traduzido para português (https://github.com/RJ-SMTR/pipelines_v3/pull/748)

## [1.0.2] - 2026-10-05

### Adicionado

- Valida o contrato dos dados brutos de guardadores de veículo antes do upload ao GCS (https://github.com/RJ-SMTR/pipelines_v3/pull/694)
- Permite pular a validação em uma execução com `skip_data_contract_validation=true` (https://github.com/RJ-SMTR/pipelines_v3/pull/694)

## [1.0.1] - 2026-07-15

### Adicionado

- Adiciona captura da tabela `entidade_34152025000122` (https://github.com/RJ-SMTR/pipelines_v3/pull/355)

## [1.0.0] - 2026-07-06

### Adicionado

- Cria flow `capture__riorotativo_credenciados` (https://github.com/RJ-SMTR/pipelines_v3/pull/352)
