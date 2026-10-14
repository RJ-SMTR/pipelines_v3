# Changelog

## [2.0.2] - 2026-10-14

### Removido

- Remove as descrições dos testes das constantes; as notificações usam as descrições definidas no dbt (https://github.com/RJ-SMTR/pipelines_v3/pull/763)

## [2.0.1] - 2026-10-14

### Adicionado

- Envia as falhas dos testes dbt ao Google Chat (`google_chat_riorotativo`) em prod (https://github.com/RJ-SMTR/pipelines_v3/pull/748)

### Alterado

- As notificações dos testes usam as descrições definidas no dbt (https://github.com/RJ-SMTR/pipelines_v3/pull/748)

## [2.0.0] - 2026-07-30

### Alterado

- Renomeia flow para `treatment__riorotativo_auxiliar` (https://github.com/RJ-SMTR/pipelines_v3/pull/458)
- Altera execução diária para horária (https://github.com/RJ-SMTR/pipelines_v3/pull/458)

## [1.0.1] - 2026-07-15

### Adicionado

- Adiciona dicionário de testes (`RIOROTATIVO_DIARIO_CHECKS_LIST`) e objeto `DBTTest` pós-materialização ao selector `riorotativo_diario` (https://github.com/RJ-SMTR/pipelines_v3/pull/378)
- Adiciona selector `snapshot_riorotativo` (`SNAPSHOT_RIOROTATIVO_SELECTOR`) ao flow, cobrindo `snapshot_area_estacionamento` e `snapshot_perfil_funcionamento` (https://github.com/RJ-SMTR/pipelines_v3/pull/378)

## [1.0.0] - 2026-07-08

### Adicionado

- Cria flow de materialização dos dados do Rio Rotativo Digital (https://github.com/RJ-SMTR/pipelines_v3/pull/355)
