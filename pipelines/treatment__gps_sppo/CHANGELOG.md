# Changelog - treatment__gps_sppo

## [1.1.0] - 2026-10-14

### Alterado

- Altera o selector para `gps_sppo`, removendo `redis_key_suffix` e as variáveis `modo_gps`, `fonte_gps` e `15_minutos` (https://github.com/RJ-SMTR/pipelines_v3/pull/757)

## [1.0.2] - 2026-10-14

### Removido

- Remove as descrições dos testes das constantes; as notificações usam as descrições definidas no dbt (https://github.com/RJ-SMTR/pipelines_v3/pull/763)

## [1.0.1] - 2026-06-01

### Adicionado

- Adiciona `collision_strategy: CANCEL_NEW` (https://github.com/RJ-SMTR/pipelines_v3/pull/221)

## [1.0.0] - 2026-04-16

### Adicionado

- Cria flow `treatment__gps_sppo` (https://github.com/RJ-SMTR/pipelines_v3/pull/119)
