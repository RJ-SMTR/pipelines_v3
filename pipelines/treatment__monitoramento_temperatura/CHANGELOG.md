# Changelog - treatment__monitoramento_temperatura

## [1.0.3] - 2026-10-26

### Adicionado

- Adiciona o parâmetro `use_partition_dates` para reprocessar diretamente as partições do intervalo informado (https://github.com/RJ-SMTR/pipelines_v3/pull/632)

### Alterado

- Altera o schedule de 7h para 9h e inclui `viagem_validacao` nas fontes do flow (https://github.com/RJ-SMTR/pipelines_v3/pull/632)

### Corrigido

- Inclui as dependências de `treatment__viagem_validacao` no Dockerfile (https://github.com/RJ-SMTR/pipelines_v3/pull/632)

## [1.0.2] - 2026-10-14

### Removido

- Remove as descrições dos testes das constantes; as notificações usam as descrições definidas no dbt (https://github.com/RJ-SMTR/pipelines_v3/pull/763)

## [1.0.1] - 2026-09-09

### Alterado

- Alterado o schedule do flow de 6h para 7h (https://github.com/RJ-SMTR/pipelines_v3/pull/658)

## [1.0.0] - 2026-04-29

### Adicionado

- Cria flow `treatment__monitoramento_temperatura` (https://github.com/RJ-SMTR/pipelines_v3/pull/120)
