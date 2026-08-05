# Changelog

## [1.1.0] - 2026-08-03

### Adicionado

- Adiciona ao pós-teste do planejamento diário as validações de trajetos alternativos e de freshness em relação ao GTFS. (https://github.com/RJ-SMTR/pipelines_v3/pull/478)

## [1.0.1] - 2026-07-29

### Corrigido

- Impede que execuções manuais ou disparadas como subflow avancem o checkpoint do planejamento diário no Redis, preservando sua atualização para a execução agendada. (https://github.com/RJ-SMTR/pipelines_v3/pull/437)

## [1.0.0] - 2026-03-02

### Adicionado

- Migração do flow `PLANEJAMENTO_DIARIO_MATERIALIZACAO` do Prefect 1.4 para Prefect 3.0 (https://github.com/RJ-SMTR/pipelines_v3/pull/64)
