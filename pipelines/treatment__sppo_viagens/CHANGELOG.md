# Changelog - treatment__sppo_viagens

## [1.2.1] - 2026-08-20

### Adicionado

- Cria teste de unicidade para `viagem_planejada` após a materialização e interrompe a execução do snapshot em caso de duplicidade (https://github.com/RJ-SMTR/pipelines_v3/pull/457)

## [1.2.0] - 2026-07-27

### Alterado

- Define o hard stop dos selectors após a execução de 01/08/2026, que materializa as viagens iniciadas em 31/07/2026, conforme a Resolução SMTR 3934/2026. (https://github.com/RJ-SMTR/pipelines_v3/pull/312)

## [1.1.1] - 2026-06-23

### Alterado

- Passa o ambiente resolvido para `setup_dbt_queries`, mantendo o checkout de `queries/` consistente com a regra de deploy de dev/prod. (https://github.com/RJ-SMTR/pipelines_v3/pull/292)

## [1.1.0] - 2026-05-27

### Adicionado

- Adiciona tasks `setup_dbt_queries` e `install_dbt_packages` para download da pasta `queries/` e instalação de pacotes dbt em runtime (https://github.com/RJ-SMTR/pipelines_v3/pull/214)

## [1.0.0] - 2026-04-30

### Adicionado

- Cria flow `treatment__sppo_viagens` (https://github.com/RJ-SMTR/pipelines_v3/pull/111)
