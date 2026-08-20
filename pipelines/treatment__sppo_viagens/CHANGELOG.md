# Changelog - treatment__sppo_viagens

## [1.1.2] - 2026-08-20

### Adicionado

- Executa o teste `unique__snapshot_key__viagem_planejada` após a materialização e interrompe o snapshot em caso de duplicidade (https://github.com/RJ-SMTR/pipelines_v3/pull/400)

## [1.1.1] - 2026-06-23

### Alterado

- Passa o ambiente resolvido para `setup_dbt_queries`, mantendo o checkout de `queries/` consistente com a regra de deploy de dev/prod. (https://github.com/RJ-SMTR/pipelines_v3/pull/292)

## [1.1.0] - 2026-05-27

### Adicionado

- Adiciona tasks `setup_dbt_queries` e `install_dbt_packages` para download da pasta `queries/` e instalação de pacotes dbt em runtime (https://github.com/RJ-SMTR/pipelines_v3/pull/214)

## [1.0.0] - 2026-04-30

### Adicionado

- Cria flow `treatment__sppo_viagens` (https://github.com/RJ-SMTR/pipelines_v3/pull/111)
