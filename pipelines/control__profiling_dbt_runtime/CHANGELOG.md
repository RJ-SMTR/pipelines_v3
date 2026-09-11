# Changelog - control__profiling_dbt_runtime

## [0.1.0] - 2026-05-19

### Adicionado

- Flow `control__profiling_dbt_runtime`: baixa a pasta `queries` do repositório
  em runtime (git sparse-checkout), roda `dbt deps` e `dbt run`/`debug`
  escrevendo no target `dev`, instrumentado com `profile_resources`.
- Dockerfile próprio sem `COPY ./queries` nem `dbt deps`, validando a futura
  remoção da pasta queries da imagem base.
