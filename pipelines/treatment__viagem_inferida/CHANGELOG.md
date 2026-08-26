# Changelog - treatment__viagem_inferida

## [1.1.0] - 2026-08-24

### Alterado

- Altera o schedule do flow para execução a cada 3 horas e ajusta o atraso incremental de 24 para 1 hora, permitindo a materialização das viagens do dia corrente (D0) após a disponibilidade dos dados de GPS (https://github.com/RJ-SMTR/pipelines_v3/pull/540)

## [1.0.0] - 2026-07-28

### Adicionado

- Cria flow `treatment__viagem_inferida` (https://github.com/RJ-SMTR/pipelines_v3/pull/444)
