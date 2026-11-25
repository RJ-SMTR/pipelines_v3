# Changelog - capture__sppo_realocacao

## [1.0.2] - 2026-11-24

### Alterado

- Desativa os schedules de produção de captura e recaptura horária (`active: false`), pois a fonte SPPO está inativa (https://github.com/RJ-SMTR/pipelines_v3/pull/917)

## [1.0.1] - 2026-05-18

### Adicionado

- Adiciona schedule inativo de recaptura a cada 15 minutos (https://github.com/RJ-SMTR/pipelines_v3/pull/193)

## [1.0.0] - 2026-04-16

### Adicionado

- Cria flow `capture__sppo_realocacao` para captura dos dados de realocação de GPS do SPPO (https://github.com/RJ-SMTR/pipelines_v3/pull/119)
