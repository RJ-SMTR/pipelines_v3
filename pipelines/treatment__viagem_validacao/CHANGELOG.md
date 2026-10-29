# Changelog - treatment__viagem_validacao

## [1.0.6] - 2026-10-26

### Adicionado

- Adiciona o parâmetro `use_partition_dates` para reprocessar diretamente as partições do intervalo informado (https://github.com/RJ-SMTR/pipelines_v3/pull/632)

## [1.0.5] - 2026-10-21

### Adicionado

- Inclui o selector `gps_v2_conecta_sistema_rio` nas fontes de dados do flow e as dependências correspondentes no Dockerfile (https://github.com/RJ-SMTR/pipelines_v3/pull/783)

### Corrigido

- Inclui a captura `capture__conecta_sistema_rio_viagem_informada` no Dockerfile do flow, dependência transitiva das constantes de `treatment__viagem_informada` (https://github.com/RJ-SMTR/pipelines_v3/pull/783)

## [1.0.4] - 2026-09-11

### Corrigido

- Inclui a fonte Maxtrack no selector de validação de viagens e as dependências correspondentes no Dockerfile do flow (https://github.com/RJ-SMTR/pipelines_v3/pull/665)

## [1.0.3] - 2026-09-10

### Alterado

- Remove o atraso de 24 horas da validação de viagens e usa diretamente o selector de `viagem_informada` como fonte de dados (https://github.com/RJ-SMTR/pipelines_v3/pull/664)

## [1.0.2] - 2026-07-29

### Alterado

- Altera `VIAGEM_VALIDACAO_DELAY_HOURS` para 24 horas, permitindo feedback diário da validação enquanto correções na janela de 5 dias são reprocessadas via partições modificadas de `viagem_informada` (https://github.com/RJ-SMTR/pipelines_v3/pull/439)

## [1.0.1] - 2026-07-07

### Removido

- Remove dependência do GPS do Cittati (https://github.com/RJ-SMTR/pipelines_v3/pull/360)

## [1.0.0] - 2026-04-29

### Adicionado

- Cria flow `treatment__viagem_validacao` (https://github.com/RJ-SMTR/pipelines_v3/pull/136)
