# Changelog - treatment__viagem_validacao

## [1.0.3] - 2026-09-10

### Alterado

- Remove o atraso de 24 horas da validação de viagens e usa diretamente o selector de `viagem_informada` como fonte de dados.
- Ajusta `indicador_servico_planejado_os` para ser aplicado somente ao sistema SPPO, buscando o sistema por data e serviço, independentemente da faixa horária da OS.

## [1.0.2] - 2026-07-29

### Alterado

- Altera `VIAGEM_VALIDACAO_DELAY_HOURS` para 24 horas, permitindo feedback diário da validação enquanto correções na janela de 5 dias são reprocessadas via partições modificadas de `viagem_informada` (https://github.com/RJ-SMTR/pipelines_v3/pull/439)

## [1.0.1] - 2026-07-07

### Removido

- Remove dependência do GPS do Cittati (https://github.com/RJ-SMTR/pipelines_v3/pull/360)

## [1.0.0] - 2026-04-29

### Adicionado

- Cria flow `treatment__viagem_validacao` (https://github.com/RJ-SMTR/pipelines_v3/pull/136)
