# Changelog

## [1.1.1] - 2026-08-31

### Adicionado

- Adiciona descrições dos testes `check_km_planejada__sumario_faixa_servico_dia`, `check_km_planejada__sumario_faixa_servico_dia_pagamento`, `check_km_planejada__viagem_planejada` e `teto_viagens__viagens_remuneradas` para notificação no Discord. (https://github.com/RJ-SMTR/pipelines_v3/pull/607)

### Corrigido

- Corrige o casamento da descrição do teste `test_consistencia_servico_planejado_faixa_horaria` na notificação do Discord. (https://github.com/RJ-SMTR/pipelines_v3/pull/607)

## [1.1.0] - 2026-08-17

### Adicionado

- Adiciona ao pós-teste do planejamento diário a validação de trajetos alternativos de `viagem_planejada_planejamento_dia`. (https://github.com/RJ-SMTR/pipelines_v3/pull/478)
- Adiciona ao pós-teste do planejamento diário o teste `test_consistencia_servico_planejado_faixa_horaria`, que compara os totais de partidas e quilometragem dos serviços planejados com a Ordem de Serviço. (https://github.com/RJ-SMTR/pipelines_v3/pull/478)

### Alterado

- Exclui os testes com a tag `freshness` do pós-teste do planejamento diário, mantendo sua execução no flow `control__model_freshness`. (https://github.com/RJ-SMTR/pipelines_v3/pull/478)

## [1.0.1] - 2026-07-29

### Corrigido

- Impede que execuções manuais ou disparadas como subflow avancem o checkpoint do planejamento diário no Redis, preservando sua atualização para a execução agendada. (https://github.com/RJ-SMTR/pipelines_v3/pull/437)

## [1.0.0] - 2026-03-02

### Adicionado

- Migração do flow `PLANEJAMENTO_DIARIO_MATERIALIZACAO` do Prefect 1.4 para Prefect 3.0 (https://github.com/RJ-SMTR/pipelines_v3/pull/64)
