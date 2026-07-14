# Motivo da alteração — JAE gap-repair: registry em vez de três tabelas paralelas

Candidato 4 do relatório de arquitetura gerado em 2026-07-13
(`/improve-codebase-architecture`, modelo Fable 5), scoped aos hot spots de
`common/` (capture/treatment builders), capturas riorotativo e o flow de
timestamps divergentes da Jaé.

## Problema identificado

`pipelines/treatment__jae_timestamps_divergentes/{flow.py, tasks.py,
constants.py}` mantinha três tabelas de constantes paralelas
(`CAPTURE_GAP_TABLES`, `CAPTURE_GAP_SELECTORS`, `SQL_TREATMENTS`) que
precisavam ser editadas em conjunto, um or-chain com seis `table_id`
hardcoded (`flow.py:64-69`), e um caso especial que identifica um flow irmão
por identidade de objeto (`tasks.py:99`,
`create_recapture_subflows_params`). Conhecimento específico de cada tabela
vazava por todas as costuras do módulo — padrão que já gerou três hotfixes
recorrentes (commits `61d1f03a`, `a4afb60e`, `0eb48bb4`).

## Solução proposta

Manter o módulo (ele esconde complexidade real de detecção de gaps e
janelamento — passa no "deletion test"), mas encolher sua interface: um
`GapWindow` tipado e um registry único mapeando cada `table_id` para sua
receita completa de reparo (flow de recaptura, selectors, sql, flows a
jusante).

## Ganhos esperados

- Localidade: adicionar uma tabela vira editar uma linha do registry, não
  cinco lugares diferentes.
- Interface tipada substitui o dict cru e o or-chain de `table_id`.
- Lógica de janelamento vira código puro, testável unitariamente.
- Remove o caso especial de identidade de flow.
