# Motivo da alteração — Materialization builder: run mode em vez de flags soltas

Candidato 5 do relatório de arquitetura gerado em 2026-07-13
(`/improve-codebase-architecture`, modelo Fable 5), scoped aos hot spots de
`common/` (capture/treatment builders), capturas riorotativo e o flow de
timestamps divergentes da Jaé.

## Problema identificado

Cada nova capacidade de `create_materialization_flows_default_tasks`
(`test_only`, `fallback_run`, `skip_pre_test`, `force_test_run`) virava mais
um parâmetro booleano e mais um `if` no corpo da função — a interface
acompanhava quase 1:1 a implementação, com risco de combinações inválidas de
flags, em uma função com 30+ pontos de chamada (todos os flows
`treatment__*`).

## Solução aplicada

As flags booleanas foram substituídas por um `run_mode: str` nomeado
(`"full"`, `"skip_pre_test"`, `"test_only"`, `"post_test_only"`), resolvido
via `MATERIALIZATION_RUN_MODES` em
`pipelines/common/treatment/default_treatment/utils.py`, e os parâmetros de
teste (horário agendado, força execução, webhook, menções) foram agrupados em
`MaterializationTestConfig`.

## ⚠️ Observação: entendimento errado herdado do relatório

O relatório (e a implementação que seguiu ele) tratou `test_only` e
`force_test_run` como se fossem semanticamente equivalentes, e nomeou a
combinação `test_only + skip_pre_test` como um modo próprio (`"post_test_only"`).
Isso **não reflete o comportamento original**:

- **Antes do refactor**, `test_only` e `force_test_run` eram flags
  **independentes**. O único flow que expunha `test_only`
  (`treatment__subsidio_sppo_apuracao/flow.py`) nunca expunha
  `force_test_run` — ele ficava no default (`False`) do builder. Ou seja,
  rodar em modo "só teste" **respeitava o `test_scheduled_time`**; não forçava
  execução imediata.
- **Depois do refactor**, `MATERIALIZATION_RUN_MODES["test_only"]` e
  `["post_test_only"]` hardcodam `force_test_run=True`. Isso colapsa dois
  eixos que antes eram ortogonais (*o quê* rodar vs. *quando* rodar) em um
  só, mudando o comportamento em produção de forma silenciosa: agora
  `run_mode="test_only"` sempre ignora o agendamento.
- `"post_test_only"` como conceito nomeado também é uma invenção do
  refactor — no código original essa combinação nunca foi documentada ou
  exposta como um modo à parte.

**Antes de mergear**: reavaliar se `force_test_run` deve continuar acoplado
ao modo, ou se deve voltar a ser controlável de forma independente via
`MaterializationTestConfig.force_run`, preservando o comportamento anterior.
