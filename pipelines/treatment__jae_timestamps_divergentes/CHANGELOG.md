# Changelog - treatment__jae_timestamps_divergentes

## [1.1.0] - 2026-07-13

### Alterado

- Substitui as constantes paralelas `CAPTURE_GAP_TABLES`/`CAPTURE_GAP_SELECTORS`/`SQL_TREATMENTS`
  pelo `GAP_REPAIR_REGISTRY`: um `GapRepairSpec` por tabela com a receita completa de reparo
  (flow de recaptura, flows de materialização, SQLs de atualização e flows downstream)
- Remove a cadeia de condições com table_ids fixos e a comparação por identidade de flow;
  a execução do `treatment__passageiro_hora` passa a ser derivada do registry
- Simplifica o resultado de `get_gaps_from_result_table` para `dict[table_id, list[timestamps]]`
- Remove a chave `selector` (nunca utilizada) da configuração de materialização

## [1.0.0] - 2026-07-03

### Adicionado

- Cria flow `treatment__jae_timestamps_divergentes`
